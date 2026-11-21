// Bounded browser PUT buffer regression tests.
//
// Covers: normal W=2 path unchanged, freeze/enqueue without timestamp or
// replication, FIFO bounded replay, capacity, session disconnect/fd reuse,
// safe abort vs COMMIT UNCERTAIN, membership reset, a second transition
// during draining, Strategy C single-count admission, replay quorum failure,
// and EventLoop responsiveness under a full queue.
//
// Real WebSocket sessions additionally verify that session generations are
// unique per accepted connection and that the disconnect callback reports the
// exact generation that ended (descriptor reuse is explicitly tolerated).
#include "peer/peer_node.hpp"
#include "replication/replication_manager.hpp"
#include "core/balanced_placement.hpp"
#include "ui/websocket_server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/time.h>
#include <thread>
#include <vector>

namespace p2place::peer {

// Test-only access to PeerNode internals. Production code never calls this.
struct PeerNodeTestAccess {
    static void set_frozen(PeerNode& n, bool value) { n.frozen_ = value; }
    static bool frozen(const PeerNode& n) { return n.frozen_; }
    static void set_placement_ready(PeerNode& n, bool value) {
        n.placement_ready_ = value;
    }
    static bool placement_ready(const PeerNode& n) { return n.placement_ready_; }
    static void set_phase_preparing(PeerNode& n) {
        n.phase_ = PeerNode::Phase::Preparing;
    }
    static void set_phase_committing(PeerNode& n) {
        n.phase_ = PeerNode::Phase::Committing;
    }
    static void set_phase_idle(PeerNode& n) { n.phase_ = PeerNode::Phase::Idle; }
    static void handle_put(PeerNode& n, int fd, std::uint64_t gen,
                           std::uint16_t x, std::uint16_t y,
                           std::uint32_t color) {
        n.handle_put(fd, gen, x, y, color);
    }
    static void drain(PeerNode& n) { n.drain_browser_puts(); }
    static bool enqueue(PeerNode& n, int fd, std::uint64_t gen,
                        std::uint16_t x, std::uint16_t y,
                        std::uint32_t color) {
        return n.queue_browser_put(fd, gen, x, y, color);
    }
    static std::size_t depth(const PeerNode& n) {
        return n.pending_browser_puts_.size();
    }
    static const PeerNode::PendingBrowserPut& front(const PeerNode& n) {
        return n.pending_browser_puts_.front();
    }
    static std::uint64_t enqueued(const PeerNode& n) {
        return n.put_buffer_enqueued_;
    }
    static std::uint64_t full_rejected(const PeerNode& n) {
        return n.put_buffer_full_rejected_;
    }
    static std::uint64_t replayed(const PeerNode& n) {
        return n.put_buffer_replayed_;
    }
    static std::uint64_t discarded_disconnected(const PeerNode& n) {
        return n.put_buffer_discarded_disconnected_;
    }
    static std::uint64_t discarded_membership(const PeerNode& n) {
        return n.put_buffer_discarded_membership_reset_;
    }
    static std::uint64_t replay_ok(const PeerNode& n) {
        return n.put_buffer_replay_quorum_success_;
    }
    static std::uint64_t replay_failed(const PeerNode& n) {
        return n.put_buffer_replay_quorum_failed_;
    }
    static core::RegionStore& store(PeerNode& n) { return n.region_store_; }
    static std::size_t announced_pixels(const PeerNode& n) {
        return n.last_browser_pixel_versions_.size();
    }
    static core::LamportClock& clock(PeerNode& n) { return n.lamport_clock_; }
    static core::RegionWriteTracker& tracker(PeerNode& n) {
        return n.write_tracker_;
    }
    static std::unique_ptr<replication::ReplicationManager>&
    replicator(PeerNode& n) { return n.replicator_; }
    static std::unique_ptr<ui::WebSocketServer>& ws(PeerNode& n) {
        return n.ws_server_;
    }
    static net::EventLoop& loop(PeerNode& n) { return n.loop_; }
    static void reset_to_joiner(PeerNode& n) {
        n.reset_to_joiner("test-membership-reset");
    }
    static void abort_transition(PeerNode& n) {
        n.abort_transition("test-abort");
    }
    static void discard_for_session(PeerNode& n, int fd, std::uint64_t gen) {
        n.discard_browser_puts(fd, gen, /*membership_reset=*/false);
    }
};

} // namespace p2place::peer

namespace p2place::ui {

// Test-only insertion of a synthetic open browser session.
struct WebSocketServerTestAccess {
    static std::uint64_t add_open_session(WebSocketServer& server, int fd) {
        auto [it, inserted] = server.sessions_.try_emplace(fd);
        if (!inserted) throw std::runtime_error("duplicate synthetic session");
        it->second.generation = ++server.next_session_generation_;
        it->second.state = WebSocketServer::ConnectionState::Open;
        return it->second.generation;
    }
    static void remove_session(WebSocketServer& server, int fd) {
        server.sessions_.erase(fd);
    }
};

} // namespace p2place::ui

namespace {

using namespace p2place;
using Clock = std::chrono::steady_clock;

void check(bool ok, const std::string& what) {
    if (!ok) throw std::runtime_error(what);
}

void check_eq(std::uint64_t got, std::uint64_t want, const std::string& what) {
    if (got != want)
        throw std::runtime_error(what + ": got=" + std::to_string(got) +
                                 " want=" + std::to_string(want));
}

// Silence the per-PUT operational logs while still keeping test failures.
struct SilenceLogs {
    std::ostringstream sink;
    std::streambuf* old_out;
    std::streambuf* old_err;
    SilenceLogs()
        : old_out(std::cout.rdbuf(sink.rdbuf())),
          old_err(std::cerr.rdbuf(sink.rdbuf())) {}
    ~SilenceLogs() {
        std::cout.rdbuf(old_out);
        std::cerr.rdbuf(old_err);
    }
};

std::string unique_state_path() {
    static std::atomic<unsigned> sequence{0};
    return (std::filesystem::temp_directory_path() /
            ("p2place-put-buffer-" + std::to_string(::getpid()) + "-" +
             std::to_string(++sequence)))
        .string();
}

// One isolated PeerNode with a synthetic open browser session. No ports are
// listened on (run() is never called); the synthetic WebSocketServer only
// supplies session identity/validity for the queue checks.
struct TestNode {
    peer::Config config;
    std::unique_ptr<peer::PeerNode> node;
    int browser_fd = 100;
    std::uint64_t generation = 0;
    std::string state_path;

    explicit TestNode(std::size_t write_w = 2,
                      peer::Config::Strategy strategy =
                          peer::Config::Strategy::A,
                      std::size_t n = 3) {
        state_path = unique_state_path();
        config.id = 1;
        config.port = 0;
        config.ws_port = 0;
        config.state_file = state_path;
        config.strategy = strategy;
        config.replication_n = n;
        config.read_r = write_w == 0 ? 1 : write_w;
        config.write_w = write_w;
        config.min_replicas = 2;
        config.max_replicas = 4;
        node = std::make_unique<peer::PeerNode>(config);
        peer::PeerNodeTestAccess::ws(*node) =
            std::make_unique<ui::WebSocketServer>(
                peer::PeerNodeTestAccess::loop(*node), 0,
                [](int, std::string_view) {},
                [](int, std::uint64_t) {});
        generation = ui::WebSocketServerTestAccess::add_open_session(
            *peer::PeerNodeTestAccess::ws(*node), browser_fd);
    }

    ~TestNode() {
        std::error_code ec;
        std::filesystem::remove(state_path, ec);
        std::filesystem::remove(state_path + ".pending", ec);
    }

    peer::PeerNode& operator*() { return *node; }
    peer::PeerNode* operator->() { return node.get(); }

    void reopen_session(int fd) {
        browser_fd = fd;
        generation = ui::WebSocketServerTestAccess::add_open_session(
            *peer::PeerNodeTestAccess::ws(*node), fd);
    }
};

// Three in-process ReplicationManagers with a fake transport (mirrors the
// Lab 05 simulation). Node 1's manager is installed in the PeerNode so the
// real PUT admission/replay path is exercised under the real W=2 contract.
struct QuorumHarness {
    struct Packet {
        core::NodeId src;
        core::NodeId dst;
        messages::Message msg;
    };

    core::BalancedPlacement placement =
        core::BalancedPlacement::create({1, 2, 3});
    core::ConsistentHashRing ring;
    core::RegionStore remote_stores[2]; // nodes 2 and 3
    std::array<std::unique_ptr<replication::FixedReplicationStrategy>, 3>
        strategies;
    std::array<std::unique_ptr<replication::ReplicationManager>, 2>
        remotes; // nodes 2 and 3
    std::deque<Packet> packets;
    std::array<bool, 3> alive{true, true, true};
    std::array<std::map<net::TimerQueue::Token, std::function<void()>>, 3>
        timers;
    std::array<net::TimerQueue::Token, 3> next_token{1, 1, 1};
    peer::PeerNode* local = nullptr;

    explicit QuorumHarness(peer::PeerNode& node) : local(&node) {
        ring.rebuild(placement);
        for (int i = 0; i < 3; ++i)
            strategies[i] =
                std::make_unique<replication::FixedReplicationStrategy>(
                    replication::QuorumConfig{3, 2, 2});
        // Replace node 1's transport with the simulated ring.
        peer::PeerNodeTestAccess::replicator(node) =
            std::make_unique<replication::ReplicationManager>(
                1, ring, peer::PeerNodeTestAccess::store(node),
                *strategies[0],
                [this](core::NodeId dst, const messages::Message& m) {
                    if (!alive[dst - 1]) return false;
                    packets.push_back({1, dst, m});
                    return true;
                },
                [this](std::function<void()> cb) {
                    auto token = next_token[0]++;
                    timers[0][token] = std::move(cb);
                    return token;
                },
                [this](net::TimerQueue::Token token) {
                    timers[0].erase(token);
                });
        for (core::NodeId id : {2, 3}) {
            const int index = static_cast<int>(id) - 1;
            remotes[index - 1] =
                std::make_unique<replication::ReplicationManager>(
                    id, ring, remote_stores[index - 1], *strategies[index],
                    [this, id](core::NodeId dst, const messages::Message& m) {
                        if (!alive[dst - 1]) return false;
                        packets.push_back({id, dst, m});
                        return true;
                    },
                    [this, index](std::function<void()> cb) {
                        auto token = next_token[index]++;
                        timers[index][token] = std::move(cb);
                        return token;
                    },
                    [this, index](net::TimerQueue::Token token) {
                        timers[index].erase(token);
                    });
        }
    }

    void pump() {
        int steps = 0;
        while (!packets.empty()) {
            check(++steps < 10000, "faulty transport packet loop");
            Packet p = std::move(packets.front());
            packets.pop_front();
            if (!alive[p.dst - 1]) continue;
            if (p.dst == 1)
                peer::PeerNodeTestAccess::replicator(*local)
                    ->on_message(p.src, p.msg);
            else
                remotes[p.dst - 2]->on_message(p.src, p.msg);
        }
    }
};

void test_normal_w2_path_unchanged() {
    TestNode t(/*write_w=*/2);
    QuorumHarness cluster(*t);
    const auto before = peer::PeerNodeTestAccess::clock(*t).now();

    peer::PeerNodeTestAccess::handle_put(*t, t.browser_fd, t.generation,
                                         0, 0, 0x112233);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 0,
             "normal PUT must not enqueue");
    check(peer::PeerNodeTestAccess::replicator(*t)->pending_count() == 1,
          "normal PUT must submit exactly one quorum request");
    cluster.pump();
    check_eq(peer::PeerNodeTestAccess::clock(*t).now(), before + 1,
             "normal PUT ticks Lamport exactly once");
    check_eq(peer::PeerNodeTestAccess::replay_ok(*t), 0,
             "normal PUT is not replay accounting");

    bool stored = peer::PeerNodeTestAccess::store(*t).get_region(0).has_value();
    for (auto& store : cluster.remote_stores)
        stored = stored || store.get_region(0).has_value();
    check(stored, "W=2 normal PUT stored on at least one replica");

    // W=2 is still mandatory: make one of three replicas unavailable and
    // confirm the same PUT cannot reach quorum with only one acknowledgement.
    TestNode lonely(/*write_w=*/2); // one-node ring, W stays 2
    const auto lonely_before =
        peer::PeerNodeTestAccess::clock(*lonely).now();
    peer::PeerNodeTestAccess::handle_put(*lonely, lonely.browser_fd,
                                         lonely.generation, 1, 1, 0x445566);
    check_eq(peer::PeerNodeTestAccess::clock(*lonely).now(),
             lonely_before + 1, "rejected W=2 PUT still ticks once");
    // The local ReplicationManager may apply the pixel optimistically, but a
    // browser-visible committed event requires the W=2 quorum callback.
    check_eq(peer::PeerNodeTestAccess::announced_pixels(*lonely), 0,
             "W=2 must not announce/commit with one replica");
    check_eq(peer::PeerNodeTestAccess::replicator(*lonely)->pending_count(), 1,
             "W=2 request stays outstanding rather than succeeding");
    std::cout << "PASS put-buffer: normal W=2 path unchanged\n";
}

void test_freeze_enqueues_without_execution() {
    TestNode t(/*write_w=*/1);
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    const auto before = peer::PeerNodeTestAccess::clock(*t).now();

    peer::PeerNodeTestAccess::handle_put(*t, t.browser_fd, t.generation,
                                         4, 5, 0xAABBCC);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 1,
             "frozen PUT must enqueue exactly one");
    check_eq(peer::PeerNodeTestAccess::enqueued(*t), 1,
             "enqueue counter");
    check_eq(peer::PeerNodeTestAccess::clock(*t).now(), before,
             "queued PUT must not tick the Lamport clock");
    check_eq(peer::PeerNodeTestAccess::replicator(*t)->pending_count(), 0,
             "queued PUT must not call replication");
    check(!peer::PeerNodeTestAccess::store(*t).all_keys().size(),
          "queued PUT must not store a pixel");
    const auto& entry = peer::PeerNodeTestAccess::front(*t);
    check(entry.browser_fd == t.browser_fd &&
              entry.session_generation == t.generation,
          "queue entry keeps fd + session generation");
    check(entry.x == 4 && entry.y == 5 && entry.color == 0xAABBCC,
          "queue entry keeps validated pixel data");
    std::cout << "PASS put-buffer: frozen committed PUT queues without "
                 "timestamp or replication\n";
}

void test_fifo_bounded_replay() {
    TestNode t(/*write_w=*/1);
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    const auto base = peer::PeerNodeTestAccess::clock(*t).now();
    constexpr int kPuts = 100;
    for (int i = 0; i < kPuts; ++i)
        peer::PeerNodeTestAccess::handle_put(
            *t, t.browser_fd, t.generation,
            static_cast<std::uint16_t>(i % 10),
            static_cast<std::uint16_t>(i / 10),
            static_cast<std::uint32_t>(0x100000 + i));
    check_eq(peer::PeerNodeTestAccess::depth(*t), kPuts, "queued 100");

    peer::PeerNodeTestAccess::set_frozen(*t, false);
    peer::PeerNodeTestAccess::drain(*t);
    check_eq(peer::PeerNodeTestAccess::depth(*t), kPuts - 32,
             "first tick drains at most 32");
    check_eq(peer::PeerNodeTestAccess::replayed(*t), 32,
             "first tick replays 32");
    // Four ticks in total: 32+32+32+4.
    for (int tick = 0; tick < 3; ++tick)
        peer::PeerNodeTestAccess::drain(*t);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 0, "queue empty");
    check_eq(peer::PeerNodeTestAccess::replayed(*t), kPuts,
             "each queued PUT submitted once");
    check_eq(peer::PeerNodeTestAccess::replay_ok(*t), kPuts,
             "each replay completed W=1 quorum");
    check_eq(peer::PeerNodeTestAccess::clock(*t).now(), base + kPuts,
             "exactly one Lamport tick per replay (no duplicates)");

    std::size_t pixels = 0;
    for (std::uint32_t key = 0; key < core::RegionStore::REGION_COUNT; ++key) {
        const auto region = peer::PeerNodeTestAccess::store(*t).get_region(key);
        if (!region) continue;
        for (std::size_t index = 0; index < core::Region::COUNT; ++index) {
            const auto& pixel = region->pixel(index);
            if (pixel.lamport == 0) continue;
            ++pixels;
            const auto x = (key % core::RegionStore::REGIONS_SIDE) *
                               core::Region::SIDE +
                           index % core::Region::SIDE;
            const auto y = (key / core::RegionStore::REGIONS_SIDE) *
                               core::Region::SIDE +
                           index / core::Region::SIDE;
            const int original = static_cast<int>(y) * 10 +
                                 static_cast<int>(x);
            check_eq(pixel.lamport,
                     static_cast<std::uint64_t>(base + original + 1),
                     "FIFO: replay order matches enqueue order");
        }
    }
    check_eq(pixels, kPuts, "one stored pixel per original PUT");
    std::cout << "PASS put-buffer: FIFO bounded replay, exactly once, "
                 "32 per tick\n";
}

void test_capacity() {
    TestNode t(/*write_w=*/1);
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    const auto before = peer::PeerNodeTestAccess::clock(*t).now();
    {
        SilenceLogs silence;
        for (std::size_t i = 0; i < 4096; ++i)
            check(peer::PeerNodeTestAccess::enqueue(
                      *t, t.browser_fd, t.generation,
                      static_cast<std::uint16_t>(i % 256),
                      static_cast<std::uint16_t>(i / 256), 0x00FF00),
                  "enqueue up to capacity");
        check_eq(peer::PeerNodeTestAccess::depth(*t), 4096, "queue at 4096");
        // The next valid PUT must be rejected explicitly, not queued.
        peer::PeerNodeTestAccess::handle_put(*t, t.browser_fd, t.generation,
                                             0, 0, 0xFFFFFF);
    }
    check_eq(peer::PeerNodeTestAccess::depth(*t), 4096,
             "full queue unchanged by rejected PUT");
    check_eq(peer::PeerNodeTestAccess::full_rejected(*t), 1,
             "full rejection accounted");
    check_eq(peer::PeerNodeTestAccess::clock(*t).now(), before,
             "rejected PUT never ticks");
    std::cout << "PASS put-buffer: capacity 4096 rejects explicitly\n";
}

void test_disconnect_fd_reuse() {
    TestNode t(/*write_w=*/1);
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    peer::PeerNodeTestAccess::handle_put(*t, 10, 1, 7, 7, 0x010203);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 1, "old session queued");
    // Session 1 disappears WITHOUT the drain seeing it; fd 10 is then reused
    // by a new session. Generation matching must prevent misdelivery.
    ui::WebSocketServerTestAccess::remove_session(
        *peer::PeerNodeTestAccess::ws(*t), 10);
    std::uint64_t new_generation =
        ui::WebSocketServerTestAccess::add_open_session(
            *peer::PeerNodeTestAccess::ws(*t), 10);
    check(new_generation != 1, "fd reuse gets a fresh generation");

    peer::PeerNodeTestAccess::set_frozen(*t, false);
    peer::PeerNodeTestAccess::drain(*t);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 0, "stale entry removed");
    check_eq(peer::PeerNodeTestAccess::replayed(*t), 0,
             "old session's PUT never executed");
    check_eq(peer::PeerNodeTestAccess::discarded_disconnected(*t), 1,
             "stale entry counted as discarded_disconnected");
    check_eq(peer::PeerNodeTestAccess::store(*t).all_keys().size(), 0,
             "no pixel from the disconnected browser");

    // The new browser (same fd) may write normally.
    peer::PeerNodeTestAccess::handle_put(*t, 10, new_generation,
                                         8, 8, 0x0A0B0C);
    peer::PeerNodeTestAccess::drain(*t);
    check_eq(peer::PeerNodeTestAccess::replayed(*t), 0,
             "direct write is not replay accounting");
    check(peer::PeerNodeTestAccess::store(*t).all_keys().size() > 0,
          "new session's direct write executes");

    // Immediate disconnect notification (the real WebSocket callback path).
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    peer::PeerNodeTestAccess::handle_put(*t, 10, new_generation,
                                         9, 9, 0x0D0E0F);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 1, "new session queued");
    peer::PeerNodeTestAccess::discard_for_session(*t, 10, new_generation);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 0,
             "disconnect callback drops that session's queue immediately");
    check_eq(peer::PeerNodeTestAccess::discarded_disconnected(*t), 2,
             "disconnect discard accounted");
    std::cout << "PASS put-buffer: disconnect + fd reuse cannot misdeliver\n";
}

void test_abort_vs_uncertain_commit() {
    TestNode t(/*write_w=*/1);
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    peer::PeerNodeTestAccess::set_phase_preparing(*t);
    for (int i = 0; i < 5; ++i)
        peer::PeerNodeTestAccess::handle_put(
            *t, t.browser_fd, t.generation,
            static_cast<std::uint16_t>(i), 0, 0x222222);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 5, "queued before abort");

    // Safe pre-COMMIT abort restores the old placement and unfreezes.
    peer::PeerNodeTestAccess::abort_transition(*t);
    check(!peer::PeerNodeTestAccess::frozen(*t),
          "safe abort unfreezes");
    peer::PeerNodeTestAccess::drain(*t);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 0,
             "safe abort lets the queue drain");
    check_eq(peer::PeerNodeTestAccess::replayed(*t), 5, "all replayed");

    // COMMIT UNCERTAIN: abort must NOT unfreeze and the queue must survive.
    TestNode u(/*write_w=*/1);
    peer::PeerNodeTestAccess::set_frozen(*u, true);
    peer::PeerNodeTestAccess::set_phase_committing(*u);
    for (int i = 0; i < 3; ++i)
        peer::PeerNodeTestAccess::handle_put(
            *u, u.browser_fd, u.generation,
            static_cast<std::uint16_t>(i), 1, 0x333333);
    peer::PeerNodeTestAccess::abort_transition(*u);
    check(peer::PeerNodeTestAccess::frozen(*u),
          "COMMIT UNCERTAIN stays frozen");
    peer::PeerNodeTestAccess::drain(*u);
    check_eq(peer::PeerNodeTestAccess::depth(*u), 3,
             "COMMIT UNCERTAIN keeps every queued PUT");
    check_eq(peer::PeerNodeTestAccess::replayed(*u), 0,
             "COMMIT UNCERTAIN never replays");
    std::cout << "PASS put-buffer: safe abort drains, uncertain commit "
                 "stays frozen\n";
}

void test_membership_reset_discards() {
    TestNode t(/*write_w=*/1);
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    for (int i = 0; i < 4; ++i)
        peer::PeerNodeTestAccess::handle_put(
            *t, t.browser_fd, t.generation,
            static_cast<std::uint16_t>(i), 2, 0x444444);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 4, "queued before reset");

    // A removed/stale node must not execute queued work under an
    // uncommitted placement.
    peer::PeerNodeTestAccess::set_frozen(*t, false);
    peer::PeerNodeTestAccess::set_placement_ready(*t, false);
    peer::PeerNodeTestAccess::drain(*t);
    check_eq(peer::PeerNodeTestAccess::replayed(*t), 0,
             "unadmitted node never replays");
    check_eq(peer::PeerNodeTestAccess::depth(*t), 4,
             "unadmitted drain leaves the queue untouched");

    peer::PeerNodeTestAccess::reset_to_joiner(*t);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 0,
             "stale reset discards the queue");
    check_eq(peer::PeerNodeTestAccess::discarded_membership(*t), 4,
             "membership reset accounted, not silent");
    std::cout << "PASS put-buffer: membership/stale reset discards with "
                 "accounting\n";
}

void test_second_transition_interrupts_drain() {
    TestNode t(/*write_w=*/1);
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    const auto base = peer::PeerNodeTestAccess::clock(*t).now();
    {
        SilenceLogs silence;
        for (int i = 0; i < 100; ++i)
            peer::PeerNodeTestAccess::enqueue(
                *t, t.browser_fd, t.generation,
                static_cast<std::uint16_t>(i % 10),
                static_cast<std::uint16_t>(i / 10), 0x555555);
    }
    peer::PeerNodeTestAccess::set_frozen(*t, false);
    peer::PeerNodeTestAccess::drain(*t);
    check_eq(peer::PeerNodeTestAccess::replayed(*t), 32, "first batch");

    // A new placement transition begins mid-queue.
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    const auto frozen_tick = peer::PeerNodeTestAccess::clock(*t).now();
    peer::PeerNodeTestAccess::drain(*t);
    check_eq(peer::PeerNodeTestAccess::replayed(*t), 32,
             "no execution while frozen again");
    check_eq(peer::PeerNodeTestAccess::clock(*t).now(), frozen_tick,
             "interrupted drain ticks nothing");
    check_eq(peer::PeerNodeTestAccess::depth(*t), 68,
             "remaining queue survives");

    peer::PeerNodeTestAccess::set_frozen(*t, false);
    for (int tick = 0; tick < 3; ++tick)
        peer::PeerNodeTestAccess::drain(*t);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 0,
             "remaining queue drains after the next safe RELEASE");
    check_eq(peer::PeerNodeTestAccess::replayed(*t), 100,
             "remaining queue drains after the next safe RELEASE");
    check_eq(peer::PeerNodeTestAccess::clock(*t).now(), base + 100,
             "exactly one tick per original PUT across both rounds");
    std::cout << "PASS put-buffer: second transition interrupts drain and "
                 "preserves the queue\n";
}

void test_strategy_c_single_count() {
    TestNode t(/*write_w=*/1, peer::Config::Strategy::C);
    const auto key = core::RegionStore::region_key(3, 4);
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    peer::PeerNodeTestAccess::handle_put(*t, t.browser_fd, t.generation,
                                         3, 4, 0x666666);
    auto delta =
        peer::PeerNodeTestAccess::tracker(*t).drain_delta(Clock::now());
    check_eq(delta.counts[key], 1,
             "buffered admission counts one Strategy C attempt");

    peer::PeerNodeTestAccess::set_frozen(*t, false);
    peer::PeerNodeTestAccess::drain(*t);
    delta = peer::PeerNodeTestAccess::tracker(*t).drain_delta(Clock::now());
    check_eq(delta.counts[key], 0,
             "replay must NOT count the same client attempt again");

    // A distinct direct PUT still counts exactly once.
    peer::PeerNodeTestAccess::handle_put(*t, t.browser_fd, t.generation,
                                         3, 4, 0x777777);
    delta = peer::PeerNodeTestAccess::tracker(*t).drain_delta(Clock::now());
    check_eq(delta.counts[key], 1,
             "distinct direct PUT counts exactly once");
    std::cout << "PASS put-buffer: Strategy C attempted-write count is not "
                 "doubled by replay\n";
}

void test_replay_quorum_failure() {
    TestNode t(/*write_w=*/2);
    QuorumHarness cluster(*t);
    // Two of the three W=2 replicas are unavailable: the quorum fails
    // synchronously instead of waiting for the deadline.
    cluster.alive[1] = false;
    cluster.alive[2] = false;
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    peer::PeerNodeTestAccess::handle_put(*t, t.browser_fd, t.generation,
                                         11, 12, 0x888888);
    peer::PeerNodeTestAccess::set_frozen(*t, false);
    {
        SilenceLogs silence;
        peer::PeerNodeTestAccess::drain(*t);
    }
    check_eq(peer::PeerNodeTestAccess::replayed(*t), 1,
             "replay submitted once");
    check_eq(peer::PeerNodeTestAccess::replay_ok(*t), 0,
             "failed replay is not a success");
    check_eq(peer::PeerNodeTestAccess::replay_failed(*t), 1,
             "failed replay counted separately");
    check_eq(peer::PeerNodeTestAccess::depth(*t), 0,
             "failed replay is not silently retried forever");
    check_eq(peer::PeerNodeTestAccess::announced_pixels(*t), 0,
             "failed replay never broadcasts a committed pixel");
    std::cout << "PASS put-buffer: replay quorum failure reported, never "
                 "counted as success\n";
}

void test_event_loop_responsiveness() {
    TestNode t(/*write_w=*/1);
    peer::PeerNodeTestAccess::set_frozen(*t, true);
    {
        SilenceLogs silence;
        for (std::size_t i = 0; i < 4096; ++i)
            peer::PeerNodeTestAccess::enqueue(
                *t, t.browser_fd, t.generation,
                static_cast<std::uint16_t>(i % 256),
                static_cast<std::uint16_t>(i / 256), 0x999999);
    }
    peer::PeerNodeTestAccess::set_frozen(*t, false);
    const auto start = Clock::now();
    {
        SilenceLogs silence;
        peer::PeerNodeTestAccess::drain(*t);
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - start);
    check_eq(peer::PeerNodeTestAccess::depth(*t), 4096 - 32,
             "one tick consumes only the bounded batch");
    check(elapsed < std::chrono::seconds(2),
          "bounded tick must not block the event loop");
    std::cout << "PASS put-buffer: full 4096 queue drains in bounded "
                 "ticks (first tick " << elapsed.count() << " ms)\n";
}

// ---- Real WebSocket session identity -------------------------------------

std::uint16_t pick_free_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    check(fd >= 0, "socket for port probe");
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    check(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0,
          "probe bind");
    socklen_t len = sizeof(addr);
    check(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0,
          "probe getsockname");
    const auto port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

int connect_ws_client(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    check(fd >= 0, "client socket");
    timeval tv{};
    tv.tv_sec = 3;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    check(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0,
          "client connect");
    const std::string request =
        "GET / HTTP/1.1\r\n"
        "Host: 127.0.0.1\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n";
    check(::send(fd, request.data(), request.size(), 0) ==
              static_cast<ssize_t>(request.size()),
          "send handshake");
    std::string response;
    char buffer[512];
    while (response.find("\r\n\r\n") == std::string::npos) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0) throw std::runtime_error("handshake response timeout");
        response.append(buffer, static_cast<std::size_t>(n));
    }
    check(response.find("101") != std::string::npos,
          "server must accept the WebSocket upgrade");
    return fd;
}

void send_masked_text(int fd, std::string_view text) {
    std::vector<std::uint8_t> frame;
    frame.push_back(0x81);
    check(text.size() < 126, "test payload fits one-byte length");
    frame.push_back(static_cast<std::uint8_t>(0x80 | text.size()));
    const std::uint8_t mask[4] = {0x1A, 0x2B, 0x3C, 0x4D};
    frame.insert(frame.end(), mask, mask + 4);
    for (std::size_t i = 0; i < text.size(); ++i)
        frame.push_back(static_cast<std::uint8_t>(text[i]) ^ mask[i % 4]);
    check(::send(fd, frame.data(), frame.size(), 0) ==
              static_cast<ssize_t>(frame.size()),
          "send ws frame");
}

void test_websocket_session_generations() {
    const auto port = pick_free_port();
    net::EventLoop loop;
    std::mutex mu;
    std::condition_variable cv;
    struct Opened { int fd; std::uint64_t generation; };
    std::vector<Opened> opened;
    std::vector<std::pair<int, std::uint64_t>> closed;

    ui::WebSocketServer* server = nullptr;
    ui::WebSocketServer ws_server(
        loop, port,
        [&](int fd, std::string_view) {
            const auto gen = server->session_generation(fd);
            std::lock_guard<std::mutex> lock(mu);
            opened.push_back({fd, gen.value_or(0)});
            cv.notify_all();
        },
        [&](int fd, std::uint64_t generation) {
            std::lock_guard<std::mutex> lock(mu);
            closed.emplace_back(fd, generation);
            cv.notify_all();
        });
    server = &ws_server;

    std::thread thread([&] { loop.run([] {}); });
    struct JoinGuard {
        net::EventLoop& loop;
        std::thread& thread;
        ~JoinGuard() {
            loop.stop();
            if (thread.joinable()) thread.join();
        }
    } guard{loop, thread};
    auto wait_for = [&](const std::function<bool()>& predicate,
                        const char* what) {
        std::unique_lock<std::mutex> lock(mu);
        check(cv.wait_for(lock, std::chrono::seconds(5), predicate), what);
    };

    int first_fd = connect_ws_client(port);
    send_masked_text(first_fd, R"({"op":"session-one"})");
    wait_for([&] { return opened.size() == 1; },
             "first session command not observed");
    const int fd_a = opened[0].fd;
    const auto gen_a = opened[0].generation;
    check(gen_a != 0, "first session has a generation");

    ::close(first_fd);
    wait_for([&] { return closed.size() == 1; }, "first disconnect missing");
    check_eq(closed[0].first, static_cast<std::uint64_t>(fd_a),
             "disconnect reports the same fd");
    check_eq(closed[0].second, gen_a, "disconnect reports the same generation");

    int second_fd = connect_ws_client(port);
    send_masked_text(second_fd, R"({"op":"session-two"})");
    wait_for([&] { return opened.size() == 2; },
             "second session command not observed");
    const int fd_b = opened[1].fd;
    const auto gen_b = opened[1].generation;
    check(gen_b != gen_a,
          "a new session gets a distinct generation even on fd reuse");
    std::cout << "PASS put-buffer: WebSocket session generations "
              << (fd_a == fd_b ? "(descriptor reused) " : "(new descriptor) ")
              << "are unique; disconnect identifies the exact session\n";

    ::close(second_fd);
    wait_for([&] { return closed.size() == 2; }, "second disconnect missing");
    loop.stop();
    thread.join();
}

} // namespace

int main() {
    try {
        test_websocket_session_generations();
        test_normal_w2_path_unchanged();
        test_freeze_enqueues_without_execution();
        test_fifo_bounded_replay();
        test_capacity();
        test_disconnect_fd_reuse();
        test_abort_vs_uncertain_commit();
        test_membership_reset_discards();
        test_second_transition_interrupts_drain();
        test_strategy_c_single_count();
        test_replay_quorum_failure();
        test_event_loop_responsiveness();
        std::cout << "PASS put-buffer: all bounded browser PUT buffer tests\n";
        return 0;
    }
    catch (const std::exception& e) {
        std::cerr << "FAIL put-buffer: " << e.what() << '\n';
        return 1;
    }
}
