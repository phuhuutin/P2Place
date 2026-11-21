#include "peer/peer_node.hpp"

#include "net/protocol_codec.hpp"
#include "net/tcp_client.hpp"
#include "net/tcp_connection.hpp"
#include <nlohmann/json.hpp>
#include "core/versioned_pixel.hpp"
#include "core/membership_gossip.hpp"
#include "messages/payloads.hpp"
#include "messages/placement_protocol.hpp"
#include "replication/churn_replication_strategy.hpp"
#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>
#include <charconv>
#include <algorithm>
#include <limits>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <cstring>
#include <filesystem>
#include <set>

using json = nlohmann::json;

namespace p2place::peer {
constexpr auto CANVAS_SIZE = core::RegionStore::CANVAS_SIDE;
namespace {
    using namespace std::chrono_literals;
    //16bits to  8 bits each. big-endian: big | small
    void put_u16(std::vector<std::uint8_t>& b, std::uint16_t v) {
        b.push_back(static_cast<std::uint8_t>(v >> 8));
        b.push_back(static_cast<std::uint8_t>(v));
    }
    void put_u32(std::vector<std::uint8_t>& b, std::uint32_t v) {
        b.push_back(static_cast<std::uint8_t>(v >> 24));
        b.push_back(static_cast<std::uint8_t>(v >> 16));
        b.push_back(static_cast<std::uint8_t>(v >> 8));
        b.push_back(static_cast<std::uint8_t>(v));
    }
    std::uint16_t get_u16(const std::vector<std::uint8_t>& b, std::size_t off) {
        return (std::uint16_t(b[off]) << 8) | b[off + 1];
    }
    std::uint32_t get_u32(const std::vector<std::uint8_t>& b, std::size_t off) {
        return (std::uint32_t(b[off]) << 24) |
            (std::uint32_t(b[off + 1]) << 16) |
            (std::uint32_t(b[off + 2]) << 8) | b[off + 3];
    }

    std::uint32_t parse_color(std::string_view color) {
        if (color.size() != 7 || color[0] != '#') {
            throw std::invalid_argument(
                "Expected #RRGGBB color"
            );
        }

        std::uint32_t value = 0;

        const auto* first = color.data() + 1;
        const auto* last = color.data() + color.size();

        auto [ptr, ec] = std::from_chars(
            first, last, value, 16
        );

        if (ec != std::errc{} || ptr != last) {
            throw std::invalid_argument(
                "Invalid hexadecimal color"
            );
        }

        return value;
    }
    
    std::string color_to_hex(std::uint32_t color) {
        constexpr char digits[] = "0123456789ABCDEF";

        std::string result = "#000000";

        for (int i = 6; i >= 1; --i) {
            result[i] = digits[color & 0xF];
            color >>= 4;
        }

        return result;
    }
} // anonymous namespace 

PeerNode::PeerNode(Config config)
    : config_(std::move(config)), placement_manager_(config_.state_file) {
    // Node 1 bootstraps only the first cluster. Joiners receive committed
    // placement from the authority after coordinated data handoff.
    if (config_.id == 1 ||
        std::filesystem::exists(config_.state_file)) {
        placement_manager_.bootstrap(config_.id);
        set_committed(placement_manager_.active());
        // A restarted former leader must not immediately resume authority:
        // peers may have elected a replacement while it was offline.
        if (config_.id == 1 && placement_.members().size() > 1) {
            authority_id_ = 0; // unknown until election/reconciliation
            std::cout << "[election] recovered node 1; authority unknown, waiting for ring election\n";
        }
    }
    // A crashed follower with a pending/committed-but-not-released
    // transition must not resume serving its old placement on restart.
    const auto stage_file=config_.state_file+".pending";
    if(std::filesystem::exists(stage_file)) {
        auto stage=core::BalancedPlacement::load(stage_file);
        if(placement_ready_ && stage.epoch()<placement_.epoch())
            throw std::runtime_error("stale transition marker; manual recovery required");
        frozen_=true;
        if(!placement_ready_ || stage.epoch()>placement_.epoch())staged_=stage;
        std::cout << "[membership] restarting FROZEN with transition marker\n";
    }
    // Reliability window + policy table are shared strategy state; the
    // strategy object only READS the table (never computes N locally).
    reliability_tracker_=core::PeerReliabilityTracker(
        std::chrono::seconds(config_.reliability_window_seconds));
    policy_table_=core::PolicyTable(3);
    if(config_.strategy==Config::Strategy::A) {
        replication_strategy_=std::make_unique<replication::FixedReplicationStrategy>(
            replication::QuorumConfig{config_.replication_n,config_.read_r,config_.write_w});
    } else {
        replication_strategy_=std::make_unique<replication::ChurnReplicationStrategy>(
            policy_table_,config_.read_r,config_.write_w,config_.max_replicas);
        core::PolicyConfig pcfg{config_.min_replicas,config_.max_replicas,
                                config_.policy_threshold_high,config_.policy_threshold_low,
                                std::chrono::seconds(config_.policy_cooldown_seconds)};
        if(config_.strategy==Config::Strategy::C) {
            // Churn + write-aware composition: same reliability term as B,
            // plus the cluster-wide per-region client PUT rate.
            pcfg.strategy='C';
            pcfg.write_threshold_mid=config_.write_threshold_mid;
            pcfg.write_threshold_high=config_.write_threshold_high;
        }
        policy_evaluator_=std::make_unique<core::PolicyEvaluator>(pcfg);
    }
    process_started_=std::chrono::steady_clock::now();
    next_policy_eval_=process_started_+std::chrono::seconds(config_.policy_interval_seconds);
    if(config_.strategy==Config::Strategy::C) {
        write_tracker_=core::RegionWriteTracker(
            std::chrono::seconds(config_.write_window_seconds),process_started_);
        write_aggregate_=core::WriteReportAggregate(
            std::chrono::seconds(config_.write_window_seconds),
            std::chrono::seconds(config_.write_report_interval_seconds));
        write_report_incarnation_=static_cast<std::uint32_t>(
            process_started_.time_since_epoch().count()); // boot-unique id
        next_write_report_=process_started_+
            std::chrono::seconds(config_.write_report_interval_seconds);
    }
    if(!config_.metrics_dir.empty())
        metrics_=std::make_unique<Metrics>(
            std::filesystem::path(config_.metrics_dir).filename().string(),
            config_.metrics_dir,config_.id);
    std::cout << "[config] strategy="
              << static_cast<char>(config_.strategy==Config::Strategy::A?'A':
                                   config_.strategy==Config::Strategy::B?'B':'C')
              << " n_base=" << config_.replication_n
              << " r=" << config_.read_r << " w=" << config_.write_w
              << " min_n=" << config_.min_replicas
              << " max_n=" << config_.max_replicas
              << " window=" << config_.reliability_window_seconds << "s"
              << " interval=" << config_.policy_interval_seconds << "s"
              << " cooldown=" << config_.policy_cooldown_seconds << "s"
              << " thresholds=" << config_.policy_threshold_high << '/'
              << config_.policy_threshold_low;
    if(config_.strategy==Config::Strategy::C)
        std::cout << " write_window=" << config_.write_window_seconds << "s"
                  << " write_report=" << config_.write_report_interval_seconds << "s"
                  << " write_thresholds=" << config_.write_threshold_mid << '/'
                  << config_.write_threshold_high;
    std::cout << '\n';
    // Thresholds from config; accelerated values allowed for lab experiments.
    failure_detector_=core::FailureDetector(core::FailureDetector::Config{
        std::chrono::seconds(config_.suspect_after_seconds),
        std::chrono::seconds(config_.unavailable_after_seconds),
        std::chrono::seconds(config_.recovery_stabilization_seconds)});
    replicator_ = std::make_unique<replication::ReplicationManager>(
        config_.id,ring_,region_store_,*replication_strategy_,
        [this](core::NodeId peer,const messages::Message& m)->bool {
            const auto fd=owner_fd(peer);
            if(!fd)return false;
            send_message(*fd,m);
            return true;
        },
        [this](std::function<void()> cb) {
            return loop_.schedule_timer(net::TimerQueue::Clock::now()+std::chrono::seconds(3),std::move(cb));
        },
        [this](net::TimerQueue::Token token) {loop_.cancel_timer(token);}
    );
    replicator_->set_metrics([this](const char* op,std::uint32_t region,
                                    bool ok,double latency_ms,const std::string& error){
        if(!metrics_)return;
        metrics_->operation(
            std::chrono::duration<double>(
                std::chrono::steady_clock::now()-process_started_).count(),
            op,region,ok,latency_ms,error);
    });
    replicator_->set_backfill_metric([this](std::uint32_t region,bool ok,
                                            const std::string& error){
        if(!metrics_)return;
        metrics_->backfill(region,ok,placement_.epoch(),
                           policy_table_.initialized()
                               ? policy_table_.current().policy_epoch : 0,
                           error);
    });
    replicator_->set_repair_progress([this](std::uint32_t region,const char* event,
                                            const std::string& classification,
                                            unsigned attempts){
        if(!metrics_ || !placement_ready_)return;
        metrics_->repair(config_.id,region,placement_.epoch(),
                         policy_table_.initialized()
                             ? policy_table_.current().policy_epoch : 0,
                         event,classification,attempts);
    });
    register_handlers();
    // Configured seeds are visible to the discovery/retry loop from t=0:
    // if the one-shot seed dial in run() fails (the seed's listener may not
    // be up yet), the periodic dialer must retry them. Without this, a
    // failed initial dial leaves peers_ empty and the node has nothing to
    // dial ever again.
    for(const auto& seed:config_.peers)
        if(seed.id!=config_.id && seed.peer_port)
            peers_.upsert(seed);
    // The local process participates in placement even when no peers are online.
    refresh_ring();
    next_gossip_ = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    next_repair_ = std::chrono::steady_clock::now() + std::chrono::seconds(2);

    // Keep runtime tables in sync when EventLoop destroys a
    // connection (EOF, RST, protocol error, failed flush, ...).
    // Without this, on_tick() would keep PINGing dead fds and the
    // first "connection not found" send would kill the run loop.
    loop_.set_on_disconnect([this](int fd) {
        forget_connection(fd);
    });
}

void PeerNode::send_message(int fd, const messages::Message& msg) {
    // REQUIRED small EventLoop addition, shown in INTEGRATION.md.
    loop_.send(fd, messages::encode(msg));
}

void PeerNode::send_hello(int fd) {
    messages::Message m;
    m.type = messages::Type::HELLO;
    put_u32(m.payload, config_.id);
    put_u16(m.payload, config_.port);
    put_u16(m.payload, config_.ws_port);
    // Cluster placement identity; reject routing to peers with divergent maps.
    const auto fp = placement_ready_ ? placement_.fingerprint() : 0;
    put_u32(m.payload, static_cast<std::uint32_t>(fp >> 32));
    put_u32(m.payload, static_cast<std::uint32_t>(fp));
    // Committed generation: lets a stale restarted peer detect that the
    // cluster moved on and reset itself to a fresh joiner session.
    const auto epoch = placement_ready_ ? placement_.epoch() : 0;
    put_u32(m.payload, static_cast<std::uint32_t>(epoch));
    // 1 = prepared but not committed, 2 = committed but not released.
    m.payload.push_back(staged_ ? 1 : (frozen_ ? 2 : 0));
    send_message(fd, m);
}

void PeerNode::forget_connection(int fd) {
    dialed_fds_.erase(fd);
    const auto it = node_by_fd_.find(fd);
    if (it == node_by_fd_.end()) return;

    const auto id = it->second;
    node_by_fd_.erase(it);
    if (auto f = fd_by_node_.find(id); f != fd_by_node_.end() && f->second == fd) {
        fd_by_node_.erase(f);
        // A TCP disconnect is SUSPECT, not a permanent membership removal.
        peers_.mark(id, core::NodeStatus::Suspect);
        log_peer_health(id, core::NodeStatus::Suspect);
        failure_detector_.disconnected(id, std::chrono::steady_clock::now());
        last_pong_.erase(id);
        join_referrals_.erase(id);
        handle_peer_disconnect(id);
        if(config_.id==authority_id_ &&
           phase_!=Phase::Idle && transition_peers_.contains(id))
            abort_transition("participant disconnected");
        std::cout << "[disconnect] node=" << config_.id << " peer=" << id
                  << " fd=" << fd << " reason=remote-close-or-error\n";
        std::cout << "Peer " << id << " disconnected\n";
        membership_dirty_ = true;
        refresh_ring();
    }
}

std::vector<core::NodeInfo> PeerNode::current_membership() const {
    core::NodeInfo self;
    self.id = config_.id;
    self.peer_port = config_.port;
    self.ws_port = config_.ws_port;
    self.status = core::NodeStatus::Alive;
    return core::live_membership(peers_, self);
}

void PeerNode::refresh_ring() {
    // Canonical ownership never depends on HELLO/PING/PONG/NODELIST liveness.
    // Manifest changes require explicit offline handoff and restart (for now).
    if (!placement_ready_) return;
    if (ring_.all_nodes() == placement_.members()) return;
    ring_.rebuild(placement_);
    std::cout << "[placement] node=" << config_.id
              << " epoch=" << placement_.epoch()
              << " partitions=" << core::BalancedPlacement::PARTITIONS
              << " fingerprint=" << placement_.fingerprint()
              << " members=" << placement_.members().size() << '\n';
}

void PeerNode::send_nodelist(int fd) {
    messages::Message msg;
    msg.type = messages::Type::NODELIST;
    msg.payload = messages::encode_nodelist(
        messages::NodeListPayload{current_membership()}
    );
    send_message(fd, msg);
}

void PeerNode::broadcast_nodelist() {
    const auto connected = fd_by_node_;
    for (const auto& [id, fd] : connected) {
        (void)id;
        send_nodelist(fd);
    }
}

std::optional<int> PeerNode::owner_fd(core::NodeId id) const {
    if (const auto it = fd_by_node_.find(id); it != fd_by_node_.end())
        return it->second;
    return std::nullopt;
}

std::uint32_t PeerNode::new_request_id() {
    if(next_request_id_==0 || next_request_id_>=0x40000000)next_request_id_=1;
    return next_request_id_++;
}

void PeerNode::report_browser_error(int fd, const std::string& error) {
    try { ws_server_->send_to(fd, json{{"op","error"},{"message",error}}.dump()); }
    catch (const std::exception&) { /* browser already disconnected */ }
}

void PeerNode::broadcast_pixel(std::uint32_t key, std::uint16_t index,
                               std::uint32_t color) {
    const auto x = (key % core::RegionStore::REGIONS_SIDE) * core::Region::SIDE
                 + index % core::Region::SIDE;
    const auto y = (key / core::RegionStore::REGIONS_SIDE) * core::Region::SIDE
                 + index / core::Region::SIDE;
    ws_server_->broadcast(json{{"op","pixel"},{"x",x},{"y",y},
                               {"color",color_to_hex(color)}}.dump());
}

void PeerNode::relay_pixel_event(const messages::Message& msg,core::NodeId except) {
    std::size_t relayed=0;
    const auto members=placement_.members();
    for(const auto& [peer,fd]:fd_by_node_) {
        if(peer==except || !placement_ready_ ||
           std::find(members.begin(),members.end(),peer)==members.end())continue;
        try {send_message(fd,msg);++relayed;}
        catch(const std::exception& e) {
            std::cerr << "[pixel-event] node=" << config_.id << " relay-failed peer=" << peer << " error=" << e.what() << '\n';
        }
    }
    std::cout << "[pixel-event] node=" << config_.id << " relayed=" << relayed << '\n';
}
void PeerNode::announce_committed_pixel(std::uint32_t key,std::uint16_t index,
                                         const core::VersionedPixel& pixel) {
    const auto pixel_key=key*core::Region::COUNT+index;
    const auto known=last_browser_pixel_versions_.find(pixel_key);
    if(known!=last_browser_pixel_versions_.end() && !pixel.newer_than(known->second)) {
        std::cout << "[pixel-event] node=" << config_.id << " drop=old-local-version region=" << key << " index=" << index << '\n';
        return;
    }
    last_browser_pixel_versions_[pixel_key]=pixel;
    broadcast_pixel(key,index,pixel.color);
    if(!placement_ready_ || frozen_)return;
    PixelEvent e{config_.id,++next_pixel_event_,placement_.fingerprint(),key,index,pixel};
    seen_pixel_events_.accept(e.origin,e.sequence);
    messages::Message m;m.type=messages::Type::PIXEL_EVENT;
    m.payload=encode_pixel_event(e);
    std::cout << "[pixel-event] node=" << config_.id << " origin=" << e.origin
              << " seq=" << e.sequence << " region=" << key << " pixel=" << index
              << " version=" << pixel.lamport << " accepted=W quorum\n";
    relay_pixel_event(m,0);
}
void PeerNode::on_pixel_event(const messages::Message& msg,core::NodeId sender) {
    if(!placement_ready_ || frozen_)return;
    const auto e=decode_pixel_event(msg.payload);
    const auto members=placement_.members();
    if(e.placement_fingerprint!=placement_.fingerprint() ||
       std::find(members.begin(),members.end(),e.origin)==members.end()){
        std::cout << "[pixel-event] node=" << config_.id << " drop=placement-mismatch sender=" << sender << '\n';
        return;
    }
    if(!seen_pixel_events_.accept(e.origin,e.sequence)) {
        std::cout << "[pixel-event] node=" << config_.id << " drop=duplicate origin=" << e.origin << " seq=" << e.sequence << '\n';
        return;
    }
    // Notification delivery does not create a replica or acknowledge a write.
    // Per-pixel LWW guard prevents an older delayed event from visually
    // overwriting a newer event on this peer's connected browser clients.
    const auto pixel_key=e.key*core::Region::COUNT+e.index;
    const auto known=last_browser_pixel_versions_.find(pixel_key);
    if(known==last_browser_pixel_versions_.end() || e.pixel.newer_than(known->second)) {
        last_browser_pixel_versions_[pixel_key]=e.pixel;
        broadcast_pixel(e.key,e.index,e.pixel.color);
    } else {
        std::cout << "[pixel-event] node=" << config_.id << " drop=stale-display-event origin=" << e.origin << " seq=" << e.sequence << '\n';
    }
    std::cout << "[pixel-event] node=" << config_.id << " received-from=" << sender
              << " origin=" << e.origin << " seq=" << e.sequence
              << " region=" << e.key << " pixel=" << e.index << " ws=broadcast\n";
    relay_pixel_event(msg,sender);
}

void PeerNode::handle_peer_disconnect(core::NodeId id) {
    if(replicator_)replicator_->on_disconnect(id);
}

void PeerNode::log_peer_health(core::NodeId id, core::NodeStatus status) {
    auto& last=last_peer_health_status_[id];
    if(last==status)return;
    const char* name[]={"","HEALTHY","SUSPECT","UNAVAILABLE"};
    const char* old_name=name[static_cast<int>(last)];
    if(!*old_name)old_name="HEALTHY";
    // Strategy B measurement input: one tracker event per actual
    // transition (duplicate HEALTHY->HEALTHY calls never reach here).
    const auto now=std::chrono::steady_clock::now();
    reliability_tracker_.observe(id,status==core::NodeStatus::Alive,now);
    std::cout << "[peer-health] node=" << config_.id
              << " peer=" << id
              << ' ' << old_name << "->" << name[static_cast<int>(status)]
              << " placement=" << (placement_ready_ && placement_.contains(id)
                                   ? "member" : "non-member")
              << '\n';
    if(metrics_) {
        const auto s=reliability_tracker_.get(id,now);
        metrics_->health(
            std::chrono::duration<double>(now-process_started_).count(),
            id,name[static_cast<int>(status)],s.observed_seconds,s.score);
    }
    last=status;
    if(status==core::NodeStatus::Alive) {
        unavailable_since_.erase(id);
        reported_suspect_.erase(id);
    }
}

void PeerNode::reset_to_joiner(const std::string& why) {
    if(!placement_ready_ && !staged_ && !frozen_ &&
       pending_browser_puts_.empty())return;
    // Buffered writes belong to the abandoned committed incarnation and must
    // never replay from an unadmitted/stale joiner.
    discard_all_browser_puts(/*membership_reset=*/true);
    const auto old_gen = placement_ready_ ? placement_.epoch() : 0;
    std::cout << "[peer-health] node=" << config_.id
              << " STALE->JOINER local_generation=" << old_gen
              << " reason=" << why << '\n';
    // Discard the stale committed state on disk so a crash before re-commit
    // cannot resurrect the obsolete placement.
    std::error_code ec;
    std::filesystem::remove(config_.state_file, ec);
    std::filesystem::remove(config_.state_file+".pending", ec);
    placement_manager_.abort();
    placement_ready_=false;
    frozen_=false;
    staged_.reset();
    ready_sent_=false;
    phase_=Phase::Idle;
    transition_keys_.clear();
    moves_.clear();
    next_move_=0;
    waiting_.clear();
    transition_peers_.clear();
    membership_queue_.clear();
    removed_peers_.clear();
    removal_transition_=false;
    transition_candidate_=0;
    commit_uncertain_logged_=false;
    // Un-acknowledged local pixels must never overwrite live replicas.
    region_store_.clear();
    if(replicator_)replicator_->reset_repair_state(); // fresh joiner re-pulls
    seen_pixel_events_=PixelEventDedup{};
    last_browser_pixel_versions_.clear();
    // Never reclaim authority: the cluster elects its own leader; this peer
    // re-enters as an ordinary joiner with a fresh admission order.
    authority_id_=0;
    last_elected_token_.clear();
    election_in_progress_=false;
    join_referrals_.clear();
    unavailable_since_.clear();
    reported_suspect_.clear();
    last_peer_health_status_.clear();
    refresh_ring();
}

void PeerNode::finish_snapshot(int browser_fd, const std::string& error) {
    if (!snapshots_.erase(browser_fd)) return;
    if (!error.empty()) report_browser_error(browser_fd, error);
    try { ws_server_->send_to(browser_fd,R"({"op":"canvas_end"})"); }
    catch (const std::exception&) { /* browser closed */ }
}

void PeerNode::deliver_region(int browser_fd, std::uint32_t key,
                              const core::Region& region) {
    constexpr std::size_t CHUNK_SIZE = 100;
    json pixels = json::array();
    for (std::size_t index=0; index<core::Region::COUNT; ++index) {
        const auto& px = region.pixel(index);
        if (px.lamport == 0) continue;
        const auto x = (key % core::RegionStore::REGIONS_SIDE) * core::Region::SIDE
                     + index % core::Region::SIDE;
        const auto y = (key / core::RegionStore::REGIONS_SIDE) * core::Region::SIDE
                     + index / core::Region::SIDE;
        pixels.push_back({{"x",x},{"y",y},{"color",color_to_hex(px.color)}});
        if (pixels.size() >= CHUNK_SIZE) {
            ws_server_->send_to(browser_fd,json{{"op","canvas_chunk"},{"pixels",pixels}}.dump());
            pixels = json::array();
        }
    }
    if (!pixels.empty())
        ws_server_->send_to(browser_fd,json{{"op","canvas_chunk"},{"pixels",pixels}}.dump());
}

void PeerNode::start_snapshot(int browser_fd) {
    if(snapshots_.contains(browser_fd)) {
        report_browser_error(browser_fd,"snapshot already in progress");
        return;
    }
    snapshots_[browser_fd]=Snapshot{};
    ws_server_->send_to(browser_fd,R"({"op":"canvas_begin"})");
    request_next_region(browser_fd);
}
void PeerNode::request_next_region(int browser_fd) {
    auto it=snapshots_.find(browser_fd);
    if(it==snapshots_.end())return;
    if(it->second.next_key>=core::RegionStore::REGION_COUNT) {
        finish_snapshot(browser_fd);return;
    }
    const auto key=it->second.next_key++;
    try {
        replicator_->get(key,[this,browser_fd,key](bool ok,const core::Region& region,const std::string& error){
            if(!snapshots_.contains(browser_fd))return;
            if(!ok){finish_snapshot(browser_fd,error);return;}
            if(region.version()!=0)lamport_clock_.update(region.version());
            try {deliver_region(browser_fd,key,region);}
            catch(const std::exception&){finish_snapshot(browser_fd,"browser snapshot delivery failed");return;}
            request_next_region(browser_fd);
        });
    } catch(const std::exception& e) {finish_snapshot(browser_fd,e.what());}
}

void PeerNode::on_message(net::TcpConnection& conn,
                          std::span<const std::uint8_t> body) {
    const auto msg = messages::decode(body);
    const auto it = node_by_fd_.find(conn.fd());
    const core::NodeId sender = (it == node_by_fd_.end()) ? 0 : it->second;

    // First message on a new connection must identify the peer.
    if (sender == 0 && msg.type != messages::Type::HELLO)
        throw std::runtime_error("expected HELLO before any other message");

    if (auto handler = handlers_.find(msg.type); handler != handlers_.end()) {
        handler->second(conn, msg, sender);
    }
}
//summary:registers callback functions
// for different message types so PeerNode knows what to do when a message arrives.
void PeerNode::register_handlers() {
    handlers_[messages::Type::HELLO] = [this](
        net::TcpConnection& conn, const messages::Message& msg, core::NodeId sender) {
        const auto size=msg.payload.size();
        if (sender != 0 || (size != 16 && size != 17 && size != 20 && size != 21))
            throw std::runtime_error("invalid or duplicate HELLO");

        // 16/17-byte HELLOs predate the committed-generation field (legacy).
        const bool has_epoch = size >= 20;
        const auto remote_stage=(size==17||size==21) ? msg.payload[size-1] : 0;
        const auto id = get_u32(msg.payload, 0);
        const auto port = get_u16(msg.payload, 4);
        const auto ws_port = get_u16(msg.payload, 6);
        const auto remote_fingerprint =
            (static_cast<std::uint64_t>(get_u32(msg.payload,8)) << 32) |
            get_u32(msg.payload,12);
        const auto remote_epoch = has_epoch ? get_u32(msg.payload, 16) : 0;

        // Stale-generation resolution: the cluster moved on while the peer
        // with the LOWER committed generation was down. That side must reset
        // to a fresh joiner session and re-sync; it must never serve or
        // overwrite data under its obsolete placement. (Deliberately
        // committed-epoch only: a frozen participant mid-transition is
        // already the same committed generation as its peers, and a stale
        // staged proposal at an equal epoch is cleared by the reset once the
        // remote's committed epoch exceeds ours.)
        if (placement_ready_ && remote_fingerprint != 0 && remote_epoch != 0) {
            if (remote_epoch < placement_.epoch()) {
                std::cout << "[peer-health] node=" << config_.id
                          << " peer=" << id << " STALE"
                          << " peer_generation=" << remote_epoch
                          << " local_generation=" << placement_.epoch() << '\n';
                throw std::runtime_error("stale peer committed placement");
            }
            if (remote_epoch > placement_.epoch()) {
                std::cout << "[stale-reset] node=" << config_.id
                          << " fd=" << conn.fd()
                          << " local_gen=" << placement_.epoch()
                          << " remote_gen=" << remote_epoch
                          << " remote_peer=" << id
                          << " old_fp=" << placement_.fingerprint()
                          << " state=joiner\n";
                reset_to_joiner("remote generation " + std::to_string(remote_epoch)
                    + " > local " + std::to_string(placement_.epoch()));
            }
        }

        // Never accept a different committed ring silently. A fresh joiner
        // announces fingerprint 0; it must receive a prepared snapshot from
        // the authority before it can serve any pixel request.
        const auto members = placement_ready_ ? placement_.members() : std::vector<core::NodeId>{};
        const bool candidate = std::find(members.begin(),members.end(),id)==members.end()
            && remote_fingerprint == 0;
        const bool joining_self = !placement_ready_;
        if (!candidate && !joining_self &&
            (!placement_ready_ || remote_fingerprint != placement_.fingerprint()))
            throw std::runtime_error("committed placement mismatch; peer is stale");
        if (id == 0 || id == config_.id || port == 0)
            throw std::runtime_error("invalid peer identity");

        // Use the configured seed host when available. The HELLO payload
        // intentionally carries ID/ports but no host string.
        core::NodeInfo info;
        info.id = id;
        info.peer_port = port;
        info.ws_port = ws_port;
        info.status = core::NodeStatus::Alive;
        for (const auto& seed : config_.peers) {
            if (seed.id == id) { info.host = seed.host; break; }
        }
        // A peer that dialed us provides its IP through the TCP socket.
        // HELLO only provides the peer's listening PORT.
        sockaddr_in address{};
        socklen_t length=sizeof(address);
        if (::getpeername(conn.fd(), reinterpret_cast<sockaddr*>(&address), &length)==0) {
            char ip[INET_ADDRSTRLEN]{};
            if (::inet_ntop(AF_INET, &address.sin_addr, ip, sizeof(ip)))
                info.host = ip;
        }

        // One active connection per NodeId. When two peers dial each other
        // concurrently, both ends see a "duplicate" HELLO. Resolve
        // deterministically: keep the connection whose initiator has the
        // smaller node id, so both ends keep the SAME TCP connection instead
        // of each keeping a different one (which would tear both down).
        if (const auto old = fd_by_node_.find(id);
            old != fd_by_node_.end() && old->second != conn.fd()) {
            const int old_fd = old->second;
            const auto old_initiator = dialed_fds_.contains(old_fd) ? config_.id : id;
            const auto new_initiator = dialed_fds_.contains(conn.fd()) ? config_.id : id;
            if (new_initiator < old_initiator) {
                // Adopt the new connection and retire the old one. Update the
                // maps first so the old fd's disconnect callback cannot be
                // mistaken for a peer disconnection (no abort, no failure
                // reporting, no HELLO-bytes lost).
                node_by_fd_.erase(old_fd);
                dialed_fds_.erase(old_fd);
                fd_by_node_[id] = conn.fd();
                loop_.remove(old_fd);
                std::cout << "[peer] replacing duplicate connection to " << id
                          << " old_fd=" << old_fd << " new_fd=" << conn.fd() << '\n';
            } else {
                throw std::runtime_error("duplicate peer connection");
            }
        }
        peers_.upsert(std::move(info));
        node_by_fd_[conn.fd()] = id;
        fd_by_node_[id] = conn.fd();
        last_pong_[id] = std::chrono::steady_clock::now();
        failure_detector_.observe(id, last_pong_[id]);
        reported_unavailable_.erase(id);
        log_peer_health(id, core::NodeStatus::Alive);
        std::cout << "HELLO from peer " << id << " (fd=" << conn.fd() << ")\n";
        membership_dirty_ = true;
        refresh_ring();
        // HELLO precedes NODELIST in each connection's outgoing byte stream.
        send_nodelist(conn.fd());
        // Synchronize the last election result with recovered/joining peers:
        // a former leader must not resume authority, and a fresh joiner must
        // learn the CURRENT elected authority (its default is node 1, which
        // may have been removed long ago).
        if (!last_elected_token_.empty() &&
            (candidate || remote_fingerprint == placement_.fingerprint()))
            send_control(id, messages::Type::RING_ELECTED, last_elected_token_);
        if (candidate && config_.id != authority_id_) {
            // This seed has verified the joiner over TCP. Tell the authority
            // how to establish its OWN authenticated HELLO connection.
            join_referrals_[id] = *peers_.find(id);
            std::cout << "[join-referral] seed=" << config_.id
                      << " candidate=" << id << " via=" << join_referrals_[id].host
                      << ':' << port << " authority=" << authority_id_ << '\n';
            next_join_referral_ = std::chrono::steady_clock::time_point{};
            forward_join_referrals();
        }
        if (config_.id == authority_id_) {
            broadcast_endpoints();
            if (remote_stage==1 && phase_==Phase::Idle &&
                (remote_fingerprint==0 ||
                 (placement_ready_ && remote_fingerprint==placement_.fingerprint()))) {
                // Authority restarted before commit; previously prepared
                // participants can safely abort to the last committed view.
                send_control(id,messages::Type::PLACEMENT_ABORT);
            }
            if (remote_stage==2 && phase_==Phase::Idle && placement_ready_ &&
                remote_fingerprint==placement_.fingerprint()) {
                // Peer committed the current generation but crashed before
                // RELEASE. Re-sending RELEASE is idempotent and unfreezes it.
                std::cout << "[membership] recovery: re-release to committed peer "
                          << id << " generation " << placement_.epoch() << '\n';
                const auto fp = placement_.fingerprint();
                std::vector<std::uint8_t> tag;
                for (int i = 7; i >= 0; --i)
                    tag.push_back(static_cast<std::uint8_t>(fp >> (i * 8)));
                send_control(id,messages::Type::PLACEMENT_RELEASE,std::move(tag));
            }
            if (candidate) schedule_join(id);
        }
    };

    handlers_[messages::Type::PING] = [this](
        net::TcpConnection& conn, const messages::Message& msg, core::NodeId sender) {
        if (!msg.payload.empty())
            throw std::runtime_error("unexpected PING payload");
        messages::Message pong;
        pong.type = messages::Type::PONG;
        pong.flags = messages::IS_RESPONSE;
        pong.request_id = msg.request_id;
        send_message(conn.fd(), pong);
        peers_.mark(sender, core::NodeStatus::Alive);
        refresh_ring();
    };

    handlers_[messages::Type::PONG] = [this](
        net::TcpConnection&, const messages::Message& msg, core::NodeId sender) {
        if (!msg.payload.empty())
            throw std::runtime_error("unexpected PONG payload");
        last_pong_[sender] = std::chrono::steady_clock::now();
        failure_detector_.observe(sender,last_pong_[sender]);
        reported_unavailable_.erase(sender);
        peers_.mark(sender, core::NodeStatus::Alive);
        log_peer_health(sender, core::NodeStatus::Alive);
        refresh_ring();
    };

    handlers_[messages::Type::NODELIST] = [this](
        net::TcpConnection&, const messages::Message& msg, core::NodeId sender) {
        if (sender == 0 || (msg.flags & messages::IS_RESPONSE)) {
            throw std::runtime_error("invalid NODELIST sender or flags");
        }
        const auto reported = messages::decode_nodelist(msg.payload);
        if (core::merge_gossip(peers_, config_.id, reported.peers)) {
            membership_dirty_ = true;
            refresh_ring();
        }
    };

    handlers_[messages::Type::RING_ELECTION] = [this](
        net::TcpConnection&, const messages::Message& msg, core::NodeId sender) {
        on_ring_election(msg, sender);
    };
    handlers_[messages::Type::RING_ELECTED] = [this](
        net::TcpConnection&, const messages::Message& msg, core::NodeId sender) {
        on_ring_elected(msg, sender);
    };

    handlers_[messages::Type::PIXEL_EVENT] = [this](net::TcpConnection&,
        const messages::Message& msg,core::NodeId sender){on_pixel_event(msg,sender);};

    // Strategy C control/metrics: per-peer write-count deltas (the authority
    // aggregates; non-authority peers drop them).
    handlers_[messages::Type::REGION_WRITE_COUNTS] = [this](net::TcpConnection&,
        const messages::Message& msg,core::NodeId sender){
        handle_write_report(sender,msg.payload);
    };

    // Requests are always sent by an authenticated neighboring peer.
    for (auto type : {messages::Type::PLACEMENT_PREPARE, messages::Type::PLACEMENT_READY,
                      messages::Type::PLACEMENT_COMMIT, messages::Type::PLACEMENT_COMMITTED,
                      messages::Type::PLACEMENT_RELEASE, messages::Type::JOIN_REQUEST,
                      messages::Type::LEAVE_REQUEST, messages::Type::PLACEMENT_ABORT,
                      messages::Type::PEER_ENDPOINTS, messages::Type::POLICY_SNAPSHOT}) {
        handlers_[type] = [this](net::TcpConnection& conn,
               const messages::Message& msg, core::NodeId sender) {
            on_placement_message(conn,msg,sender);
        };
    }
    for(auto type : {messages::Type::HANDOFF_GET, messages::Type::HANDOFF_DATA,
                     messages::Type::HANDOFF_PUT, messages::Type::HANDOFF_OK}) {
        handlers_[type] = [this](net::TcpConnection& conn,
               const messages::Message& msg, core::NodeId sender) {
            on_handoff_message(conn,msg,sender);
        };
    }

    for(auto type:{messages::Type::PUT,messages::Type::PUT_OK,
                   messages::Type::GET,messages::Type::GET_OK,
                   messages::Type::REGION_PUT,messages::Type::ERROR}) {
        handlers_[type]=[this](net::TcpConnection&,const messages::Message& msg,core::NodeId sender){
            if(frozen_ || !placement_ready_) {
                // Allow outstanding replies while draining a transition.
                if(!(msg.flags & messages::IS_RESPONSE)) {
                    // Best-effort anti-entropy repair expects no reply; an
                    // ERROR with its request_id 0 would pollute the sender's
                    // quorum accounting ([quorum-ignore] request=0 noise).
                    if(msg.type==messages::Type::REGION_PUT)return;
                    messages::Message error;
                    error.type=messages::Type::ERROR;
                    error.flags=messages::IS_RESPONSE;
                    error.request_id=msg.request_id;
                    if(const auto fd=owner_fd(sender))send_message(*fd,error);
                    return;
                }
            }
            if(msg.type==messages::Type::PUT && !(msg.flags & messages::IS_RESPONSE)) {
                const auto payload=messages::decode_put(msg.payload);
                lamport_clock_.update(payload.pixel.lamport);
            }
            replicator_->on_message(sender,msg);
        };
    }

}
//this is called when a new connection is accepted. 
//It creates a TcpConnection object, registers it with the EventLoop, 
//and sends a HELLO message to the new peer.
void PeerNode::on_accepted(net::Socket client) {
    // Avoid 40ms delayed-ACK/Nagle stalls in the serial region handoff RPCs.
    int no_delay=1;
    ::setsockopt(client.get(),IPPROTO_TCP,TCP_NODELAY,&no_delay,sizeof(no_delay));
    auto conn = std::make_unique<net::TcpConnection>(
        std::move(client),
        std::make_unique<net::ProtocolCodec>(),
        [this](net::TcpConnection& c, std::span<const std::uint8_t> bytes) {
            on_message(c, bytes);
        }
    );
    const int fd = conn->fd();
    loop_.add(std::move(conn));
    send_hello(fd);
}

void PeerNode::on_tick() {
    const auto now = std::chrono::steady_clock::now();
    // Iterate a snapshot in case callbacks or disconnect handling change membership.
    const auto connected = fd_by_node_;
    for (const auto& [id, fd] : connected) {
        auto seen = last_pong_.find(id);
        if (seen != last_pong_.end() &&
            now - seen->second > std::chrono::seconds(config_.suspect_after_seconds) &&
            peers_.find(id)->status == core::NodeStatus::Alive) {
            peers_.mark(id, core::NodeStatus::Suspect);
            log_peer_health(id, core::NodeStatus::Suspect);
            membership_dirty_ = true;
            refresh_ring();
        }

        messages::Message ping;
        ping.type = messages::Type::PING;
        ping.request_id = next_request_id_++;
        send_message(fd, ping);
    }

    // Failure detection never edits canonical ownership by itself: the
    // UNAVAILABLE state is only a signal to the membership authority.
    for (auto id: failure_detector_.unavailable(now)) {
        auto info=peers_.find(id);
        if(info && info->status!=core::NodeStatus::Dead) {
            peers_.mark(id, core::NodeStatus::Dead);
            log_peer_health(id, core::NodeStatus::Dead);
            membership_dirty_ = true;
            std::cout << "[churn] node=" << id
                      << " unavailable; awaiting authority-led removal\n";
        }
    }
    schedule_auto_removal_tick(now);
    // Send promptly following a membership change and at least every 5 seconds.
    // Do not reset the periodic deadline on a change-triggered broadcast.
    const bool periodic = now >= next_gossip_;
    if (membership_dirty_ || periodic) {
        broadcast_nodelist();
        if (config_.id == authority_id_)
            broadcast_endpoints();
        membership_dirty_ = false;
        if (periodic) {
            next_gossip_ = now + 5s;
        }
    }
    connect_discovered_peers();
    election_tick();
    forward_join_referrals();
    // Lab 06 shared repair path; do not run across in-progress reconfiguration.
    if (placement_ready_ && !frozen_ && phase_==Phase::Idle &&
        now>=next_repair_ && replicator_) {
        replicator_->anti_entropy_step();
        next_repair_=now+std::chrono::seconds(2);
    }
    if(config_.leave_after_seconds>0 && !leave_requested_ && placement_ready_ &&
       config_.id!=authority_id_ &&
       now-started_>=std::chrono::seconds(config_.leave_after_seconds) &&
       owner_fd(authority_id_)) {
        send_control(authority_id_,messages::Type::LEAVE_REQUEST);
        leave_requested_=true;
    }
    // Strategy C: per-peer write-count delta reports to the authority.
    if(config_.strategy==Config::Strategy::C && placement_ready_ &&
       now>=next_write_report_) {
        send_write_report(now);
        next_write_report_=now+
            std::chrono::seconds(config_.write_report_interval_seconds);
    }
    // Strategy B: the authority re-evaluates the shared policy snapshot on
    // a fixed interval (all peers apply the same table — no local decisions).
    if(policy_evaluator_ && config_.id==authority_id_ && placement_ready_ &&
       !frozen_ && phase_==Phase::Idle && now>=next_policy_eval_) {
        next_policy_eval_=now+std::chrono::seconds(config_.policy_interval_seconds);
        evaluate_and_broadcast_policy(now,false);
    }
    // Policy-activation backfill: pull one queued region per tick.
    if(replicator_ && placement_ready_ && !frozen_ && phase_==Phase::Idle)
        replicator_->backfill_step();
    if (config_.id == authority_id_) progress_transition();
    else send_ready_if_quiescent();
    // Safe replay runs only AFTER transition progression finalized this
    // tick's frozen/phase flags (successful RELEASE, safe ABORT, or a
    // completed removal). COMMIT UNCERTAIN leaves frozen_=true and never
    // reaches the queue.
    drain_browser_puts();
}

void PeerNode::send_write_report(std::chrono::steady_clock::time_point now) {
    if(!placement_ready_)return;
    const auto delta=write_tracker_.drain_delta(now);
    last_own_delta_=delta.counts;
    const auto payload=core::write_report::encode(
        config_.id,write_report_incarnation_,++write_report_sequence_,delta);
    if(config_.id==authority_id_) {
        // The authority aggregates its own local delta directly (no self
        // message), using the same codec path for dedup/validation.
        core::write_report::Decoded d;
        if(core::write_report::decode(payload,d))
            handle_write_report(config_.id,payload);
        return;
    }
    const auto fd=owner_fd(authority_id_);
    if(!fd)return; // authority unreachable; interval is lost (logged as a gap)
    messages::Message m;
    m.type=messages::Type::REGION_WRITE_COUNTS;
    m.request_id=0; // control/metrics broadcast; no quorum accounting
    m.payload=payload;
    try {send_message(*fd,m);}
    catch(const std::exception& e) {
        std::cerr << "[write-report] node=" << config_.id
                  << " send_failed error=" << e.what() << '\n';
        return;
    }
    std::cout << "[write-report] node=" << config_.id
              << " sent seq=" << write_report_sequence_
              << " bytes=" << payload.size() << '\n';
}

void PeerNode::handle_write_report(core::NodeId sender,
                                   const std::vector<std::uint8_t>& payload) {
    if(config_.strategy!=Config::Strategy::C)return; // A/B never aggregate
    if(config_.id!=authority_id_)return; // only the authority aggregates
    core::write_report::Decoded d;
    if(!core::write_report::decode(payload,d)) {
        std::cerr << "[write-report] node=" << config_.id
                  << " malformed from=" << sender << '\n';
        return;
    }
    if(d.node!=sender) {
        std::cerr << "[write-report] node=" << config_.id
                  << " node-mismatch transport=" << sender
                  << " payload=" << d.node << '\n';
        return;
    }
    if(!peers_.find(d.node) && d.node!=config_.id) {
        std::cerr << "[write-report] node=" << config_.id
                  << " unknown-peer=" << d.node << '\n';
        return;
    }
    std::string reason;
    const bool ok=write_aggregate_.accept(d,std::chrono::steady_clock::now(),
                                          &reason);
    std::cout << "[write-report] node=" << config_.id << " from=" << d.node
              << " incarnation=" << d.incarnation
              << " seq=" << d.sequence
              << " interval_ms=" << (d.end_ns-d.start_ns)/1000000
              << " accepted=" << (ok?"true":"false")
              << (!ok && !reason.empty() ? " reason="+reason : "") << '\n';
    if(metrics_)metrics_->write_report(d.node,d.incarnation,d.sequence,
                                       d.start_ns,d.end_ns,ok,reason);
}

void PeerNode::run() {
    started_=std::chrono::steady_clock::now();
    server_ = std::make_unique<net::TcpServer>(config_.port);
    // register this function to eventloop,
    // which will be called when Incoming connections on listener.
    loop_.register_listener(server_->fd(),

        [this] {
        // Accept all pending connections in tcp_server.cpp, 
        //then transfer the ownership back to the PeerNode.
        server_->accept_ready([this](net::Socket socket) {
            on_accepted(std::move(socket));
        });
    });
    // New Lab 03 browser listener.
    ws_server_ = std::make_unique<ui::WebSocketServer>(
        loop_,
        config_.ws_port,
        [this](int fd, std::string_view json) {
            on_browser_command(fd, json);
        },
        // A session ended: drop its private buffered PUTs immediately. The
        // (fd, generation) pair prevents touching a newer session that later
        // reuses the descriptor.
        [this](int fd, std::uint64_t generation) {
            discard_browser_puts(fd, generation, /*membership_reset=*/false);
        }
    );


         // 3. Connect to configured peers
    for (const auto& peer : config_.peers) {
        if (peer.id == config_.id)
            continue;

        try {
            auto result = net::TcpClient::connect_to(
                peer.host,
                peer.peer_port
            );

            if (result.status == net::ConnectStatus::Connected) {
                // TCP connection established immediately
                dialed_fds_.insert(result.socket.get());
                on_accepted(std::move(result.socket));
            }
            else {
                // EINPROGRESS: let EventLoop finish connecting
                loop_.add_pending_connection(
                    std::move(result.socket),

                    // Success callback
                    [this](net::Socket connected) {
                        dialed_fds_.insert(connected.get());
                        on_accepted(std::move(connected));
                    },

                    // Failure callback
                    [id = peer.id](int error) {
                        std::cerr
                            << "Connection to peer " << id
                            << " failed: "
                            << std::system_category().message(error)
                            << '\n';
                    }
                );
            }
        }
        catch (const std::exception& e) {
            std::cerr
                << "Failed to connect to peer "
                << peer.id << ": "
                << e.what() << '\n';
        }
    }

    std::cout << "Peer " << config_.id << " listening on port "
              << config_.port << '\n';
    loop_.run([this] { on_tick(); });
}


    void PeerNode::on_browser_command(
        int fd,
        std::string_view message
    ) {
        try {
            const auto command = json::parse(message);

            if (!command.is_object()) {
                throw std::invalid_argument(
                    "Command must be a JSON object"
                );
            }

            const std::string op =
                command.at("op").get<std::string>();

            if (op == "put") {
                // Parse into signed/wider integers first
                // so out-of-range values cannot wrap.
                const auto x =
                    command.at("x").get<std::int64_t>();

                const auto y =
                    command.at("y").get<std::int64_t>();

                const auto color_string =
                    command.at("color").get<std::string>();

                if (x < 0 || x >= CANVAS_SIZE  ||
                    y < 0 || y >= CANVAS_SIZE ) {
                    throw std::out_of_range(
                        "Pixel coordinates out of range"
                    );
                }

                const auto color =
                    parse_color(color_string);

                // Resolve the exact session instance now: a queued PUT keeps
                // this token so descriptor reuse cannot misdeliver it later.
                const auto generation =
                    ws_server_->session_generation(fd);
                if (!generation) {
                    std::cerr << "[WebSocket] dropping PUT from vanished fd="
                              << fd << '\n';
                    return;
                }

                handle_put(
                    fd,
                    *generation,
                    static_cast<std::uint16_t>(x),
                    static_cast<std::uint16_t>(y),
                    color
                );
            }
            else if (op == "leave") {
                // Development-only administrative operation. Restrict access
                // to trusted local clients before deploying publicly.
                if (!placement_ready_) throw std::runtime_error("not a member");
                if (config_.id == authority_id_)
                    schedule_leave(config_.id);
                else send_control(authority_id_, messages::Type::LEAVE_REQUEST);
            }
            else if (op == "get_canvas") {
                handle_get_canvas(fd);
            }
            else {
                ws_server_->send_to(
                    fd,
                    json{
                        {"op", "error"},
                        {"message", "Unsupported operation"}
                    }.dump()
                );
            }
        }
        catch (const std::exception& e) {
            ws_server_->send_to(
                fd,
                json{
                    {"op", "error"},
                    {"message", e.what()}
                }.dump()
            );
        }
    }


    bool PeerNode::queue_browser_put(int browser_fd,
                                     std::uint64_t session_generation,
                                     std::uint16_t x,std::uint16_t y,
                                     std::uint32_t color) {
        if(pending_browser_puts_.size()>=MAX_PENDING_BROWSER_PUTS)
            return false;
        pending_browser_puts_.push_back(PendingBrowserPut{
            browser_fd,session_generation,x,y,color,
            std::chrono::steady_clock::now()});
        ++put_buffer_enqueued_;
        std::cout << "[put-buffer] node=" << config_.id << " event=enqueue"
                  << " depth=" << pending_browser_puts_.size()
                  << " x=" << x << " y=" << y
                  << " gen=" << session_generation << '\n';
        return true;
    }

    void PeerNode::discard_browser_puts(int browser_fd,
                                        std::uint64_t session_generation,
                                        bool membership_reset) {
        std::size_t discarded=0;
        for(auto it=pending_browser_puts_.begin();
            it!=pending_browser_puts_.end();) {
            if(it->browser_fd==browser_fd &&
               it->session_generation==session_generation) {
                it=pending_browser_puts_.erase(it);
                ++discarded;
            } else {
                ++it;
            }
        }
        if(discarded==0)return;
        if(membership_reset)
            put_buffer_discarded_membership_reset_+=discarded;
        else
            put_buffer_discarded_disconnected_+=discarded;
        std::cout << "[put-buffer] node=" << config_.id
                  << " event=discard"
                  << (membership_reset?" reason=membership_reset"
                                      :" reason=disconnected")
                  << " count=" << discarded
                  << " fd=" << browser_fd
                  << " gen=" << session_generation
                  << " depth=" << pending_browser_puts_.size() << '\n';
    }

    void PeerNode::discard_all_browser_puts(bool membership_reset) {
        if(pending_browser_puts_.empty())return;
        const auto discarded=pending_browser_puts_.size();
        pending_browser_puts_.clear();
        if(membership_reset)
            put_buffer_discarded_membership_reset_+=discarded;
        else
            put_buffer_discarded_disconnected_+=discarded;
        std::cout << "[put-buffer] node=" << config_.id
                  << " event=discard_all"
                  << (membership_reset?" reason=membership_reset"
                                      :" reason=disconnected")
                  << " count=" << discarded << " depth=0\n";
    }

    void PeerNode::report_browser_error_to_session(
        int browser_fd,std::uint64_t session_generation,
        const std::string& error) {
        if(!ws_server_ ||
           !ws_server_->session_is_open(browser_fd,session_generation)) {
            // Never deliver an error (or later an ACK) to a different browser
            // that happened to inherit the same descriptor number.
            std::cout << "[put-buffer] node=" << config_.id
                      << " event=delivery_skipped reason=session_closed"
                      << " fd=" << browser_fd
                      << " gen=" << session_generation << '\n';
            return;
        }
        try {
            const bool delivered=ws_server_->send_to(
                browser_fd,json{{"op","error"},{"message",error}}.dump());
            if(!delivered) {
                std::cerr << "[put-buffer] node=" << config_.id
                          << " event=delivery_failed fd=" << browser_fd
                          << " gen=" << session_generation << '\n';
            }
        }
        catch(const std::exception& e) {
            std::cerr << "[put-buffer] node=" << config_.id
                      << " event=delivery_failed fd=" << browser_fd
                      << " gen=" << session_generation
                      << " error=" << e.what() << '\n';
        }
    }

    void PeerNode::execute_browser_put(int fd,std::uint64_t session_generation,
                                       std::uint16_t x,std::uint16_t y,
                                       std::uint32_t color,bool replayed) {
        const auto key=core::RegionStore::region_key(x,y);
        const auto index=core::RegionStore::pixel_index(x,y);
        // Timestamp at EXECUTION time, never at queue time: the queued PUT
        // must participate in the current committed placement and LWW order.
        const core::VersionedPixel pixel{color,lamport_clock_.tick(),config_.id};
        if(replayed) {
            ++put_buffer_replayed_;
            std::cout << "[put-buffer] node=" << config_.id
                      << " event=replay depth=" << pending_browser_puts_.size()
                      << " gen=" << session_generation
                      << " region=" << key << " index=" << index
                      << " version=" << pixel.lamport << '\n';
        }
        try {
            const auto request=replicator_->put(key,index,pixel,
                [this,fd,session_generation,key,index,pixel,replayed]
                (bool ok,const std::string& error){
                if(ok) {
                    std::cout << "[ws-put] node=" << config_.id << " quorum=success region=" << key << " index=" << index << '\n';
                    if(replayed) {
                        ++put_buffer_replay_quorum_success_;
                        std::cout << "[put-buffer] node=" << config_.id
                                  << " event=quorum_ok gen=" << session_generation
                                  << " region=" << key << '\n';
                    }
                    announce_committed_pixel(key,index,pixel);
                } else {
                    std::cerr << "[ws-put] node=" << config_.id << " quorum=failed region=" << key << " error=" << error << '\n';
                    if(replayed) {
                        ++put_buffer_replay_quorum_failed_;
                        std::cout << "[put-buffer] node=" << config_.id
                                  << " event=quorum_failed gen=" << session_generation
                                  << " region=" << key
                                  << " error=" << error << '\n';
                    }
                    report_browser_error_to_session(fd,session_generation,error);
                }
            });
            if(replayed) {
                std::cout << "[put-buffer] node=" << config_.id
                          << " event=submitted request=" << request
                          << " region=" << key << " index=" << index
                          << " color=" << color_to_hex(color)
                          << " version=" << pixel.lamport << '\n';
            } else {
                std::cout << "[ws-put] node=" << config_.id << " request=" << request
                          << " x=" << x << " y=" << y << " region=" << key
                          << " index=" << index << " color=" << color_to_hex(color)
                          << " version=" << pixel.lamport << '\n';
            }
        }catch(const std::exception& e){
            if(replayed)++put_buffer_replay_quorum_failed_;
            report_browser_error_to_session(fd,session_generation,e.what());
        }
    }

    void PeerNode::drain_browser_puts() {
        if(pending_browser_puts_.empty())return;
        // A queued PUT may execute only on a committed, fully released
        // placement. Frozen, unadmitted and COMMIT-UNCERTAIN states fail
        // closed; the queue survives until the next safe tick.
        if(!placement_ready_ || frozen_ || phase_!=Phase::Idle || !replicator_)
            return;
        const auto now=std::chrono::steady_clock::now();
        const auto oldest_age_ms=pending_browser_puts_.empty()?0:
            std::chrono::duration_cast<std::chrono::milliseconds>(
                now-pending_browser_puts_.front().enqueued_at).count();
        std::size_t submitted=0;
        std::size_t discarded=0;
        while(!pending_browser_puts_.empty() &&
              submitted<MAX_REPLAY_PER_TICK) {
            // A callback (or a re-entrant path) may begin a new transition.
            if(!placement_ready_ || frozen_ || phase_!=Phase::Idle)break;
            PendingBrowserPut request=std::move(pending_browser_puts_.front());
            pending_browser_puts_.pop_front();
            if(!ws_server_ ||
               !ws_server_->session_is_open(request.browser_fd,
                                            request.session_generation)) {
                ++put_buffer_discarded_disconnected_;
                ++discarded;
                std::cout << "[put-buffer] node=" << config_.id
                          << " event=discard reason=disconnected"
                          << " fd=" << request.browser_fd
                          << " gen=" << request.session_generation
                          << " depth=" << pending_browser_puts_.size() << '\n';
                continue;
            }
            const auto waited_ms=std::chrono::duration_cast<
                std::chrono::milliseconds>(now-request.enqueued_at).count();
            std::cout << "[put-buffer] node=" << config_.id
                      << " event=dequeue waited_ms=" << waited_ms
                      << " gen=" << request.session_generation << '\n';
            execute_browser_put(request.browser_fd,
                                request.session_generation,
                                request.x,request.y,request.color,
                                /*replayed=*/true);
            ++submitted;
        }
        std::cout << "[put-buffer] node=" << config_.id
                  << " event=tick submitted=" << submitted
                  << " discarded=" << discarded
                  << " depth=" << pending_browser_puts_.size()
                  << " oldest_age_ms=" << oldest_age_ms << '\n';
    }

    void PeerNode::handle_put(int fd,std::uint64_t session_generation,
                              std::uint16_t x,std::uint16_t y,
                              std::uint32_t color) {
        // A joining/new/unadmitted gateway must not enqueue speculative
        // writes: only an already committed peer may buffer during a
        // placement transition.
        if(!placement_ready_) {
            report_browser_error(fd,"placement transition or join in progress");
            return;
        }
        const bool needs_buffer=frozen_;
        if(needs_buffer &&
           !queue_browser_put(fd,session_generation,x,y,color)) {
            ++put_buffer_full_rejected_;
            std::cout << "[put-buffer] node=" << config_.id
                      << " event=reject reason=full depth="
                      << pending_browser_puts_.size() << '\n';
            report_browser_error_to_session(fd,session_generation,
                                            "PUT buffer full");
            return;
        }
        // Strategy C: exactly ONE attempted client PUT per valid original
        // command, counted at admission (enqueue included), never again at
        // replay. Quorum outcomes, forwards and repairs never count here.
        if(config_.strategy==Config::Strategy::C)
            write_tracker_.record_put_attempt(
                core::RegionStore::region_key(x,y),
                std::chrono::steady_clock::now());
        if(needs_buffer)return; // no tick, no quorum call, no ACK
        execute_browser_put(fd,session_generation,x,y,color,
                            /*replayed=*/false);
    }

    void PeerNode::handle_get_canvas(int fd) {
        if(frozen_ || !placement_ready_) {
            report_browser_error(fd,"placement transition or join in progress");
            return;
        }
        start_snapshot(fd);
    }


} // namespace p2place::peer


