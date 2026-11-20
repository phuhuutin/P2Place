/** @file test_backfill_priority.cpp
 * @brief Regression tests for policy-activation priority scheduling.
 *
 * Confirmed defect (blind fault run 2026-11-17): policy activation queued
 * every owned region at normal priority in an ordered set, so a newly
 * selected N4 target with a high region id (192) sat behind ~170 low-id
 * initial-sync pulls (one region per 500 ms tick) and never synced before
 * the 60-second write window decayed the policy back to N2.
 *
 * Fix under test: urgent FIFO for newly selected N-increase targets, normal
 * queue for initial sync / reconciliation; urgent preferred with a bounded
 * burst (kUrgentBurst) for normal-work fairness; promotion from normal to
 * urgent; dedup across both queues; same-queue retries; stale-epoch drop.
 * Plus the pure activation-decision rule (only NEW suffix positions urgent).
 */
#include "replication/replication_manager.hpp"
#include "core/balanced_placement.hpp"
#include "core/replication_policy.hpp"
#include "messages/payloads.hpp"
#include <array>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <utility>
using namespace p2place;
namespace {
void check(bool x, const char* why) { if (!x) throw std::runtime_error(why); }

struct SwitchStrategy final : public replication::ReplicationStrategy {
    std::size_t n_;
    explicit SwitchStrategy(std::size_t n) : n_(n) {}
    replication::QuorumConfig plan(std::uint32_t) const override { return {n_, 2, 2}; }
    std::size_t max_targets() const override { return 4; }
};

struct Cluster {
    struct Packet { core::NodeId src, dst; messages::Message msg; };
    core::ConsistentHashRing ring;
    std::array<core::RegionStore, 4> stores;
    std::array<std::unique_ptr<replication::ReplicationStrategy>, 4> strategies;
    std::array<std::unique_ptr<replication::ReplicationManager>, 4> nodes;
    std::deque<Packet> packets;
    std::array<bool, 4> alive = {true, true, true, true};
    std::array<std::map<net::TimerQueue::Token, std::function<void()>>, 4> timers;
    std::array<net::TimerQueue::Token, 4> next = {1, 1, 1, 1};
    std::size_t plan_n = 4;
    std::vector<std::tuple<std::uint32_t, std::string, std::string, unsigned>>
        progress;  // (region, event, classification, attempts)
    Cluster() {
        ring.rebuild(core::BalancedPlacement::create({1, 2, 3, 4}));
        for (int i = 0; i < 4; ++i) {
            strategies[i] = std::make_unique<SwitchStrategy>(plan_n);
            nodes[i] = std::make_unique<replication::ReplicationManager>(
                i + 1, ring, stores[i], *strategies[i],
                [this, i](core::NodeId dst, const messages::Message& msg) {
                    if (!alive[i] || !alive[dst - 1]) return false;
                    packets.push_back({static_cast<core::NodeId>(i + 1), dst, msg});
                    return true;
                },
                [this, i](std::function<void()> cb) {
                    auto t = next[i]++; timers[i][t] = std::move(cb); return t;
                },
                [this, i](auto id) { timers[i].erase(id); });
            nodes[i]->set_repair_progress(
                [this](std::uint32_t region, const char* event,
                       const std::string& cls, unsigned attempts) {
                    progress.emplace_back(region, event, cls, attempts);
                });
        }
    }
    void pump() {
        int steps = 0;
        while (!packets.empty()) {
            check(++steps < 100000, "message loop");
            auto p = std::move(packets.front());
            packets.pop_front();
            if (alive[p.dst - 1]) nodes[p.dst - 1]->on_message(p.src, p.msg);
        }
    }
    // Region of the first GET message currently queued (or 0 with no GETs).
    std::uint32_t first_get_region() const {
        for (const auto& p : packets)
            if (p.msg.type == messages::Type::GET)
                return messages::decode_get(p.msg.payload).key;
        return 0xFFFFFFFFu;
    }
    std::vector<std::uint32_t> get_regions() const {
        std::vector<std::uint32_t> out;
        for (const auto& p : packets)
            if (p.msg.type == messages::Type::GET)
                out.push_back(messages::decode_get(p.msg.payload).key);
        return out;
    }
    void clear_packets() { packets.clear(); }
    int count_progress(std::uint32_t region, const std::string& event) const {
        int n = 0;
        for (const auto& [r, e, c, a] : progress)
            if (r == region && e == event) ++n;
        return n;
    }
};

std::vector<std::uint32_t> step_regions(Cluster& c, int steps) {
    std::vector<std::uint32_t> order;
    for (int i = 0; i < steps; ++i) {
        if (!c.nodes[0]->backfill_step()) break;
        // Identify the processed region from the started event this step
        // emitted (dispatch queues no packets when every source is dead).
        std::uint32_t region = 0xFFFFFFFFu;
        for (auto it = c.progress.rbegin(); it != c.progress.rend(); ++it) {
            if (std::get<1>(*it) == "backfill_started") {
                region = std::get<0>(*it);
                break;
            }
        }
        order.push_back(region);
        c.pump();
    }
    return order;
}
}

int main() {
    try {
        // ---- 1. A newly selected region jumps the normal backlog ---------
        {
            Cluster c;
            c.nodes[0]->request_backfill(1);
            c.nodes[0]->request_backfill(2);
            c.nodes[0]->request_backfill(3);
            c.nodes[0]->request_backfill(192);
            c.nodes[0]->request_priority_backfill(192);
            const auto order = step_regions(c, 1);
            check(order.size() == 1 && order[0] == 192,
                  "urgent region 192 must be processed first, not the lowest id");
            // The urgent enqueue event was reported exactly once.
            check(c.count_progress(192, "urgent_enqueued") == 1,
                  "urgent_enqueued must be reported once");
        }
        std::cout << "PASS priority: urgent 192 precedes normal regions [1,2,3,192]\n";

        // ---- 2. Dedup and promotion --------------------------------------
        {
            Cluster c;
            c.nodes[0]->request_priority_backfill(192);
            c.nodes[0]->request_priority_backfill(192);  // duplicate urgent
            auto order = step_regions(c, 1);
            check(order.size() == 1 && order[0] == 192, "urgent request processed");
            // Nothing left: the duplicate did not create a second entry.
            check(!c.nodes[0]->backfill_step(), "duplicate urgent must not re-queue");
            check(c.count_progress(192, "urgent_enqueued") == 1,
                  "duplicate urgent must not report a second enqueue");
        }
        {
            Cluster c;
            c.nodes[0]->request_backfill(192);          // normal queue first
            c.nodes[0]->request_priority_backfill(192); // promoted to urgent
            auto order = step_regions(c, 1);
            check(order.size() == 1 && order[0] == 192, "promotion processed region");
            check(!c.nodes[0]->backfill_step(),
                  "promotion must leave exactly one request (no normal leftover)");
        }
        std::cout << "PASS priority: dedup + normal->urgent promotion\n";

        // ---- 3. Urgent retries never starve normal work -------------------
        {
            Cluster c;
            // Two urgent regions whose pulls can never succeed (all remote
            // sources dead) plus normal backlog. Every failure retries the
            // same urgent region at the BACK of the FIFO, and the burst cap
            // (4) forces a normal region through.
            c.alive[1] = c.alive[2] = c.alive[3] = false;
            c.nodes[0]->request_backfill(1);
            c.nodes[0]->request_backfill(2);
            c.nodes[0]->request_backfill(3);
            c.nodes[0]->request_priority_backfill(192);
            c.nodes[0]->request_priority_backfill(200);
            const auto order = step_regions(c, 12);
            check(order.size() >= 5, "enough steps executed");
            for (int i = 0; i < 4; ++i)
                check(order[i] == 192 || order[i] == 200,
                      "first four steps must be urgent work");
            check(order[4] == 1 || order[4] == 2 || order[4] == 3,
                  "after the urgent burst, a normal region must run");
        }
        std::cout << "PASS priority: bounded urgent burst gives normal work a slot\n";

        // ---- 4. Activation rule: only NEW suffix positions are urgent ------
        {
            using core::ActivationAction;
            using core::policy_activation_action;
            // N2 -> N4: positions 0,1 stay targets (reconcile), 2,3 are new.
            check(policy_activation_action(2, 4, 0) == ActivationAction::reconcile,
                  "existing target on increase reconciles");
            check(policy_activation_action(2, 4, 1) == ActivationAction::reconcile,
                  "existing target on increase reconciles");
            check(policy_activation_action(2, 4, 2) == ActivationAction::urgent,
                  "position 2 is newly selected by N4");
            check(policy_activation_action(2, 4, 3) == ActivationAction::urgent,
                  "position 3 is newly selected by N4");
            // N3 -> N4: position 2 was already a target, only 3 is new.
            check(policy_activation_action(3, 4, 2) == ActivationAction::reconcile,
                  "N3 target reconciles on N4 increase");
            check(policy_activation_action(3, 4, 3) == ActivationAction::urgent,
                  "position 3 is newly selected by the N4 increase");
            // Shrink: remaining targets reconcile; dropped members do nothing.
            check(policy_activation_action(4, 2, 0) == ActivationAction::reconcile,
                  "shrink keeps remaining targets reconciling");
            check(policy_activation_action(4, 2, 2) == ActivationAction::none,
                  "dropped member takes no action");
            // Unchanged set: nothing to do (no heartbeat floods).
            check(policy_activation_action(2, 2, 0) == ActivationAction::none,
                  "unchanged N must not enqueue");
            check(policy_activation_action(4, 4, 3) == ActivationAction::none,
                  "unchanged N must not enqueue");
            // No prior table (initial sync) is normal priority, not urgent.
            check(policy_activation_action(0, 4, 3) == ActivationAction::reconcile,
                  "initial sync is normal priority");
            // Not a current target at all.
            check(policy_activation_action(2, 4, 5) == ActivationAction::none,
                  "out-of-range position is not a target");
        }
        std::cout << "PASS priority: activation rule (urgent only for new N-increase "
                     "suffix positions)\n";

        // ---- 5. Completion classification + events ------------------------
        {
            Cluster c;
            // Seed the region on remote holders so the pull is non-empty.
            c.stores[1].apply_pixel(192, 7, core::VersionedPixel{0xABCDEF, 50, 2});
            c.stores[2].apply_pixel(192, 7, core::VersionedPixel{0xABCDEF, 50, 3});
            c.nodes[0]->request_priority_backfill(192);
            check(c.nodes[0]->backfill_step(), "urgent pull processed");
            c.pump();
            c.pump();
            auto r = c.stores[0].get_region(192);
            check(r && r->pixel(7).color == 0xABCDEF,
                  "urgent pull must merge the populated copy");
            check(c.count_progress(192, "backfill_started") == 1,
                  "started event reported");
            check(c.count_progress(192, "backfill_completed") == 1,
                  "completed event reported");
            bool complete_seen = false;
            for (const auto& [rgn, e, cls, a] : c.progress)
                if (rgn == 192 && e == "backfill_completed" && cls == "complete")
                    complete_seen = true;
            check(complete_seen, "completion must classify as complete");
        }
        std::cout << "PASS priority: started/completed events with classification\n";

        // ---- 6. Stale-epoch callbacks cannot fabricate readiness ----------
        {
            Cluster c;
            c.stores[1].apply_pixel(22, 9, core::VersionedPixel{0x123456, 80, 2});
            c.stores[2].apply_pixel(22, 9, core::VersionedPixel{0x123456, 80, 3});
            c.nodes[0]->request_priority_backfill(22);
            check(c.nodes[0]->backfill_step(), "urgent pull started");
            check(!c.packets.empty(), "GETs sent");
            c.nodes[0]->reset_repair_state(); // stale-generation reset
            c.stores[0].clear();              // mirrors PeerNode::reset_to_joiner
            c.pump(); // late replies must be dropped
            check(!c.stores[0].get_region(22).has_value(),
                  "stale-epoch urgent result must not populate the reset store");
            c.nodes[0]->request_priority_backfill(22);
            check(c.nodes[0]->backfill_step(), "post-reset urgent pull proceeds");
            c.pump();
            c.pump();
            auto r = c.stores[0].get_region(22);
            check(r && r->pixel(9).color == 0x123456,
                  "post-reset urgent pull lands the populated copy");
        }
        std::cout << "PASS priority: stale epochs drop, post-reset urgent pull works\n";

        std::cout << "PASS backfill priority (urgent activation queue)\n";
    } catch (const std::exception& e) {
        std::cerr << "FAIL backfill priority: " << e.what() << '\n';
        return 1;
    }
    return 0;
}
