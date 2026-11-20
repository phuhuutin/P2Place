// Strategy C unit test: churn+write-aware max-composition mapping, threshold
// boundaries, C >= B invariant, strategy byte encoding, cooldown sharing.
#include "core/balanced_placement.hpp"
#include "core/peer_reliability.hpp"
#include "core/replication_policy.hpp"
#include <chrono>
#include <cmath>
#include <iostream>
#include <stdexcept>
using p2place::core::BalancedPlacement;
using p2place::core::PeerReliabilityTracker;
using p2place::core::PolicyEvaluator;
using p2place::core::PolicyTable;
using p2place::core::PolicyConfig;
using Clock = PeerReliabilityTracker::Clock;
namespace {
void check(bool ok, const char* why) {
    if (!ok) throw std::runtime_error(why);
}
constexpr auto W = std::chrono::seconds(120);

// Drive a peer's history so its window score equals x exactly.
void set_score(PeerReliabilityTracker& t, p2place::core::NodeId id, double x,
               Clock::time_point now) {
    const auto back = now - W;
    t.observe(id, true, back);
    if (x < 1.0) {
        t.observe(id, false, back);
        const auto healthy_from = back + std::chrono::milliseconds(
            static_cast<long long>(W.count() * 1000 * (1.0 - x)));
        t.observe(id, true, healthy_from);
    }
}
std::uint8_t all_targets_equal(const p2place::core::PolicySnapshot& s) {
    for (auto n : s.target_n)
        if (n != s.target_n[0]) return 0;
    return s.target_n[0];
}
PolicyConfig cfg_c() {
    PolicyConfig c;
    c.strategy = 'C';
    c.min_n = 2; c.max_n = 4;
    c.threshold_high = 0.95; c.threshold_low = 0.80;
    c.write_threshold_mid = 0.20; c.write_threshold_high = 1.00;
    c.cooldown = std::chrono::seconds(30);
    return c;
}
std::uint8_t evaluate_c(PolicyEvaluator& ev, PolicyTable& table, double score,
                        double rate, Clock::time_point now,
                        bool placement_changed = false) {
    // Fresh tracker per call: set_score anchors a history to `now`, and
    // reusing one tracker across calls would accumulate extra unhealthy
    // segments and drift the score below the intended value.
    PeerReliabilityTracker t(W);
    auto placement = BalancedPlacement::create({1, 2, 3, 4, 5});
    for (auto id : placement.members()) set_score(t, id, score, now);
    std::array<double, BalancedPlacement::PARTITIONS> rates{};
    rates.fill(rate);
    ev.set_write_rates(&rates);
    auto out = ev.evaluate(placement, t, table, now, placement_changed);
    return all_targets_equal(out.snap);
}
}

int main() {
    try {
        // ---- 1. C max-composition mapping (handoff cases) ----------------
        struct Case { double a, f; std::uint8_t n; };
        for (const auto& c : {Case{0.98, 0.05, 2}, Case{0.98, 0.40, 3},
                              Case{0.98, 1.50, 4}, Case{0.90, 0.05, 3},
                              Case{0.65, 0.05, 4}}) {
            PolicyTable table(2);
            PolicyEvaluator ev(cfg_c());
            const auto n = evaluate_c(ev, table, c.a, c.f, Clock::now());
            check(n == c.n, "C mapping case mismatch");
        }
        std::cout << "PASS strategy C: mapping (A,F) -> (2,2)/(3,3)/(4,4) "
                     "max-composition\n";

        // ---- 2. Threshold boundaries at exactly 0.20 and 1.00 ------------
        {
            PolicyTable table(2);
            PolicyEvaluator ev(cfg_c());
            check(evaluate_c(ev, table, 0.98, 0.20, Clock::now()) == 3,
                  "rate exactly 0.20 must reach the mid tier (>=)");
            check(evaluate_c(ev, table, 0.98, 1.00, Clock::now()) == 4,
                  "rate exactly 1.00 must reach N4 (>=)");
            check(evaluate_c(ev, table, 0.98, 0.199, Clock::now()) == 2,
                  "rate just below 0.20 must stay N2");
        }
        std::cout << "PASS strategy C: threshold boundaries 0.20/1.00\n";

        // ---- 3. C >= B for the same reliability input --------------------
        {
            PolicyTable table(2);
            PolicyEvaluator ev(cfg_c());
            // Healthy peers, cold region: C == B == N2.
            check(evaluate_c(ev, table, 0.98, 0.05, Clock::now()) == 2,
                  "cold region must be N2");
            // Healthy peers, hot region: B would stay N2; C raises to N4.
            check(evaluate_c(ev, table, 0.98, 1.50, Clock::now()) == 4,
                  "hot region must be N4 while churn term stays N2");
        }
        std::cout << "PASS strategy C: N_C >= N_B for identical reliability\n";

        // ---- 4. Snapshot carries the 'C' strategy byte -------------------
        {
            PolicyTable table(2);
            PolicyEvaluator ev(cfg_c());
            auto placement = BalancedPlacement::create({1, 2, 3, 4, 5});
            const auto now = Clock::now();
            PeerReliabilityTracker t(W);
            for (auto id : placement.members()) set_score(t, id, 0.98, now);
            std::array<double, BalancedPlacement::PARTITIONS> rates{};
            rates.fill(0.4);
            ev.set_write_rates(&rates);
            auto out = ev.evaluate(placement, t, table, now, false);
            check(out.snap.strategy == static_cast<std::uint8_t>('C'),
                  "snapshot strategy byte must be 'C'");
            const auto bytes = out.snap.encode();
            auto back = p2place::core::PolicySnapshot::decode(bytes);
            check(back.strategy == static_cast<std::uint8_t>('C') &&
                      back.target_n == out.snap.target_n &&
                      back.policy_epoch == out.snap.policy_epoch,
                  "C snapshot codec roundtrip");
            // Diagnostic arrays are populated for C.
            bool any = false;
            for (auto n : out.n_write)
                if (n == 3) any = true;
            check(any, "n_write diagnostics must reflect the 0.40 rate (N3)");
        }
        std::cout << "PASS strategy C: snapshot strategy byte + diagnostics\n";

        // ---- 5. Shared per-region cooldown; placement change waives -------
        {
            PeerReliabilityTracker t(W);
            PolicyTable table(2);
            PolicyEvaluator ev(cfg_c());
            auto placement = BalancedPlacement::create({1, 2, 3, 4, 5});
            const auto now = Clock::now();
            for (auto id : placement.members()) set_score(t, id, 0.98, now);
            std::array<double, BalancedPlacement::PARTITIONS> rates{};
            rates.fill(1.5); // hot: wants N4
            ev.set_write_rates(&rates);
            auto first = ev.evaluate(placement, t, table, now, false);
            check(all_targets_equal(first.snap) == 4, "hot initial snapshot must be N4");
            table.apply(first.snap, nullptr);
            // Rate collapses below mid -> desired N2, blocked by cooldown.
            rates.fill(0.05);
            auto blocked = ev.evaluate(placement, t, table, now + std::chrono::seconds(5),
                                       false);
            check(!blocked.changed, "cooldown must block the N4->N2 shrink");
            // Placement change waives the cooldown.
            auto waived = ev.evaluate(placement, t, table, now + std::chrono::seconds(5),
                                      true);
            check(waived.changed && all_targets_equal(waived.snap) == 2,
                  "placement change must waive the cooldown");
        }
        std::cout << "PASS strategy C: 30s per-region cooldown shared with B\n";

        // ---- 6. Config validation ----------------------------------------
        {
            bool rejected = false;
            try {
                PolicyConfig bad = cfg_c();
                bad.strategy = 'D';
                PolicyEvaluator ev(bad);
            } catch (const std::invalid_argument&) { rejected = true; }
            check(rejected, "invalid strategy byte must be rejected");
            rejected = false;
            try {
                PolicyConfig bad = cfg_c();
                bad.write_threshold_mid = 1.5; // mid >= high
                PolicyEvaluator ev(bad);
            } catch (const std::invalid_argument&) { rejected = true; }
            check(rejected, "invalid write thresholds must be rejected");
        }
        std::cout << "PASS strategy C: config validation\n";

        std::cout << "PASS strategy C (churn + write-aware evaluator)\n";
    } catch (const std::exception& e) {
        std::cerr << "FAIL strategy C: " << e.what() << '\n';
        return 1;
    }
    return 0;
}
