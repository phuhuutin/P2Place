#pragma once
#include "core/balanced_placement.hpp"
#include "core/peer_reliability.hpp"
#include <array>
#include <chrono>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace p2place::core {
// One versioned, immutable, authority-computed replication-policy snapshot.
// placement_gen/fingerprint bind the table to exactly one committed
// placement; policy_epoch increments whenever the table changes. The target
// table is the ONLY policy input peers use for per-region N — every peer
// applies the same snapshot, so no peer computes N from local observations.
struct PolicySnapshot {
    std::uint64_t placement_gen = 0;
    std::uint64_t placement_fp = 0;
    std::uint64_t policy_epoch = 0;
    std::uint8_t strategy = 0;                       // 'A' or 'B'
    std::array<std::uint8_t, BalancedPlacement::PARTITIONS> target_n{};

    [[nodiscard]] std::vector<std::uint8_t> encode() const;
    static PolicySnapshot decode(std::span<const std::uint8_t> b);
};

// Accepted policy table on every peer; atomically replaced on a validated
// snapshot. Unset regions fall back to the fixed baseline (3).
class PolicyTable final {
public:
    explicit PolicyTable(std::uint8_t fallback_n = 3);
    // Accept if the snapshot binds to the current committed placement and
    // is not stale. reason is filled on rejection.
    bool apply(const PolicySnapshot& snap, std::string* reason);
    [[nodiscard]] std::uint8_t target_n(std::uint32_t region) const;
    [[nodiscard]] const PolicySnapshot& current() const noexcept { return current_; }
    [[nodiscard]] bool initialized() const noexcept { return initialized_; }
private:
    std::uint8_t fallback_n_;
    PolicySnapshot current_{};
    bool initialized_ = false;
};

// Authority-side Strategy B evaluator (deterministic).
//
//   baseline(r)   = first min(3, members) preferred members of region r
//   risk(r)       = 1 - min(A_i) over baseline(r)   (worst-replica rule)
//   desired_n     = min_n (3)  when min(A_i) >= threshold_low
//                   max_n (4)  when min(A_i) <  threshold_low
//   assigned_n    = min(desired_n, distinct committed members)
//
// Min N = 3: the adaptation may RAISE replicas under churn but never
// shrinks a region below the fixed baseline (the N2 tier is disabled;
// threshold_high is retained in the config for compatibility but unused).
// The first evaluation of a cluster therefore starts every region at the
// N3 baseline (capped by the live member count), and a 30-second per-region
// cooldown gates changes; a placement change resets all cooldowns and
// recomputes immediately. R and W stay fixed at 2.
struct PolicyConfig {
    std::size_t min_n = 2, max_n = 4;
    double threshold_high = 0.95;   // >= high -> desired 2
    double threshold_low = 0.80;    // <  low  -> desired 4
    std::chrono::seconds cooldown{30};
};

struct PolicyEvaluation {
    PolicySnapshot snap;
    std::array<double, BalancedPlacement::PARTITIONS> risk{}; // 1 - min score
    bool changed = false;
};

class PolicyEvaluator final {
public:
    using Clock = std::chrono::steady_clock;
    explicit PolicyEvaluator(PolicyConfig cfg);
    // Recompute the target table. placement_changed resets cooldowns and
    // always yields a fresh epoch on change. unchanged snapshots keep the
    // previous table (authority may re-broadcast them as-is).
    PolicyEvaluation evaluate(const BalancedPlacement& placement,
                              const PeerReliabilityTracker& reliability,
                              const PolicyTable& table,
                              Clock::time_point now,
                              bool placement_changed);
private:
    PolicyConfig cfg_;
    std::array<Clock::time_point, BalancedPlacement::PARTITIONS> last_change_{};
};
}
