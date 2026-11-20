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

struct PolicyConfig {
    std::size_t min_n = 2, max_n = 4;
    double threshold_high = 0.95;   // >= high -> desired 2
    double threshold_low = 0.80;    // <  low  -> desired 4
    std::chrono::seconds cooldown{30};
    // Strategy C (churn + write-aware) composition inputs.
    char strategy = 'B';                    // 'B' or 'C' (snapshot byte)
    double write_threshold_mid = 0.20;      // rate >= mid  -> mid tier
    double write_threshold_high = 1.00;     // rate >= high -> max N
};

// What a member must do for one region after a real policy change, given the
// region's previous target N (`old_n`; 0 when no prior accepted table), the
// new target N, and this member's index in the region's preference order.
//   none      - no action (unchanged target set, or not a current target)
//   urgent    - newly selected by an N INCREASE (position in the new suffix):
//               activate BEFORE the normal repair backlog; the hot policy
//               window may decay while a low-priority pull waits.
//   reconcile - the target set changed while this member stayed a target
//               (shrink/partial-copy reconciliation); normal priority.
enum class ActivationAction { none, urgent, reconcile };

constexpr ActivationAction policy_activation_action(std::uint8_t old_n,
                                                    std::uint8_t new_n,
                                                    int position) {
    if (new_n == 0 || position < 0 || position >= new_n)
        return ActivationAction::none;
    if (old_n == 0) return ActivationAction::reconcile;  // initial sync
    if (new_n > old_n && position >= old_n) return ActivationAction::urgent;
    if (new_n != old_n) return ActivationAction::reconcile;
    return ActivationAction::none;
}

struct PolicyEvaluation {
    PolicySnapshot snap;
    std::array<double, BalancedPlacement::PARTITIONS> risk{}; // 1 - min score
    // Strategy C diagnostics (populated for every region when strategy C):
    std::array<double, BalancedPlacement::PARTITIONS> write_rate{};
    std::array<std::uint8_t, BalancedPlacement::PARTITIONS> n_churn{};
    std::array<std::uint8_t, BalancedPlacement::PARTITIONS> n_write{};
    bool changed = false;
};

class PolicyEvaluator final {
public:
    using Clock = std::chrono::steady_clock;
    explicit PolicyEvaluator(PolicyConfig cfg);
    // Strategy C input: cluster-wide per-region attempted-write rate
    // (writes/sec over the write window). Null -> pure churn (B) behavior.
    void set_write_rates(
        const std::array<double, BalancedPlacement::PARTITIONS>* rates) {
        write_rates_ = rates;
    }
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
    const std::array<double, BalancedPlacement::PARTITIONS>* write_rates_ =
        nullptr;
};
}
