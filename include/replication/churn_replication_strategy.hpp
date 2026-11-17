#pragma once
#include "core/replication_policy.hpp"
#include "replication/replication_strategy.hpp"
#include <cstdint>
namespace p2place::replication {
// Strategy B: per-region desired N comes exclusively from the SHARED,
// authority-computed PolicyTable. This strategy owns no policy decisions:
// every peer reads the same table, so N can never diverge. Regions without
// an accepted snapshot fall back to the fixed baseline (3).
class ChurnReplicationStrategy final : public ReplicationStrategy {
public:
    ChurnReplicationStrategy(const core::PolicyTable& table,
                             std::size_t r, std::size_t w,
                             std::size_t max_n)
        : table_(table), r_(r), w_(w), max_n_(max_n) {
        if(!r_ || !w_ || !max_n_)throw std::invalid_argument("R/W/max N must be positive");
    }
    QuorumConfig plan(std::uint32_t region) const override {
        return {table_.target_n(region), r_, w_};
    }
    std::size_t max_targets() const override {return max_n_;}
private:
    const core::PolicyTable& table_;
    std::size_t r_, w_;
    std::size_t max_n_;
};
}
