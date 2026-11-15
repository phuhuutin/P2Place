#pragma once
#include "replication/replication_strategy.hpp"
#include <array>
#include <chrono>
#include <cstdint>
namespace p2place::replication {
// Strategy B policy only. Feed committed/shared reliability scores to all
// replicas before enabling: divergent inputs imply divergent N assignments.
class ChurnReplicationStrategy final : public ReplicationStrategy {
public:
 struct Config {
   std::size_t base_n=3,min_n=2,max_n=5,R=2,W=2;
   double increase_risk=0.60,decrease_risk=0.25;
   std::chrono::seconds cooldown{30};
 };
 using Clock=std::chrono::steady_clock;
 explicit ChurnReplicationStrategy(Config config);
 // risk: fraction in [0,1] derived from peer churn; call from authoritative
 // shared metrics, NOT each node's independent local gossip.
 void update(std::uint32_t region,double risk,Clock::time_point now);
 [[nodiscard]] QuorumConfig plan(std::uint32_t region) const override;
 private:
  struct Entry {std::size_t n;Clock::time_point changed{};bool initialized=false;};
  Config config_;
  std::array<Entry,256> regions_{};
};
}
