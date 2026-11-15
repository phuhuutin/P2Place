#include "replication/churn_replication_strategy.hpp"
#include <algorithm>
#include <cmath>
#include <stdexcept>
namespace p2place::replication {
ChurnReplicationStrategy::ChurnReplicationStrategy(Config c):config_(c) {
 if(!c.min_n || c.min_n>c.base_n || c.base_n>c.max_n || c.max_n>256 ||
    !c.R || !c.W || c.R>c.min_n || c.W>c.min_n ||
    c.decrease_risk<0 || c.increase_risk>1 || c.decrease_risk>=c.increase_risk ||
    c.cooldown.count()<0)throw std::invalid_argument("invalid churn strategy configuration");
 for(auto& e:regions_)e.n=c.base_n;
}
void ChurnReplicationStrategy::update(std::uint32_t region,double risk,Clock::time_point now) {
 if(region>=regions_.size() || !std::isfinite(risk) || risk<0 || risk>1)
    throw std::invalid_argument("invalid region or risk");
 auto& e=regions_[region];
 if(e.initialized && now-e.changed<config_.cooldown)return;
 std::size_t next=e.n;
 if(risk>=config_.increase_risk)next=std::min(e.n+1,config_.max_n);
 else if(risk<=config_.decrease_risk)next=std::max(e.n-1,config_.min_n);
 if(next!=e.n){e.n=next;e.changed=now;e.initialized=true;}
}
QuorumConfig ChurnReplicationStrategy::plan(std::uint32_t region)const {
 if(region>=regions_.size())throw std::out_of_range("region out of bounds");
 return {regions_[region].n,config_.R,config_.W};
}
}
