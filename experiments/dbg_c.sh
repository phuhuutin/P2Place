#!/bin/bash
cd ~/projects/p2place
cat > /tmp/dbg_c.cpp <<'EOF'
#include "core/balanced_placement.hpp"
#include "core/peer_reliability.hpp"
#include "core/replication_policy.hpp"
#include <chrono>
#include <iostream>
using namespace p2place::core;
using Clock = PeerReliabilityTracker::Clock;
constexpr auto W = std::chrono::seconds(120);
void set_score(PeerReliabilityTracker& t, p2place::core::NodeId id, double x, Clock::time_point now) {
    const auto back = now - W;
    t.observe(id, true, back);
    if (x < 1.0) {
        t.observe(id, false, back);
        const auto healthy_from = back + std::chrono::milliseconds(
            static_cast<long long>(W.count() * 1000 * (1.0 - x)));
        t.observe(id, true, healthy_from);
    }
}
int main(){
  PolicyConfig cfg; cfg.strategy='C'; cfg.min_n=2; cfg.max_n=4;
  cfg.threshold_high=0.95; cfg.threshold_low=0.80;
  cfg.write_threshold_mid=0.20; cfg.write_threshold_high=1.00;
  cfg.cooldown=std::chrono::seconds(30);
  PolicyEvaluator ev(cfg);
  PeerReliabilityTracker t(W);
  PolicyTable table(2);
  auto placement=BalancedPlacement::create({1,2,3,4,5});
  auto now=Clock::now();
  for(auto id:placement.members()) set_score(t,id,0.98,now);
  std::array<double,BalancedPlacement::PARTITIONS> rates{};
  rates.fill(0.20);  ev.set_write_rates(&rates);
  auto o1=ev.evaluate(placement,t,table,now,false);
  std::cout<<"call1 t0="<<(int)o1.snap.target_n[0]<<" chg="<<o1.changed<<"\n";
  rates.fill(1.00);
  auto o2=ev.evaluate(placement,t,table,Clock::now(),false);
  std::cout<<"call2 t0="<<(int)o2.snap.target_n[0]<<" chg="<<o2.changed<<"\n";
  rates.fill(0.199);
  auto o3=ev.evaluate(placement,t,table,Clock::now(),false);
  std::cout<<"call3 t0="<<(int)o3.snap.target_n[0]<<" chg="<<o3.changed
           <<" n_churn="<<(int)o3.n_churn[0]<<" n_write="<<(int)o3.n_write[0]<<"\n";
}
EOF
g++ -std=c++20 -Iinclude /tmp/dbg_c.cpp \
  build/CMakeFiles/p2place.dir/src/core/balanced_placement.cpp.o \
  build/CMakeFiles/p2place.dir/src/core/peer_reliability.cpp.o \
  build/CMakeFiles/p2place.dir/src/core/replication_policy.cpp.o \
  -o /tmp/dbg_c && /tmp/dbg_c
