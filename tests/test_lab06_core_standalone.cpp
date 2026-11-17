#include "core/failure_detector.hpp"
#include "core/replication_policy.hpp"
#include "replication/churn_replication_strategy.hpp"
#include <cassert>
#include <chrono>
#include <iostream>
#include <string>
using namespace std::chrono_literals;
int main() {
 using p2place::core::FailureDetector;
 using p2place::core::PolicyTable;
 using p2place::core::PolicySnapshot;
 using p2place::replication::ChurnReplicationStrategy;
 using Clock=FailureDetector::Clock;
 auto start=Clock::now();
 FailureDetector d({3000ms,15000ms,2000ms});
 d.observe(2,start);
 assert(d.state(2,start+1000ms)==FailureDetector::State::Healthy);
 assert(d.state(2,start+4000ms)==FailureDetector::State::Suspect);
 assert(d.state(2,start+16000ms)==FailureDetector::State::Unavailable);
 d.observe(2,start+17000ms);
 assert(d.state(2,start+17000ms)==FailureDetector::State::Healthy);
 d.disconnected(2,start+18000ms);
 assert(d.state(2,start+19000ms)==FailureDetector::State::Suspect);
 d.observe(2,start+20000ms);
 d.observe(2,start+22500ms);
 assert(d.state(2,start+22500ms)==FailureDetector::State::Healthy);
 // Strategy B is a VIEW over the shared authority policy table: per-region
 // N never diverges across peers, and regions without a snapshot fall back
 // to the fixed baseline.
 PolicyTable table(3);
 PolicySnapshot s;
 s.placement_gen=1;s.placement_fp=0xABCD;s.policy_epoch=1;s.strategy='B';
 s.target_n.fill(2);s.target_n[163]=4;
 std::string reason;
 assert(table.apply(s,&reason));
 ChurnReplicationStrategy b(table,2,2,4);
 assert(b.plan(163).N==4);
 assert(b.plan(0).N==2 && b.plan(255).N==2);
 PolicyTable empty(3);
 ChurnReplicationStrategy fallback(empty,2,2,4);
 assert(fallback.plan(7).N==3);
 bool rejected=false;
 try{ChurnReplicationStrategy bad(empty,3,2,4);}catch(const std::invalid_argument&){rejected=true;}
 assert(!rejected); // R/W<=fallback baseline is valid at construction
 std::cout<<"PASS lab06 core failure detection / shared policy strategy view\n";
}
