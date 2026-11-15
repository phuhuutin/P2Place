#include "core/failure_detector.hpp"
#include "replication/churn_replication_strategy.hpp"
#include <cassert>
#include <chrono>
#include <iostream>
using namespace std::chrono_literals;
int main() {
 using p2place::core::FailureDetector;
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
 ChurnReplicationStrategy p({3,2,5,2,2,0.60,0.25,30s});
 assert(p.plan(163).N==3);
 p.update(163,0.90,start);assert(p.plan(163).N==4);
 p.update(163,0.95,start+10s);assert(p.plan(163).N==4);
 p.update(163,0.95,start+31s);assert(p.plan(163).N==5);
 p.update(163,0.10,start+62s);assert(p.plan(163).N==4);
 assert(p.plan(164).N==3);
 bool rejected=false;try {p.update(163,1.5,start);}catch(const std::invalid_argument&) {rejected=true;}
 assert(rejected);
 std::cout<<"PASS lab06 core failure detection / hysteresis / adaptive bounds\n";
}
