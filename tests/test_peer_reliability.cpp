// Strategy B unit test: rolling-window peer reliability tracker.
#include "core/peer_reliability.hpp"
#include <cassert>
#include <chrono>
#include <cmath>
#include <iostream>
using p2place::core::PeerReliabilityTracker;
using Clock=PeerReliabilityTracker::Clock;

namespace {
void check(bool ok,const char* why){if(!ok)throw std::runtime_error(why);}
constexpr auto W=std::chrono::seconds(120);
}

int main() {
 try {
  // 1. Unknown node: neutral score, not sampled.
  {
   PeerReliabilityTracker t(W);
   auto s=t.get(4,Clock::now());
   check(s.score==1.0 && !s.sampled && s.observed_seconds==0.0,
         "unknown node must be neutral and unsampled");
  }
  // 2. All-healthy window -> 1.0, sampled.
  {
   PeerReliabilityTracker t(W);
   const auto t0=Clock::now()-std::chrono::seconds(60);
   t.observe(1,true,t0);
   auto s=t.get(1,t0+std::chrono::seconds(60));
   check(s.sampled && std::fabs(s.score-1.0)<1e-9 && s.observed_seconds>59.9,
         "all-healthy window must score 1.0");
  }
  // 3. 70 healthy + 50 unhealthy seconds -> 70/120.
  {
   PeerReliabilityTracker t(W);
   const auto t0=Clock::now()-std::chrono::seconds(120);
   t.observe(4,true,t0);
   t.observe(4,false,t0+std::chrono::seconds(70));
   auto s=t.get(4,t0+std::chrono::seconds(120));
   check(s.sampled && std::fabs(s.score-70.0/120.0)<1e-6,
         "70/120 split must score 70/120");
  }
  // 4. Window expiry removes old intervals.
  {
   PeerReliabilityTracker t(W);
   const auto t0=Clock::now()-std::chrono::seconds(200);
   t.observe(2,false,t0);                                   // 80s unhealthy
   t.observe(2,true,t0+std::chrono::seconds(80));           // 120s healthy
   auto s=t.get(2,t0+std::chrono::seconds(200));
   check(s.sampled && std::fabs(s.score-1.0)<1e-9,
         "expired unhealthy history must leave a healthy window");
  }
  // 5. Repeated same-state events do not double-count.
  {
   PeerReliabilityTracker t(W);
   const auto t0=Clock::now()-std::chrono::seconds(120);
   t.observe(3,true,t0);
   t.observe(3,true,t0+std::chrono::seconds(10)); // duplicate, ignored
   t.observe(3,true,t0+std::chrono::seconds(20)); // duplicate, ignored
   auto s=t.get(3,t0+std::chrono::seconds(120));
   check(s.sampled && std::fabs(s.score-1.0)<1e-9,
         "duplicate observations must not double-count");
  }
  // 6. Removal counts unhealthy; rejoin preserves history.
  {
   PeerReliabilityTracker t(W);
   const auto t0=Clock::now()-std::chrono::seconds(120);
   t.observe(4,true,t0);
   t.observe(4,false,t0+std::chrono::seconds(100)); // crash
   t.mark_removed(4,t0+std::chrono::seconds(105));  // committed removal
   t.observe(4,true,t0+std::chrono::seconds(115));  // rejoin (same id)
   auto s=t.get(4,t0+std::chrono::seconds(120));
   check(s.sampled && s.score<1.0 && std::fabs(s.score-105.0/120.0)<1e-6,
         "removal+rejoin must preserve unhealthy history");
  }
  // 7. Late joiner is only exposed for its observed time.
  {
   PeerReliabilityTracker t(W);
   const auto t0=Clock::now()-std::chrono::seconds(120);
   t.observe(5,true,t0+std::chrono::seconds(90)); // joined 30s ago
   auto s=t.get(5,t0+std::chrono::seconds(120));
   check(s.sampled && std::fabs(s.score-1.0)<1e-9 &&
         std::fabs(s.observed_seconds-30.0)<1.0,
         "late joiners must not be penalized for pre-observation time");
  }
  std::cout<<"PASS peer-reliability: window scoring, expiry, dedup, removal history\n";
 } catch(const std::exception& e) {
  std::cerr<<"FAIL peer-reliability: "<<e.what()<<'\n';return 1;
 }
}
