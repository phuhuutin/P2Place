// Strategy B unit test: deterministic three-tier policy mapping
// (min N = 2, max N = 4: 0.95/0.80 -> N2/N3/N4), assignment caps,
// per-region cooldown, placement-change waiver, snapshot wire codec, and
// stale-snapshot rejection.
#include "core/balanced_placement.hpp"
#include "core/peer_reliability.hpp"
#include "core/replication_policy.hpp"
#include <cassert>
#include <chrono>
#include <cmath>
#include <iostream>
using p2place::core::BalancedPlacement;
using p2place::core::PeerReliabilityTracker;
using p2place::core::PolicyEvaluator;
using p2place::core::PolicyTable;
using p2place::core::PolicyConfig;
using Clock=PeerReliabilityTracker::Clock;

namespace {
void check(bool ok,const char* why){if(!ok)throw std::runtime_error(why);}
constexpr auto W=std::chrono::seconds(120);

// Drive a peer's history so its window score equals x exactly: unhealthy
// for (1-x)*W seconds at the start of the window, healthy afterwards.
void set_score(PeerReliabilityTracker& t, p2place::core::NodeId id, double x,
               Clock::time_point now) {
    const auto back=now-W;
    t.observe(id,true,back);
    if(x<1.0) {
        t.observe(id,false,back);
        const auto healthy_from=back+std::chrono::milliseconds(
            static_cast<long long>(W.count()*1000*(1.0-x)));
        t.observe(id,true,healthy_from);
    }
}
std::uint8_t all_targets_equal(const p2place::core::PolicySnapshot& s) {
    for(auto n:s.target_n)if(n!=s.target_n[0])return 0;
    return s.target_n[0];
}
}

int main() {
 try {
  // ---- 1. Risk mapping: min score -> desired N (min N = 2, max N = 4) --
  struct Case {double x;std::uint8_t n;};
  for(const auto& c: {Case{0.98,2},Case{0.90,3},Case{0.65,4},
                      Case{0.95,2},Case{0.80,3},Case{0.799,4}}) {
   PeerReliabilityTracker t(W);
   PolicyTable table(2);
   PolicyEvaluator ev(PolicyConfig{2,4,0.95,0.80,std::chrono::seconds(30)});
   auto placement=BalancedPlacement::create({1,2,3,4,5});
   const auto now=Clock::now();
   for(auto id:placement.members())set_score(t,id,c.x,now);
   auto out=ev.evaluate(placement,t,table,now,false);
   check(out.changed,"first evaluation must produce a snapshot");
   check(all_targets_equal(out.snap)==c.n,"wrong desired N mapping");
   check(out.snap.policy_epoch==1,"first snapshot must have epoch 1");
  }
  std::cout<<"PASS policy-b: N2 policy mapping 0.98->2, 0.90->3, 0.65->4, "
             "boundaries 0.95/0.80 (three tiers N2/N3/N4)\n";

  // ---- 1b. Min N = 3 config collapses the mid tier: two-tier N3/N4 ------
  for(const auto& c: {Case{0.98,3},Case{0.90,3},Case{0.65,4},Case{0.80,3}}) {
   PeerReliabilityTracker t(W);
   PolicyTable table(3);
   PolicyEvaluator ev(PolicyConfig{3,4,0.95,0.80,std::chrono::seconds(30)});
   auto placement=BalancedPlacement::create({1,2,3,4,5});
   const auto now=Clock::now();
   for(auto id:placement.members())set_score(t,id,c.x,now);
   auto out=ev.evaluate(placement,t,table,now,false);
   check(all_targets_equal(out.snap)==c.n,
         "min N=3 config must keep the two-tier N3/N4 behavior");
  }
  std::cout<<"PASS policy-b: min N=3 config keeps two-tier N3/N4 "
             "(A/B comparison baseline preserved)\n";

  // ---- 2. Assignment capped by committed members -----------------------
  {
   PeerReliabilityTracker t(W);
   PolicyTable table(2);
   PolicyEvaluator ev(PolicyConfig{2,4,0.95,0.80,std::chrono::seconds(30)});
   auto placement=BalancedPlacement::create({1,2,3});
   const auto now=Clock::now();
   for(auto id:placement.members())set_score(t,id,0.60,now);
   auto out=ev.evaluate(placement,t,table,now,false);
   check(all_targets_equal(out.snap)==3,
         "desired 4 must be capped to the 3 committed members");
  }
  std::cout<<"PASS policy-b: assigned_n capped by committed membership\n";

  // ---- 3. Cooldown blocks oscillation; placement change waives it -------
  {
   PeerReliabilityTracker t(W);
   PolicyTable table(2);
   PolicyEvaluator ev(PolicyConfig{2,4,0.95,0.80,std::chrono::seconds(30)});
   auto placement=BalancedPlacement::create({1,2,3,4,5});
   const auto now=Clock::now();
   for(auto id:placement.members())set_score(t,id,1.0,now);
   auto first=ev.evaluate(placement,t,table,now,false);
   check(all_targets_equal(first.snap)==2,"initial snapshot must keep the N2 baseline");
   table.apply(first.snap,nullptr);
   // Node 1 churns: its window score collapses below 0.80 -> affected
   // regions want N4, but the 30s cooldown blocks the change.
   set_score(t,1,0.60,now+std::chrono::seconds(5));
   auto blocked=ev.evaluate(placement,t,table,now+std::chrono::seconds(5),false);
   check(!blocked.changed,"cooldown must block a change 5s after the last one");
   // The SAME inputs apply immediately when the placement changed: the
   // cooldown is waived.
   auto waived=ev.evaluate(placement,t,table,now+std::chrono::seconds(5),true);
   check(waived.changed,"placement change must waive the cooldown");
   std::size_t fours=0;
   for(auto n:waived.snap.target_n)if(n==4)++fours;
   check(fours>0 && fours<256,"waived re-evaluation must map affected regions to N4");
   table.apply(waived.snap,nullptr);
   // Cooldown expiry (no placement change): node 2 churns at +35s.
   // - [2,3,4] baselines (min score 0.60) move 2->4 (cooldown expired);
   // - [4,5,1] baselines (node 1 recovered to 0.85) move 4->3 (mid tier);
   // - [1,2,3]/[5,1,2] stay 4; [3,4,5] stays at the N2 baseline.
   set_score(t,2,0.60,now+std::chrono::seconds(35));
   auto expired=ev.evaluate(placement,t,table,now+std::chrono::seconds(35),false);
   check(expired.changed,"cooldown must expire after 30s");
   check(expired.snap.target_n[1]==4,"[2,3,4] regions must move to N4 after cooldown");
   check(expired.snap.target_n[2]==2,"[3,4,5] regions must stay at the N2 baseline");
   check(expired.snap.target_n[0]==4,"[1,2,3] regions must stay at N4");
   std::size_t threes=0;
   for(auto n:expired.snap.target_n)if(n==3)++threes;
   check(threes>0 && threes<256,"recovered [4,5,1] regions must move to the N3 mid tier");
  }
  std::cout<<"PASS policy-b: 30s per-region cooldown + placement-change waiver\n";

  // ---- 4. Wire codec roundtrip ------------------------------------------
  {
   PeerReliabilityTracker t(W);
   PolicyTable table(2);
   PolicyEvaluator ev(PolicyConfig{2,4,0.95,0.80,std::chrono::seconds(30)});
   auto placement=BalancedPlacement::create({1,2,3});
   const auto now=Clock::now();
   for(auto id:placement.members())set_score(t,id,0.90,now);
   auto out=ev.evaluate(placement,t,table,now,false);
   const auto bytes=out.snap.encode();
   auto back=p2place::core::PolicySnapshot::decode(bytes);
   check(back.placement_gen==out.snap.placement_gen &&
         back.placement_fp==out.snap.placement_fp &&
         back.policy_epoch==out.snap.policy_epoch &&
         back.strategy==out.snap.strategy &&
         back.target_n==out.snap.target_n,"snapshot roundtrip mismatch");
  }
  std::cout<<"PASS policy-b: snapshot wire codec roundtrip\n";

  // ---- 5. PolicyTable validation ----------------------------------------
  {
   PolicyTable table(2);
   p2place::core::PolicySnapshot s;
   s.placement_gen=5;s.placement_fp=12345;s.policy_epoch=3;s.strategy='B';
   s.target_n.fill(2);
   std::string reason;
   check(table.apply(s,&reason),"valid snapshot must apply");
   check(table.target_n(0)==2 && table.target_n(255)==2,"table must serve targets");
   s.policy_epoch=2;
   check(!table.apply(s,&reason) && reason=="stale policy epoch",
         "stale epoch must be rejected");
   s.policy_epoch=3;s.placement_fp=999;
   check(!table.apply(s,&reason),"conflicting fingerprint must be rejected");
   s.placement_fp=12345;
   check(table.apply(s,&reason),"idempotent re-broadcast must be accepted");
   s.policy_epoch=4;s.target_n[7]=4;
   check(table.apply(s,&reason) && table.target_n(7)==4,
         "newer epoch must replace the table");
  }
  std::cout<<"PASS policy-b: stale/conflicting/idempotent snapshot validation\n";

  std::cout<<"PASS policy-b (strategy B evaluator)\n";
 } catch(const std::exception& e) {
  std::cerr<<"FAIL policy-b: "<<e.what()<<'\n';return 1;
 }
}
