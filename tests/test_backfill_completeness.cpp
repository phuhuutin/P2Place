/**
 * @file test_backfill_completeness.cpp
 * @brief Regression test for the backfill false-complete defect.
 *
 * Confirmed bug: a backfill read quorum completed on the first R responses
 * (including the backfilling node's own synchronous EMPTY reply), merged only
 * those, discarded the populated reply that arrived moments later as
 * "late-or-unknown", and logged success=true version=0 — an empty merge that
 * was never retried (archive evidence: node8_boot7.log requests 1073741828/29,
 * region 23/24, targets=8,9,10; acks from 8+9, sender=10 discarded).
 *
 * Fix under test: backfill uses get(key, cb, complete_all=true,
 * exclude_self=true) so every reachable source is merged; empty merges retry
 * a bounded number of times before being accepted as empty_valid; a stale
 * store epoch drops in-flight backfill callbacks.
 */
#include "replication/replication_manager.hpp"
#include "core/balanced_placement.hpp"
#include <array>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <stdexcept>
#include <utility>
using namespace p2place;
namespace {
void check(bool x,const char* why){if(!x)throw std::runtime_error(why);}
// Strategy with a switchable plan().N and a FIXED max-targets superset of 4:
// models Strategy B's N2..N4 policy where acknowledged copies may live on
// any of the four preference replicas even after a shrink. n_ is a reference
// so the manager-held strategy references stay valid across set_plan_n().
struct SwitchStrategy final:public replication::ReplicationStrategy {
    std::size_t& n_;
    explicit SwitchStrategy(std::size_t& n):n_(n){}
    replication::QuorumConfig plan(std::uint32_t)const override{return {n_,2,2};}
    std::size_t max_targets()const override{return 4;}
};
struct Cluster {
 struct Packet {core::NodeId src,dst;messages::Message msg;};
 core::ConsistentHashRing ring;
 std::array<core::RegionStore,4> stores;
 std::array<std::unique_ptr<replication::ReplicationStrategy>,4> strategies;
 std::array<std::unique_ptr<replication::ReplicationManager>,4> nodes;
 std::deque<Packet> packets;
 std::array<bool,4> alive={true,true,true,true};
 std::array<std::map<net::TimerQueue::Token,std::function<void()>>,4> timers;
 std::array<net::TimerQueue::Token,4> next={1,1,1,1};
 std::size_t plan_n=4;
 Cluster(){
  ring.rebuild(core::BalancedPlacement::create({1,2,3,4}));
  for(int i=0;i<4;++i){
   strategies[i]=std::make_unique<SwitchStrategy>(plan_n);
   nodes[i]=std::make_unique<replication::ReplicationManager>(i+1,ring,stores[i],
    *strategies[i],
    [this,i](core::NodeId dst,const messages::Message& msg){
     if(!alive[i] || !alive[dst-1])return false;
     packets.push_back({static_cast<core::NodeId>(i+1),dst,msg});return true;
    },
    [this,i](std::function<void()> cb){auto t=next[i]++;timers[i][t]=std::move(cb);return t;},
    [this,i](auto id){timers[i].erase(id);}
   );
  }
 }
 void set_plan_n(std::size_t n){plan_n=n;}
 void pump(){
  int steps=0;
  while(!packets.empty()){
   check(++steps<100000,"message loop");
   auto p=std::move(packets.front());packets.pop_front();
   if(alive[p.dst-1])nodes[p.dst-1]->on_message(p.src,p.msg);
  }
 }
};
void seed_pixel(Cluster& c,std::uint32_t key,std::uint32_t color,std::uint64_t lamport){
 c.stores[3].apply_pixel(key,7,core::VersionedPixel{color,lamport,4});
}
std::uint32_t find_key_with_pair(Cluster& c,core::NodeId a,core::NodeId b){
 for(std::uint32_t k=0;k<256;++k){
  const auto v=c.ring.successors(k,2);
  std::vector<core::NodeId> s(v.begin(),v.end());
  if(s.size()==2 && s[0]==a && s[1]==b)return k;
 }
 throw std::runtime_error("no region key with the required successor pair");
}
}
int main(){
 try {
  const std::uint32_t K=23,K2=6,K3=22;
  Cluster c;
  // Node 4 holds data for K and K3; nodes 1-3 (incl. the backfiller) are empty.
  seed_pixel(c,K,0x112233,50);
  seed_pixel(c,K3,0x445566,80);

  // --- Test 1: the OLD read quorum reproduces the false complete. ---
  // Dispatch order is the numeric set order {1,2,3,4}: self (empty) replies
  // synchronously, node 2 (empty) is the first remote reply -> R=2 satisfied
  // with zero content; node 4's populated reply is discarded as late.
  {
   bool ok=false;core::Region merged;
   c.nodes[0]->get(K,[&](bool o,const core::Region& r,const auto&){ok=o;merged=r;});
   c.pump();
   check(ok,"old-style quorum GET must succeed");
   check(merged.version()==0,"old-style first-R merge must be empty (bug signature)");
   check(!c.stores[0].get_region(K),"plain GET must not merge into the local store");
  }

  // --- Test 2: complete_all + exclude_self merges every reachable source. ---
  {
   bool ok=false;core::Region merged;
   c.nodes[0]->get(K,[&](bool o,const core::Region& r,const auto&){ok=o;merged=r;},
                  /*complete_all=*/true,/*exclude_self=*/true);
   c.pump();
   check(ok,"complete_all GET must succeed");
   check(merged.version()==50,"complete_all merge must include node 4's data");
   check(merged.pixel(7).color==0x112233,"complete_all merge must carry the pixel");
  }

  // --- Test 3: backfill_step() applies the complete merge and resolves. ---
  {
   c.nodes[0]->request_backfill(K);
   check(c.nodes[0]->backfill_step(),"backfill step must process the region");
   c.pump();
   auto r=c.stores[0].get_region(K);
   check(r && r->version()==50 && r->pixel(7).color==0x112233,
         "backfill must land the populated copy locally");
   // Re-requesting a resolved region must be a no-op (no new messages).
   c.nodes[0]->request_backfill(K);
   check(c.nodes[0]->backfill_step(),"resolved backfill must be consumed");
   check(c.packets.empty(),"resolved backfill must not re-issue GETs");
  }

  // --- Test 4a: every source reports empty -> empty_valid on the FIRST try. ---
  {
   c.nodes[0]->request_backfill(K2);
   check(c.nodes[0]->backfill_step(),"empty-region backfill must process");
   c.pump();c.pump(); // GETs out, replies back; all sources agree empty
   check(!c.nodes[0]->backfill_step(),"empty_valid acceptance must not re-queue");
   c.nodes[0]->request_backfill(K2);
   check(c.nodes[0]->backfill_step(),"empty_valid region must be guarded");
   check(c.packets.empty(),"empty_valid region must not retry");
   check(c.stores[0].get_region(K2).has_value(),"empty_valid region must exist locally");
  }

  // --- Test 4b: a failed source makes the merge partial -> BOUNDED retries. ---
  {
   c.alive[3]=false; // node 4 (the only holder of K3 data) is unreachable
   c.nodes[0]->request_backfill(K3);
   for(int attempt=1;attempt<=4;++attempt){
    check(c.nodes[0]->backfill_step(),"partial backfill attempt must process");
    c.pump();c.pump();
   }
   check(!c.nodes[0]->backfill_step(),"partial acceptance must not re-queue");
   c.nodes[0]->request_backfill(K3);
   check(c.nodes[0]->backfill_step(),"partial-accepted region must be guarded");
   check(c.packets.empty(),"partial-accepted region must not retry forever");
   c.alive[3]=true;
  }

  // --- Test 5: a stale store epoch drops in-flight backfill callbacks. ---
  {
   c.nodes[0]->reset_repair_state(); // clears resolved/attempt state from test 4b
   c.nodes[0]->request_backfill(K3);
   check(c.nodes[0]->backfill_step(),"backfill must start");
   check(!c.packets.empty(),"backfill must have sent GETs");
   c.nodes[0]->reset_repair_state(); // stale-generation reset clears the store
   c.stores[0].clear();              // (mirrors PeerNode::reset_to_joiner)
   c.pump(); // late replies must be dropped, not applied
   check(!c.stores[0].get_region(K3).has_value(),
         "stale-epoch backfill result must not populate the reset store");
   c.nodes[0]->request_backfill(K3);
   check(c.nodes[0]->backfill_step(),"post-reset backfill must proceed");
   c.pump();c.pump();
   auto r=c.stores[0].get_region(K3);
   check(r && r->version()==80 && r->pixel(7).color==0x445566,
         "post-reset backfill must land the populated copy");
  }

  // --- Test 6: Strategy B N=2 quorum semantics ---------------------------
  {
   Cluster c;
   const std::uint32_t K=find_key_with_pair(c,1,2);
   c.set_plan_n(2);
   // 6a. N=2 healthy: W=2/R=2 succeed and the value lands on both targets.
   bool ok=false;int calls=0;
   c.nodes[0]->put(K,9,{0xAB1234,50,1},[&](bool o,const auto&){ok=o;++calls;});
   c.pump();
   check(ok&&calls==1,"N=2 healthy write must ACK");
   for(auto id:c.ring.successors(K,2)) {
    auto r=c.stores[id-1].get_region(K);
    check(r && r->pixel(9).color==0xAB1234,"N=2 targets must hold the pixel");
   }
   bool read=false;core::Region merged;
   c.nodes[2]->get(K,[&](bool o,const auto& r,const auto&){read=o;merged=r;});
   c.pump();
   check(read&&merged.pixel(9).color==0xAB1234,"N=2 read must see the pixel");
   // 6b. N=2 with one target down: W=2 cannot be reached -> no false ACK.
   c.alive[0]=false; // node 1 down
   ok=true;calls=0;
   c.nodes[2]->put(K,10,{0x00FF00,60,2},[&](bool o,const auto&){ok=o;++calls;});
   c.pump();
   check(!ok&&calls==1,"N=2 with one target down must fail W=2 (no false ACK)");
  }

  // --- Test 7: N4->N2 shrink strands ACKed copies; superset backfill recovers ---
  {
   Cluster c;
   const std::uint32_t K=find_key_with_pair(c,1,2);
   const std::uint32_t color=0x556677;
   // Write at N=4 while the future N2 pair (nodes 1,2) is DOWN: W=2 is
   // satisfied by nodes 3 and 4, which the policy drops from the read set
   // when the region later shrinks to N=2 (the confirmed archive scenario:
   // region 133 pixel (86,140) held by nodes 1 and 10 while the N2 pair
   // became nodes 8 and 9).
   c.alive[0]=false;c.alive[1]=false;
   bool ok=false;
   c.nodes[2]->put(K,5,{color,70,1},[&](bool o,const auto&){ok=o;}); // coordinator = node 3 (up)
   c.pump();
   check(ok,"N=4 write with two failures must ACK via the survivors");
   auto r3=c.stores[2].get_region(K);
   auto r4=c.stores[3].get_region(K);
   check(r3&&r3->pixel(5).color==color,"node3 must hold the copy");
   check(r4&&r4->pixel(5).color==color,"node4 must hold the copy");
   c.alive[0]=true;c.alive[1]=true;
   // Policy shrinks the region to N=2: reads now target nodes 1 and 2,
   // whose stores are empty. Without the superset pull their backfill would
   // have a single source vs R=2 (guaranteed give_up) and the ACKed pixel
   // would be unreadable forever. The superset backfill pulls from the
   // former holders 3/4 and recovers it.
   c.set_plan_n(2);
   c.nodes[0]->reconsider_backfill(K);
   check(c.nodes[0]->backfill_step(),"post-shrink backfill must process");
   c.pump();c.pump();
   auto r1=c.stores[0].get_region(K);
   check(r1&&r1->pixel(5).color==color,
         "superset backfill must recover the copy stranded by the shrink");
   // A quorum read at N=2 succeeds, and read-repair propagates the copy to
   // the other N2 target (node 2).
   bool read=false;core::Region merged;
   c.nodes[0]->get(K,[&](bool o,const auto& r,const auto&){read=o;merged=r;});
   c.pump();
   check(read&&merged.pixel(5).color==color,"post-shrink N=2 read must see the pixel");
   auto r2=c.stores[1].get_region(K);
   check(r2&&r2->pixel(5).color==color,
         "read-repair must propagate the recovered copy to the other N2 target");
  }

  // --- Test 8: partial local copy must still re-pull after a shrink -------
  {
   Cluster c;
   const std::uint32_t K=find_key_with_pair(c,1,2);
   // px1: written at N4 while nodes 1,2 are down -> holders are 3 and 4.
   c.alive[0]=false;c.alive[1]=false;
   bool ok=false;
   c.nodes[2]->put(K,5,{0x111111,70,1},[&](bool o,const auto&){ok=o;});
   c.pump();
   check(ok,"px1 write must ACK via survivors");
   // px2: written with all nodes up -> the future N2 pair (1,2) holds it.
   c.alive[0]=true;c.alive[1]=true;
   ok=false;
   c.nodes[2]->put(K,9,{0x222222,80,1},[&](bool o,const auto&){ok=o;});
   c.pump();
   check(ok,"px2 write must ACK");
   // Shrink to N2 {1,2}: node 1 holds px2 but NOT px1 (partial, version>0).
   c.set_plan_n(2);
   check(c.stores[0].get_region(K) && c.stores[0].get_region(K)->pixel(9).color==0x222222,
         "node 1 must hold px2 (partial copy setup)");
   check(c.stores[0].get_region(K)->pixel(5).color!=0x111111,
         "node 1 must be missing px1 (stranded on former holders)");
   // Policy-change reconsider must re-pull even the partial region and merge
   // the stranded px1 from former holders 3/4 (LWW keeps px2 intact).
   c.nodes[0]->reconsider_backfill(K);
   check(c.nodes[0]->backfill_step(),"post-shrink reconsider must process the partial region");
   c.pump();c.pump();
   auto r1=c.stores[0].get_region(K);
   check(r1 && r1->pixel(5).color==0x111111,
         "superset pull must recover px1 stranded by the shrink");
   check(r1->pixel(9).color==0x222222,
         "merge must keep the newer local pixel (LWW)");
  }

  std::cout<<"PASS backfill completeness regression: first-R empty merge reproduced; "
             "complete_all+exclude_self backfill merges the populated replica; "
             "all-sources-empty accepts empty_valid once; partial merges retry "
             "bounded and terminate; stale epochs drop; N=2 quorum is strict; "
             "N4->N2 shrink stranding is recovered via the superset pull; "
             "partial local copies re-pull and merge stranded pixels\n";
 }catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}
 return 0;
}
