#include "replication/replication_manager.hpp"
#include "core/balanced_placement.hpp"
#include <array>
#include <deque>
#include <iostream>
#include <stdexcept>
#include <memory>
using namespace p2place;
namespace {
void check(bool x,const char* why){if(!x)throw std::runtime_error(why);}
struct Cluster {
 struct Packet {core::NodeId src,dst;messages::Message msg;};
 core::ConsistentHashRing ring;
 std::array<core::RegionStore,3> stores;
 std::array<std::unique_ptr<replication::FixedReplicationStrategy>,3> strategies;
 std::array<std::unique_ptr<replication::ReplicationManager>,3> nodes;
 std::deque<Packet> packets;
 std::array<bool,3> alive={true,true,true};
 std::array<std::map<net::TimerQueue::Token,std::function<void()>>,3> timers;
 std::array<net::TimerQueue::Token,3> next={1,1,1};
 Cluster(){
  ring.rebuild(core::BalancedPlacement::create({1,2,3}));
  for(int i=0;i<3;++i){
   strategies[i]=std::make_unique<replication::FixedReplicationStrategy>(
    replication::QuorumConfig{3,2,2});
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
 void pump(){
  int steps=0;
  while(!packets.empty()){
   check(++steps<10000,"message loop");
   auto p=std::move(packets.front());packets.pop_front();
   if(alive[p.dst-1])nodes[p.dst-1]->on_message(p.src,p.msg);
  }
 }
};
}
int main(){
 try {
  Cluster c;bool result=false;int calls=0;
  c.nodes[0]->put(0,7,{0x123456,50,1},[&](bool ok,const auto&){result=ok;++calls;});
  c.pump();check(result&&calls==1,"three-node write quorum failed");
  for(int i=0;i<3;++i){auto r=c.stores[i].get_region(0);check(r&&r->pixel(7).color==0x123456,"replica missing pixel");}
  bool read=false;core::Region canvas;
  c.nodes[1]->get(0,[&](bool ok,const auto& r,const auto&){read=ok;canvas=r;});c.pump();
  check(read&&canvas.pixel(7).color==0x123456,"remote quorum GET wrong");
  // One physical replica crashes; read/write still reach W=2, R=2.
  c.alive[2]=false;
  result=false;calls=0;
  c.nodes[1]->put(0,7,{0xCC8844,60,2},[&](bool ok,const auto&){result=ok;++calls;});c.pump();
  check(result&&calls==1,"W2 failed with one unavailable node");
  read=false;c.nodes[0]->get(0,[&](bool ok,const auto& r,const auto&){read=ok;canvas=r;});c.pump();
  check(read&&canvas.pixel(7).color==0xCC8844,"R2 failed with one unavailable node");
  // Node 3 returns with stale in-memory replica; background anti-entropy
  // pushes RED(v2) from Node 1 without a browser GET.
  c.alive[2]=true;
  check(c.stores[2].get_region(0)->pixel(7).color==0x123456,
        "test setup must have stale replica");
  check(c.nodes[0]->anti_entropy_step(),"anti-entropy push failed");
  c.pump();
  check(c.stores[2].get_region(0)->pixel(7).color==0xCC8844,
        "background repair did not converge");
  c.alive[2]=false;
  c.alive[1]=false;
  result=true;calls=0;
  c.nodes[0]->put(0,7,{0xAABBCC,70,1},[&](bool ok,const auto&){result=ok;++calls;});c.pump();
  check(!result&&calls==1,"two failed nodes incorrectly satisfied quorum");
  std::cout<<"PASS Lab06 anti-entropy plus Lab05 network simulation: 3 physical copies, remote R2 read, one failure tolerated, two fail\n";
 }catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}
}
