#include "replication/replication_manager.hpp"
#include "core/balanced_placement.hpp"
#include "messages/payloads.hpp"
#include <iostream>
#include <stdexcept>
#include <map>
#include <set>
#include <vector>
#include <cassert>
using namespace p2place;
namespace {
void check(bool yes,const char* why){if(!yes)throw std::runtime_error(why);}
struct Test {
 core::ConsistentHashRing ring;
 core::RegionStore store;
 std::vector<std::pair<core::NodeId,messages::Message>> sent;
 std::map<net::TimerQueue::Token,std::function<void()>> timers;
 std::set<core::NodeId> disconnected;
 net::TimerQueue::Token next=1;
 replication::QuorumConfig config;
 std::unique_ptr<replication::FixedReplicationStrategy> strategy;
 std::unique_ptr<replication::ReplicationManager> mgr;
 Test(replication::QuorumConfig q={3,2,2}, std::vector<core::NodeId> members={1,2,3,4,5}) :config(q) {
  ring.rebuild(core::BalancedPlacement::create(std::move(members)));
  strategy=std::make_unique<replication::FixedReplicationStrategy>(config);
  mgr=std::make_unique<replication::ReplicationManager>(1,ring,store,*strategy,
   [this](auto id,const auto& m){if(disconnected.contains(id))return false;sent.emplace_back(id,m);return true;},
   [this](auto f){auto id=next++;timers.emplace(id,std::move(f));return id;},
   [this](auto id){timers.erase(id);});
 }
 void expire(){auto work=std::move(timers);timers.clear();for(auto& [id,fn]:work){(void)id;fn();}}
 void respond(core::NodeId from,const messages::Message& request, core::VersionedPixel px={}){
  messages::Message reply;reply.flags=messages::IS_RESPONSE;reply.request_id=request.request_id;
  if(request.type==messages::Type::PUT){
   auto put=messages::decode_put(request.payload);
   reply.type=messages::Type::PUT_OK;reply.payload=messages::encode_put_ok({put.key,put.pixel.lamport});
  } else {
   auto get=messages::decode_get(request.payload);
   core::Region r;if(px.lamport)r.set_pixel(0,px);
   reply.type=messages::Type::GET_OK;reply.payload=messages::encode_region({get.key,r});
  }
  mgr->on_message(from,reply);
 }
};
void duplicate_acks(){
 Test t;int calls=0;bool ok=false;
 auto id=t.mgr->put(1,0,{0x112233,10,1},[&](bool result,const auto&){++calls;ok=result;});
 check(t.sent.size()>=2,"expected remote sends");
 auto first=t.sent[0],second=t.sent[1];
 t.respond(first.first,first.second);t.respond(first.first,first.second);
 check(calls==0,"duplicate ACK counted");
 t.respond(second.first,second.second);
 check(calls==1&&ok,"2 unique ACKs should succeed");
 t.respond(second.first,second.second);t.expire();
 check(calls==1,"completed callback repeated");
 (void)id;
}
void timeout_and_bad_ack(){
 Test t;int calls=0;bool ok=true;
 t.mgr->put(1,0,{5,5,1},[&](bool r,const auto&){++calls;ok=r;});
 auto req=t.sent.at(0).second;
 t.respond(777,req);
 check(calls==0,"unselected reply counted");
 t.expire();check(calls==1&&!ok,"timeout failure missing");
 t.respond(t.sent[0].first,req);check(calls==1,"late ACK counted");
}
void one_failed_replica(){
 Test t;int calls=0;bool ok=false;
 auto replicas=t.ring.successors(1,3);
 t.disconnected.insert(replicas.back());
 t.mgr->put(1,1,{0x99,9,1},[&](bool r,const auto&){++calls;ok=r;});
 for(auto [id,msg]:t.sent)if(msg.type==messages::Type::PUT)t.respond(id,msg);
 check(calls==1&&ok,"W2 must succeed with one failed replica");
}
void two_failed_replicas(){
 Test t;int calls=0;bool ok=true;
 auto replicas=t.ring.successors(1,3);
 t.disconnected.insert(replicas.at(1));t.disconnected.insert(replicas.at(2));
 t.mgr->put(1,1,{0x99,9,1},[&](bool r,const auto&){++calls;ok=r;});
 check(calls==1&&!ok,"W2 should fail with two failed replicas");
}
void read_merge_repair(){
 Test t;int calls=0;bool ok=false;core::Region result;
 t.mgr->get(1,[&](bool r,const auto& data,const auto&){++calls;ok=r;result=data;});
 check(t.sent.size()>=2,"GET did not fan out");
 auto first=t.sent.at(0),second=t.sent.at(1);
 t.respond(first.first,first.second,{0xFF0000,10,1});
 t.respond(second.first,second.second,{0x0000FF,15,2});
 check(ok&&calls==1&&result.pixel(0).color==0x0000FF,"per-pixel LWW merge wrong");
 bool repair=false;
 for(const auto& [node,msg]:t.sent)
  if(node==first.first&&msg.type==messages::Type::REGION_PUT) {
   auto p=messages::decode_region(msg.payload);
   repair=p.region.pixel(0).color==0x0000FF;
  }
 check(repair,"stale replica did not receive read repair");
}
void local_replica_and_invalid_quorum(){
 Test t;int calls=0;bool ok=false;
 t.mgr->put(0,1,{0x22,3,1},[&](bool r,const auto&){++calls;ok=r;});
 // Local primary automatically applied and counted exactly once.
 check(t.store.get_region(0).has_value(),"local replica not stored");
 check(calls==0,"local replica counted twice");
 for(const auto& [node,msg]:t.sent)if(msg.type==messages::Type::PUT){t.respond(node,msg);break;}
 check(calls==1&&ok,"remote ACK did not complete local+remote W2");
 bool rejected=false;
 try{replication::QuorumConfig{3,4,2}.validate();}catch(const std::invalid_argument&){rejected=true;}
 check(rejected,"invalid R>N accepted");
 // Under-replication after failed-member removal: with only two live
 // members the manager assigns the two available replicas (target N stays 3,
 // reported via the assigned= log field) and the strict R/W quorum still
 // decides. It must never silently lower R or W.
 Test small({3,2,2},{1,2});
 int gcount=0;bool gok=false;core::Region gr;
 small.mgr->get(0,[&](bool r,const auto& data,const auto&){++gcount;gok=r;gr=data;});
 check(small.sent.size()==1,"GET must assign the single remote available replica");
 check(gcount==0,"local ACK alone must not satisfy R2");
 small.respond(small.sent.at(0).first,small.sent.at(0).second,{0xAB0000,4,1});
 check(gcount==1&&gok&&gr.pixel(0).color==0xAB0000,
       "strict R2 succeeds only when both live replicas respond");
 // One of the two live replicas unreachable: R2 must fail, not downgrade.
 Test small2({3,2,2},{1,2});
 small2.disconnected.insert(2);
 int fcount=0;bool fok=true;
 small2.mgr->get(0,[&](bool r,const auto&,const auto&){++fcount;fok=r;});
 check(fcount==1&&!fok,"R2 must fail when one of two live replicas is unreachable");
}
void mismatched_response(){
 Test t;bool finished=false;
 t.mgr->put(1,1,{0x22,1,1},[&](bool,const auto&){finished=true;});
 auto [node,msg]=t.sent.at(0);
 messages::Message wrong;wrong.type=messages::Type::GET_OK;wrong.flags=messages::IS_RESPONSE;
 wrong.request_id=msg.request_id;wrong.payload=messages::encode_region({1,{}});
 t.mgr->on_message(node,wrong);
 check(!finished,"wrong response type counted");
}
}
int main(){
 try {
  duplicate_acks();timeout_and_bad_ack();one_failed_replica();two_failed_replicas();
  read_merge_repair();local_replica_and_invalid_quorum();mismatched_response();
  std::cout<<"PASS Lab05: duplicate/late ACK, quorum failures, read merge, repair, local and invalid config\n";
 }catch(const std::exception& ex){std::cerr<<"FAIL Lab05: "<<ex.what()<<'\n';return 1;}
}
