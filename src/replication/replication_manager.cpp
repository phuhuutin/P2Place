#include "replication/replication_manager.hpp"
#include "messages/payloads.hpp"
#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <utility>
namespace p2place::replication {
void QuorumConfig::validate() const {
    if(!N || !R || !W || R>N || W>N || N>256)
        throw std::invalid_argument("require 1 <= R,W <= N <= 256");
}
ReplicationManager::ReplicationManager(NodeId local,core::ConsistentHashRing& ring,
 core::RegionStore& store,const ReplicationStrategy& strategy,Send send,Schedule schedule,Cancel cancel)
 :local_(local),ring_(ring),store_(store),strategy_(strategy),send_(std::move(send)),
  schedule_(std::move(schedule)),cancel_(std::move(cancel)) {
 strategy_.plan(0).validate();
}
std::uint32_t ReplicationManager::id() {
 for(std::uint64_t n=0;n<UINT32_MAX;++n) {
  const auto candidate=++next_id_;
  if(candidate && !pending_.contains(candidate))return candidate;
 }
 throw std::runtime_error("replication request IDs exhausted");
}
std::set<ReplicationManager::NodeId> ReplicationManager::targets(std::uint32_t key,std::size_t count) const {
 if(key>=core::RegionStore::REGION_COUNT)throw std::out_of_range("region key");
 const auto v=ring_.successors(key,count);
 if(v.empty())throw std::runtime_error("no physical replicas on ring");
 // Under-replication: with fewer live members than target N, assign the
 // available distinct replicas and let the strict R/W quorum decide success.
 // The configured N/R/W contract is never silently altered; callers log
 // target_n vs assigned so experiments can see the shortfall.
 return {v.begin(),v.end()};
}
bool ReplicationManager::dispatch(NodeId peer,const messages::Message& m) {
 if(peer==local_) {
  // Local messages run the same authenticated response path as remote messages.
  if(m.type==messages::Type::PUT || m.type==messages::Type::GET || m.type==messages::Type::REGION_PUT) {
   on_message(local_,m);return true;
  }
  return false;
 }
 try {return send_(peer,m);}catch(const std::exception&){return false;}
}
std::uint32_t ReplicationManager::put(std::uint32_t key,std::uint16_t index,
 core::VersionedPixel pixel,PutResult callback) {
 if(key>=core::RegionStore::REGION_COUNT || index>=core::Region::COUNT)
  throw std::out_of_range("PUT key/index");
 const auto plan=strategy_.plan(key);plan.validate();
 auto peers=targets(key,plan.N);
 const auto request=id();
 Pending p; p.key=key;p.read=false;p.expected=peers;p.required=plan.W;p.put_callback=std::move(callback);
 p.started=std::chrono::steady_clock::now();
 pending_.emplace(request,std::move(p));
 std::cout << "[quorum] node=" << local_ << " op=PUT request=" << request << " region=" << key << " index=" << index << " N=" << plan.N << " assigned=" << peers.size() << " W=" << plan.W << " targets=";
 for(auto n:peers)std::cout << n << ",";
 std::cout << '\n';
 pending_.at(request).timer=schedule_([this,request]{deadline(request);});
 messages::Message m; m.type=messages::Type::PUT;m.request_id=request;
 m.payload=messages::encode_put({key,index,pixel});
 for(auto peer:peers) {
  if(!pending_.contains(request))break;
  if(!dispatch(peer,m))failed(request,peer);
 }
 return request;
}
std::uint32_t ReplicationManager::get(std::uint32_t key,GetResult callback) {
 const auto plan=strategy_.plan(key);plan.validate();
 auto peers=targets(key,plan.N);
 const auto request=id();
 Pending p; p.key=key;p.read=true;p.expected=peers;p.required=plan.R;p.get_callback=std::move(callback);
 p.started=std::chrono::steady_clock::now();
 pending_.emplace(request,std::move(p));
 std::cout << "[quorum] node=" << local_ << " op=GET request=" << request << " region=" << key << " N=" << plan.N << " assigned=" << peers.size() << " R=" << plan.R << " targets=";
 for(auto n:peers)std::cout << n << ",";
 std::cout << '\n';
 pending_.at(request).timer=schedule_([this,request]{deadline(request);});
 messages::Message m;m.type=messages::Type::GET;m.request_id=request;
 m.payload=messages::encode_get({key});
 for(auto peer:peers) {
  if(!pending_.contains(request))break;
  if(!dispatch(peer,m))failed(request,peer);
 }
 return request;
}
void ReplicationManager::deadline(std::uint32_t request) {
 if(auto it=pending_.find(request);it!=pending_.end()) {
  std::cerr << "[quorum-timeout] node=" << local_ << " request=" << request << " received=" << it->second.responded.size() << "/" << it->second.required << '\n';
  if(it->second.read)finish_get(request,false,"read quorum timeout");
  else finish_put(request,false,"write quorum timeout");
 }
}
void ReplicationManager::finish_put(std::uint32_t request,bool ok,const std::string& error) {
 auto it=pending_.find(request);if(it==pending_.end())return;
 auto p=std::move(it->second);pending_.erase(it);cancel_(p.timer);
 const auto ms=std::chrono::duration_cast<std::chrono::milliseconds>(
     std::chrono::steady_clock::now()-p.started).count();
 std::cout << "[quorum] op=PUT ok=" << ok << " ack_unique=" << p.responded.size() << "/" << p.required
           << " assigned=" << p.expected.size() << " latency_ms=" << ms << " error=" << error << '\n';
 std::cout << "[quorum-done] node=" << local_ << " op=PUT request=" << request << " ok=" << ok << " ack=" << p.responded.size() << "/" << p.required << " error=" << error << '\n';
 if(p.put_callback)p.put_callback(ok,error);
}
void ReplicationManager::finish_get(std::uint32_t request,bool ok,const std::string& error) {
 auto it=pending_.find(request);if(it==pending_.end())return;
 auto p=std::move(it->second);pending_.erase(it);cancel_(p.timer);
 const auto ms=std::chrono::duration_cast<std::chrono::milliseconds>(
     std::chrono::steady_clock::now()-p.started).count();
 std::cout << "[quorum] op=GET ok=" << ok << " ack_unique=" << p.responded.size() << "/" << p.required
           << " assigned=" << p.expected.size() << " latency_ms=" << ms << " error=" << error << '\n';
 std::cout << "[quorum-done] node=" << local_ << " op=GET request=" << request << " ok=" << ok << " ack=" << p.responded.size() << "/" << p.required << " error=" << error << '\n';
 core::Region merged;
 if(ok) {
  for(const auto& [node,region]:p.versions) { (void)node;merged.merge(region); }
  // Repair every stale respondent. A newer reply arrives later? Future reads repair it.
  for(const auto& [node,region]:p.versions) {
   bool stale=false;
   for(std::size_t i=0;i<core::Region::COUNT;++i)
    if(merged.pixel(i).newer_than(region.pixel(i))) {stale=true;break;}
   if(stale) {
    std::cout << "[read-repair] node=" << local_ << " region=" << p.key << " target=" << node << '\n';
    messages::Message repair;repair.type=messages::Type::REGION_PUT;
    repair.payload=messages::encode_region({p.key,merged});
    dispatch(node,repair); // best effort; Lab06 adds durable anti-entropy.
   }
  }
 }
 if(p.get_callback)p.get_callback(ok,merged,error);
}
void ReplicationManager::failed(std::uint32_t request,NodeId peer) {
 auto it=pending_.find(request);if(it==pending_.end())return;
 auto& p=it->second;
 if(!p.expected.contains(peer) || p.responded.contains(peer))return;
 p.failed.insert(peer);
 std::cerr << "[quorum-failure] node=" << local_ << " request=" << request << " failed-peer=" << peer << " failed=" << p.failed.size() << '\n';
 if(p.expected.size()-p.failed.size()<p.required) {
  if(p.read)finish_get(request,false,"read quorum unavailable");
  else finish_put(request,false,"write quorum unavailable");
 }
}
void ReplicationManager::on_disconnect(NodeId peer) {
 std::vector<std::uint32_t> ids;
 for(const auto& [id,p]:pending_)if(p.expected.contains(peer))ids.push_back(id);
 for(auto id:ids)failed(id,peer);
}
bool ReplicationManager::anti_entropy_step() {
 // Send at most one populated region per tick to its other canonical replicas.
 // No disk writes; receiver LWW-merges. A send is never counted as a quorum ACK.
 const auto keys=store_.all_keys();
 if(keys.empty())return false;
 if(repair_cursor_>=keys.size())repair_cursor_=0;
 const auto key=keys[repair_cursor_++];
 const auto plan=strategy_.plan(key);plan.validate();
 const auto owners=ring_.successors(key,plan.N);
 if(std::find(owners.begin(),owners.end(),local_)==owners.end())return false;
 const auto region=store_.get_region(key);
 if(!region)return false;
 messages::Message m;m.type=messages::Type::REGION_PUT;
 m.payload=messages::encode_region({key,*region});
 bool sent=false;
 for(auto peer:owners) {
   if(peer==local_)continue;
   if(dispatch(peer,m)) {repair_bytes_sent_+=m.payload.size();sent=true;
    std::cout << "[anti-entropy] node=" << local_ << " region=" << key << " to=" << peer << " bytes=" << m.payload.size() << " success=true\n";}
 }
 return sent;
}
void ReplicationManager::cancel_all(const std::string& reason) {
 while(!pending_.empty()) {
  auto id=pending_.begin()->first;
  if(pending_.at(id).read)finish_get(id,false,reason);
  else finish_put(id,false,reason);
 }
}
void ReplicationManager::on_message(NodeId sender,const messages::Message& msg) {
 const bool response=(msg.flags & messages::IS_RESPONSE)!=0;
 if(!response) {
  if(msg.type==messages::Type::REGION_PUT) {
   const auto r=messages::decode_region(msg.payload);
   if(r.key>=core::RegionStore::REGION_COUNT)throw std::runtime_error("invalid repair key");
   const auto pref=ring_.successors(r.key,strategy_.plan(r.key).N);
   if(std::find(pref.begin(),pref.end(),local_)==pref.end() ||
      std::find(pref.begin(),pref.end(),sender)==pref.end())return;
   store_.merge_region(r.key,r.region);
   std::cout << "[replica-repair] node=" << local_ << " from=" << sender << " region=" << r.key << " version=" << r.region.version() << '\n';
   return;
  }
  if(msg.type!=messages::Type::PUT && msg.type!=messages::Type::GET)return;
  messages::Message reply;reply.request_id=msg.request_id;reply.flags=messages::IS_RESPONSE;
  if(msg.type==messages::Type::PUT) {
   const auto put=messages::decode_put(msg.payload);
   if(put.key>=core::RegionStore::REGION_COUNT || put.pixel_index>=core::Region::COUNT)
    throw std::runtime_error("invalid replica PUT");
   const auto pref=ring_.successors(put.key,strategy_.plan(put.key).N);
   if(std::find(pref.begin(),pref.end(),local_)==pref.end()) {
    reply.type=messages::Type::ERROR;
   }else{
    // Stale/replayed writes are idempotent: acknowledge the valid stored max.
    const bool changed=store_.apply_pixel(put.key,put.pixel_index,put.pixel);
    std::cout << "[replica-put] node=" << local_ << " from=" << sender << " request=" << msg.request_id << " region=" << put.key << " index=" << put.pixel_index << " version=" << put.pixel.lamport << " applied=" << changed << '\n';
    reply.type=messages::Type::PUT_OK;
    reply.payload=messages::encode_put_ok({put.key,store_.get_region(put.key)->version()});
   }
  } else {
   const auto get=messages::decode_get(msg.payload);
   if(get.key>=core::RegionStore::REGION_COUNT)throw std::runtime_error("invalid replica GET");
   const auto pref=ring_.successors(get.key,strategy_.plan(get.key).N);
   if(std::find(pref.begin(),pref.end(),local_)==pref.end())reply.type=messages::Type::ERROR;
   else {
    std::cout << "[replica-get] node=" << local_ << " from=" << sender << " request=" << msg.request_id << " region=" << get.key << '\n';
    reply.type=messages::Type::GET_OK;
    reply.payload=messages::encode_region({get.key,store_.get_region(get.key).value_or(core::Region{})});}
  }
  if(sender==local_)on_message(local_,reply);
  else { try {send_(sender,reply);}catch(const std::exception&){} }
  return;
 }
 auto it=pending_.find(msg.request_id);if(it==pending_.end()){
  // request_id 0 responses (e.g. anti-entropy/repair echoes) are not quorum
  // ACKs; drop them silently instead of logging misleading quorum-ignore noise.
  if(msg.request_id==0)return;
  std::cout << "[quorum-ignore] node=" << local_ << " reason=late-or-unknown request=" << msg.request_id << " sender=" << sender << '\n';return;}
 auto& p=it->second;
 if(!p.expected.contains(sender) || p.responded.contains(sender) || p.failed.contains(sender)){
  std::cout << "[quorum-ignore] node=" << local_ << " request=" << msg.request_id << " sender=" << sender << " reason=duplicate-unexpected-or-failed\n";return;}
 if(msg.type==messages::Type::ERROR) {failed(msg.request_id,sender);return;}
 if(!p.read && msg.type==messages::Type::PUT_OK) {
  const auto ack=messages::decode_put_ok(msg.payload);
  if(ack.key!=p.key)return;
  p.responded.insert(sender);
  std::cout << "[quorum-ack] node=" << local_ << " op=PUT request=" << msg.request_id << " from=" << sender << " count=" << p.responded.size() << "/" << p.required << '\n';
  if(p.responded.size()>=p.required)finish_put(msg.request_id,true,{});
 } else if(p.read && msg.type==messages::Type::GET_OK) {
  const auto region=messages::decode_region(msg.payload);
  if(region.key!=p.key)return;
  p.responded.insert(sender);p.versions.emplace(sender,region.region);
  std::cout << "[quorum-ack] node=" << local_ << " op=GET request=" << msg.request_id << " from=" << sender << " count=" << p.responded.size() << "/" << p.required << '\n';
  if(p.responded.size()>=p.required)finish_get(msg.request_id,true,{});
 }
}
}
