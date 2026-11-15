#include "core/failure_detector.hpp"
#include <stdexcept>
namespace p2place::core {
FailureDetector::FailureDetector(Config cfg):cfg_(cfg) {
 if(cfg_.suspect_after.count()<=0 || cfg_.unavailable_after<=cfg_.suspect_after ||
    cfg_.recovery_stable_for.count()<0) throw std::invalid_argument("invalid failure detector thresholds");
}
void FailureDetector::observe(NodeId id, Clock::time_point at) {
 if(id==0)throw std::invalid_argument("invalid node id");
 auto it=peers_.find(id);
 if(it==peers_.end()) {peers_.emplace(id, Peer{at,{},{}});return;}
 auto& p=it->second;
 if(at<p.last)return; // old observation cannot turn back the clock
 if(p.disconnected_at && !p.recovery_started)p.recovery_started=at;
 if(p.recovery_started && at-*p.recovery_started>=cfg_.recovery_stable_for){
    p.disconnected_at.reset();p.recovery_started.reset();
 }
 p.last=at;
}
void FailureDetector::disconnected(NodeId id, Clock::time_point at) {
 auto it=peers_.find(id);if(it==peers_.end()) {peers_.emplace(id,Peer{at,at,{}});return;}
 it->second.disconnected_at=at;it->second.recovery_started.reset();
}
FailureDetector::State FailureDetector::state(NodeId id,Clock::time_point now) const {
 auto it=peers_.find(id);if(it==peers_.end())return State::Unavailable;
 const auto& p=it->second;
 if(p.disconnected_at) {
    if(p.recovery_started && now-*p.recovery_started>=cfg_.recovery_stable_for &&
       now-p.last<cfg_.suspect_after) return State::Healthy;
    if(now-*p.disconnected_at>=cfg_.unavailable_after)return State::Unavailable;
    return State::Suspect;
 }
 if(now-p.last>=cfg_.unavailable_after)return State::Unavailable;
 if(now-p.last>=cfg_.suspect_after)return State::Suspect;
 return State::Healthy;
}
std::vector<NodeId> FailureDetector::unavailable(Clock::time_point now) const {
 std::vector<NodeId> ids;for(const auto& [id,p]:peers_) {
   (void)p;if(state(id,now)==State::Unavailable)ids.push_back(id);
 }return ids;
}
}
