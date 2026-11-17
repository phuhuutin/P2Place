#include "core/peer_reliability.hpp"
#include <algorithm>
#include <stdexcept>
namespace p2place::core {
namespace {
double to_seconds(PeerReliabilityTracker::Clock::duration d) {
    return std::chrono::duration<double>(d).count();
}
}
PeerReliabilityTracker::PeerReliabilityTracker(std::chrono::seconds window)
    : window_(window) {
    if(window_.count()<=0)throw std::invalid_argument("reliability window must be positive");
}

void PeerReliabilityTracker::observe(NodeId id,bool healthy,Clock::time_point now) {
    if(id==0)throw std::invalid_argument("invalid node id");
    auto& p=peers_[id];
    if(p.observed && healthy==p.current_healthy)return; // not a transition
    if(p.observed) {
        // Close the open interval (its stored end == start is the
        // "still open" sentinel; extend it to now). A zero-length
        // interval contributes nothing and is harmless to keep.
        p.intervals.back().end=now;
    } else {
        p.observed=true;
    }
    p.current_healthy=healthy;
    p.intervals.push_back({now,now,healthy});
    // Trim expired history: intervals entirely before the window start.
    const auto from=now-window_;
    while(p.intervals.size()>1 && p.intervals.front().end<=from)
        p.intervals.pop_front();
}

void PeerReliabilityTracker::mark_removed(NodeId id,Clock::time_point now) {
    if(id==0)throw std::invalid_argument("invalid node id");
    auto it=peers_.find(id);
    if(it==peers_.end())return; // never observed locally; nothing to close
    auto& p=it->second;
    if(!p.observed)return;
    if(!p.current_healthy)return; // already unhealthy (e.g. UNAVAILABLE)
    p.intervals.back().end=now;
    p.current_healthy=false;
    p.intervals.push_back({now,now,false});
    const auto from=now-window_;
    while(p.intervals.size()>1 && p.intervals.front().end<=from)
        p.intervals.pop_front();
}

PeerReliabilityTracker::Snapshot PeerReliabilityTracker::get(NodeId id,Clock::time_point now) const {
    Snapshot out;out.id=id;
    auto it=peers_.find(id);
    if(it==peers_.end())return out; // unknown: neutral, sampled=false
    const auto& p=it->second;
    const auto from=now-window_;
    if(p.intervals.empty())return out;
    double observed=0, healthy=0;
    for(std::size_t i=0;i<p.intervals.size();++i) {
        const auto& iv=p.intervals[i];
        // The final interval is still open: it extends to now (its stored
        // end==start is the "still open" sentinel).
        const bool open=(i+1==p.intervals.size());
        const auto s=std::max(iv.start,from);
        const auto e=open ? now : std::min(iv.end,now);
        if(e<=s)continue;
        observed+=to_seconds(e-s);
        if(iv.healthy)healthy+=to_seconds(e-s);
    }
    out.observed_seconds=observed;
    out.sampled=p.observed && observed>0;
    out.score=out.sampled ? healthy/observed : 1.0;
    return out;
}

std::vector<PeerReliabilityTracker::Snapshot> PeerReliabilityTracker::snapshot(Clock::time_point now) const {
    std::vector<Snapshot> out;out.reserve(peers_.size());
    for(const auto& [id,p]:peers_) {(void)p;out.push_back(get(id,now));}
    return out;
}
}
