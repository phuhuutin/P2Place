#include "core/replication_policy.hpp"
#include <algorithm>
#include <cmath>
#include <stdexcept>
namespace p2place::core {
namespace {
void put(std::vector<std::uint8_t>& v,std::uint64_t n) {
    for(int i=7;i>=0;--i)v.push_back(static_cast<std::uint8_t>(n>>(i*8)));
}
std::uint64_t take(std::span<const std::uint8_t> b,std::size_t& at) {
    if(at+8>b.size())throw std::invalid_argument("truncated policy snapshot");
    std::uint64_t n=0;for(int i=0;i<8;++i)n=(n<<8)|b[at++];
    return n;
}
}
std::vector<std::uint8_t> PolicySnapshot::encode() const {
    std::vector<std::uint8_t> b;
    b.reserve(8+8+8+1+BalancedPlacement::PARTITIONS);
    put(b,placement_gen);put(b,placement_fp);put(b,policy_epoch);
    b.push_back(strategy);
    b.insert(b.end(),target_n.begin(),target_n.end());
    return b;
}
PolicySnapshot PolicySnapshot::decode(std::span<const std::uint8_t> b) {
    if(b.size()!=8+8+8+1+BalancedPlacement::PARTITIONS)
        throw std::invalid_argument("invalid policy snapshot length");
    std::size_t at=0;
    PolicySnapshot s;
    s.placement_gen=take(b,at);s.placement_fp=take(b,at);s.policy_epoch=take(b,at);
    s.strategy=b[at++];
    for(auto& n:s.target_n)n=b[at++];
    return s;
}

PolicyTable::PolicyTable(std::uint8_t fallback_n):fallback_n_(fallback_n) {
    if(!fallback_n_)throw std::invalid_argument("invalid policy fallback N");
}
bool PolicyTable::apply(const PolicySnapshot& snap,std::string* reason) {
    const auto reject=[&](const char* why){if(reason)*reason=why;return false;};
    if(snap.placement_gen==0||snap.placement_fp==0||snap.policy_epoch==0)
        return reject("invalid snapshot fields");
    for(auto n:snap.target_n)
        if(n==0)return reject("target out of bounds");
    if(initialized_) {
        if(snap.placement_gen<current_.placement_gen)
            return reject("stale placement generation");
        if(snap.placement_gen==current_.placement_gen &&
           snap.placement_fp!=current_.placement_fp)
            return reject("conflicting placement fingerprint");
        if(snap.placement_gen==current_.placement_gen &&
           snap.policy_epoch<current_.policy_epoch)
            return reject("stale policy epoch");
        if(snap.placement_gen==current_.placement_gen &&
           snap.policy_epoch==current_.policy_epoch &&
           snap.target_n!=current_.target_n)
            return reject("same epoch with different targets");
        if(snap.placement_gen==current_.placement_gen &&
           snap.policy_epoch==current_.policy_epoch)
            return true; // idempotent re-broadcast
    }
    current_=snap;
    initialized_=true;
    return true;
}
std::uint8_t PolicyTable::target_n(std::uint32_t region) const {
    if(region>=BalancedPlacement::PARTITIONS)
        throw std::out_of_range("region out of bounds");
    if(!initialized_ || current_.target_n[region]==0)return fallback_n_;
    return current_.target_n[region];
}

PolicyEvaluator::PolicyEvaluator(PolicyConfig cfg):cfg_(cfg) {
    if(cfg_.min_n<2||cfg_.min_n>cfg_.max_n||cfg_.max_n>256)
        throw std::invalid_argument("invalid policy N bounds (min N = 2)");
    if(!(cfg_.threshold_low>0 && cfg_.threshold_low<cfg_.threshold_high &&
         cfg_.threshold_high<=1.0))
        throw std::invalid_argument("invalid policy thresholds");
    if(cfg_.cooldown.count()<0)throw std::invalid_argument("invalid policy cooldown");
}

PolicyEvaluation PolicyEvaluator::evaluate(const BalancedPlacement& placement,
        const PeerReliabilityTracker& reliability,
        const PolicyTable& table,
        Clock::time_point now,bool placement_changed) {
    PolicyEvaluation out;
    out.snap.placement_gen=placement.epoch();
    out.snap.placement_fp=placement.fingerprint();
    out.snap.strategy=static_cast<std::uint8_t>('B');
    const auto members=placement.members();
    const std::size_t baseline=std::min<std::size_t>(3,members.size());
    for(std::uint32_t r=0;r<BalancedPlacement::PARTITIONS;++r) {
        // Deterministic baseline: first three preferred members.
        const auto candidates=placement.preference(r,baseline);
        double min_score=1.0;
        for(auto id:candidates) {
            const auto s=reliability.get(id,now);
            // Unobserved/unknown peers are neutral 1.0 per spec; only
            // sampled scores lower the minimum.
            if(s.sampled)min_score=std::min(min_score,s.score);
        }
        const double risk=1.0-min_score;
        out.risk[r]=risk;
        // Three-tier adaptive mapping (N2 policy): healthy baselines
        // (min score >= threshold_high) keep the N2 baseline; moderate
        // churn (threshold_low..high) moves to the middle tier; heavy churn
        // (below threshold_low) raises the region to N4. With min N = 3 the
        // middle tier collapses to min_n, preserving the earlier two-tier
        // behavior for A/B comparison runs.
        std::uint8_t desired=static_cast<std::uint8_t>(cfg_.min_n);
        if(min_score<cfg_.threshold_high) {
            const int mid=std::max<int>(static_cast<int>(cfg_.min_n),
                std::min<int>(static_cast<int>(cfg_.min_n)+1,
                              static_cast<int>(cfg_.max_n)-1));
            desired=static_cast<std::uint8_t>(mid);
        }
        if(min_score<cfg_.threshold_low)
            desired=static_cast<std::uint8_t>(cfg_.max_n);
        const auto assigned=static_cast<std::uint8_t>(
            std::min<std::size_t>(desired,members.size()));
        // Carry the previously applied target forward.
        const auto previous=table.current().target_n[r]
            ? static_cast<std::uint8_t>(table.current().target_n[r]) : desired;
        out.snap.target_n[r]=previous;
        if(assigned==previous)continue;
        // 30-second per-region cooldown, waived on placement changes.
        if(!placement_changed && now-last_change_[r]<cfg_.cooldown)continue;
        out.snap.target_n[r]=assigned;
        last_change_[r]=now;
        out.changed=true;
    }
    if(out.changed || !table.initialized()) {
        // Monotonic epoch across placements; never reuse an old epoch.
        out.changed=true;
        out.snap.policy_epoch=std::max(table.current().policy_epoch+1,
                                       std::uint64_t{1});
        if(!table.initialized()) {
            // The baseline->first snapshot assignment starts every
            // region's cooldown clock.
            for(auto& t:last_change_)t=now;
        }
    } else {
        out.snap.policy_epoch=table.current().policy_epoch;
        out.snap.target_n=table.current().target_n;
    }
    return out;
}
}
