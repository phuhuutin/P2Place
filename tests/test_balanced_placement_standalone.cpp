#include "core/balanced_placement.hpp"
#include "core/consistent_hash_ring.hpp"
#include <algorithm>
#ifdef NDEBUG
#undef NDEBUG
#endif
#include <cassert>
#include <filesystem>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
using namespace p2place::core;
int main() {
    auto p=BalancedPlacement::create({3,1,2});
    assert(p.epoch()==1);
    assert(p.admission_order(1)==1);
    assert(p.admission_order(2)==2);
    assert(p.admission_order(3)==3);
    assert(p.primary_counts().at(1)==86);
    assert(p.primary_counts().at(2)==85);
    assert(p.primary_counts().at(3)==85);
    for(unsigned k=0;k<256;++k) {
        assert(p.primary(k)==1+(k%3));
        const auto pref=p.preference(k,3);
        assert(pref.size()==3 && pref[0]==p.primary(k));
        assert(std::set<NodeId>(pref.begin(),pref.end()).size()==3);
    }
    ConsistentHashRing ring;
    ring.rebuild(p);
    assert(ring.coordinator(34)==p.primary(34));
    assert(ring.successors(34,3)==p.preference(34,3));
    // Verify one full clockwise preference order and replica workload balance.
    auto count_replicas=[](const BalancedPlacement& x, std::size_t n){
        std::map<NodeId, std::size_t> counts;
        for (NodeId id : x.members()) counts[id]=0;
        for(unsigned key=0;key<256;++key) {
            auto pref=x.preference(key,n);
            assert(pref.size()==std::min(n,x.members().size()));
            assert(std::set<NodeId>(pref.begin(),pref.end()).size()==pref.size());
            for(NodeId id:pref) ++counts[id];
        }
        auto [mi,ma]=std::minmax_element(counts.begin(),counts.end(),
            [](const auto& a,const auto& b){return a.second<b.second;});
        assert(ma->second-mi->second<=n+1);
    };
    count_replicas(p,3);
    const auto original=p;
    const auto moved=p.plan_join(4);
    assert(moved==64);
    assert(p.admission_order(4)==4);
    for(unsigned k=0;k<256;++k)
        assert(p.primary(k)==original.primary(k) || p.primary(k)==4);
    for(const auto& [id,count]:p.primary_counts()) {
        (void)id; assert(count==64);
    }
    count_replicas(p,3);
    const auto file=std::filesystem::temp_directory_path()/"p2place_balanced_test.txt";
    p.save(file);
    const auto loaded=BalancedPlacement::load(file);
    assert(loaded.owners()==p.owners());
    assert(loaded.fingerprint()==p.fingerprint());
    assert(loaded.epoch()==p.epoch());
    assert(loaded.admission_order(4)==4);
    std::filesystem::remove(file);
    assert(p.plan_leave(4)>=64);
    assert(p.primary_counts().at(1)==86);
    assert(p.primary_counts().at(2)==85);
    assert(p.primary_counts().at(3)==85);
    assert(p.members()==original.members());
    count_replicas(p,3);
    // Stress many consecutive joins/leaves; balance invariant always holds.
    auto many=BalancedPlacement::create({1});
    for(NodeId id=2; id<=32; ++id) { many.plan_join(id); many.validate(); }
    for(NodeId id=32; id>=2; --id) { many.plan_leave(id); many.validate(); }
    for(unsigned k=0;k<256;++k) assert(many.primary(k)==1);
    bool threw=false;
    try { (void)many.primary(256); } catch(const std::out_of_range&) { threw=true; }
    assert(threw);
    std::cout << "PASS balanced-placement: quotas, join, leave, persistence, ring, churn\n";
}
