#include "core/automatic_placement.hpp"
#include "core/consistent_hash_ring.hpp"
#include "core/region_store.hpp"
#include "messages/placement_protocol.hpp"
#include "messages/payloads.hpp"
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <vector>
using namespace p2place;
static void check_counts(const core::BalancedPlacement& p,std::size_t difference) {
    auto c=p.primary_counts();
    std::size_t small=256,large=0,sum=0;
    for(auto [id,n]:c) { (void)id;small=std::min(small,n);large=std::max(large,n);sum+=n; }
    assert(sum==256 && large-small<=difference);
}
int main() {
    namespace fs=std::filesystem;
    auto folder=fs::temp_directory_path()/"p2place_dynamic_placement_standalone";
    fs::remove_all(folder);
    fs::create_directories(folder);
    const auto path=folder/"placement.state";
    core::AutomaticPlacement authority(path);
    authority.bootstrap(1);
    assert(authority.generation()==1);
    assert(authority.active().primary_counts().at(1)==256);
    authority.propose_join(2);
    assert(authority.pending().has_value());
    assert(authority.generation()==1); // PREPARE never activates ownership
    assert(authority.pending()->epoch()==2);
    authority.abort();
    assert(authority.generation()==1);
    assert(!authority.pending());
    assert(authority.propose_join(2)==128);
    authority.activate();
    assert(authority.generation()==2);
    assert(authority.active().primary_counts().at(1)==128);
    assert(authority.active().primary_counts().at(2)==128);
    authority.propose_join(3);
    authority.activate();
    check_counts(authority.active(),1);
    assert(authority.propose_join(4)==64);
    auto new_placement=*authority.pending();
    std::size_t ownership_changes=0;
    for(unsigned k=0;k<256;++k)
        ownership_changes+=(authority.active().primary(k)!=new_placement.primary(k));
    assert(ownership_changes==64);
    auto b=messages::encode_placement(new_placement);
    auto decoded=messages::decode_placement(b);
    assert(decoded.fingerprint()==new_placement.fingerprint());
    b.back()^=1;
    try { (void)messages::decode_placement(b);assert(false); }
    catch(const std::invalid_argument&) {}
    authority.activate();
    assert(authority.generation()==4);
    assert(decoded.admission_order(4)==4);
    check_counts(authority.active(),0);
    assert(authority.active().primary_counts().at(4)==64);
    core::ConsistentHashRing ring;
    ring.rebuild(authority.active());
    assert(ring.coordinator(0).has_value());
    for(unsigned k=0;k<256;++k) {
        const auto list=ring.successors(k,3);
        assert(list.size()==3);
        assert(list[0]==*ring.coordinator(k));
        assert(list[0]!=list[1] && list[1]!=list[2] && list[0]!=list[2]);
    }
    // Confirm a region can be transferred without losing Lamport versions.
    core::RegionStore old_owner,new_owner;
    core::VersionedPixel px{0x367F63,100,1};
    old_owner.apply_pixel(163,126,px);
    const auto snapshot=old_owner.get_region(163).value();
    const auto data=messages::encode_region({163,snapshot});
    const auto roundtrip=messages::decode_region(data);
    new_owner.merge_region(roundtrip.key,roundtrip.region);
    const auto received=new_owner.get_region(163).value();
    assert(received.pixel(126).color==0x367F63);
    assert(received.pixel(126).lamport==100);
    // Replay is safe: LWW keeps the existing pixel.
    new_owner.merge_region(roundtrip.key,roundtrip.region);
    assert(new_owner.get_region(163)->pixel(126).lamport==100);
    // Restart with same state: no ownership changes or manual epoch file.
    core::AutomaticPlacement restarted(path);
    restarted.bootstrap(1);
    assert(restarted.generation()==4);
    assert(restarted.active().fingerprint()==authority.active().fingerprint());
    // Graceful departure rebalances the same 256 partitions.
    assert(restarted.propose_leave(2)>=64);
    restarted.activate();
    assert(restarted.generation()==5);
    assert(!restarted.active().contains(2));
    check_counts(restarted.active(),1);
    // Node 2's local state may exclude itself after departure.
    auto follower_path=folder/"node2.state";
    restarted.active().save(follower_path);
    core::AutomaticPlacement returning(follower_path);
    returning.bootstrap(2);
    assert(!returning.active().contains(2));
    const auto before=returning.active().fingerprint();
    returning.install_committed(restarted.active());
    assert(returning.active().fingerprint()==before);
    auto rejoin=restarted.active();
    rejoin.plan_join(2);
    returning.install_committed(rejoin);
    assert(returning.active().contains(2));
    assert(returning.generation()==6);
    fs::remove_all(folder);
    std::cout<<"PASS: local versions, joins/leaves, checksums, handoff, rejoin\n";
}
