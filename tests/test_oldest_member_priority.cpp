#include "core/balanced_placement.hpp"
#include "messages/placement_protocol.hpp"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <vector>

int main() {
    using namespace p2place;
    auto p = core::BalancedPlacement::create({1});
    p.plan_join(9);       // joined second, but higher numeric ID
    p.plan_join(2);       // joined third, but lower numeric ID
    assert(p.members() == (std::vector<core::NodeId>{1,2,9}));
    assert(p.admission_order(1)==1 && p.admission_order(9)==2 && p.admission_order(2)==3);

    auto oldest=[](const core::BalancedPlacement& p, const std::vector<core::NodeId>& live){
        auto best=live.at(0);
        for (auto n : live) if(p.admission_order(n)<p.admission_order(best)) best=n;
        return best;
    };
    assert(oldest(p,{2,9})==9);          // after node 1 crashes, NOT node 2
    assert(oldest(p,{1,2,9})==1);

    const auto path=std::filesystem::temp_directory_path()/"p2place_oldest_test.state";
    p.save(path);
    const auto recovered=core::BalancedPlacement::load(path);
    assert(recovered.fingerprint()==p.fingerprint());
    assert(oldest(recovered,{2,9})==9);
    std::filesystem::remove(path);

    const auto payload=messages::encode_placement(p);
    auto received=messages::decode_placement(payload);
    assert(received.fingerprint()==p.fingerprint());
    assert(oldest(received,{2,9})==9);

    received.plan_leave(9);
    assert(received.admission_order(2)==3);
    received.plan_join(9); // permanent leave + later rejoin => newest member
    assert(received.admission_order(9)==4);
    assert(oldest(received,{2,9})==2);
    std::cout << "PASS: oldest member order, non-monotonic Node IDs, persistence, codec, rejoin\n";
}
