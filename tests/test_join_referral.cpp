#include "messages/join_request.hpp"
#include <cassert>
#include <stdexcept>
#include <vector>
int main() {
    using namespace p2place::messages;
    const JoinReferral a{4, 0x7f000001u, 9030};
    const auto bytes=encode_join_referral(a);
    assert(bytes.size()==10);
    assert((bytes==std::vector<std::uint8_t>{0,0,0,4,127,0,0,1,0x23,0x46}));
    const auto b=decode_join_referral(bytes);
    assert(b.node_id==a.node_id && b.ipv4==a.ipv4 && b.tcp_port==a.tcp_port);
    for(auto invalid : {std::vector<std::uint8_t>{}, std::vector<std::uint8_t>(9),
                        std::vector<std::uint8_t>(11), std::vector<std::uint8_t>(10)}) {
        bool rejected=false;
        try { (void)decode_join_referral(invalid); } catch(const std::invalid_argument&) { rejected=true; }
        assert(rejected);
    }
    return 0;
}
