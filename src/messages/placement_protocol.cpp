#include "messages/placement_protocol.hpp"
#include <stdexcept>
#include <limits>
namespace p2place::messages {
namespace {
void put(std::vector<std::uint8_t>& v, std::uint64_t n, unsigned bytes) {
    while(bytes--) v.push_back(static_cast<std::uint8_t>(n >> (bytes*8)));
}
std::uint64_t take(std::span<const std::uint8_t> b, std::size_t& at, unsigned bytes) {
    if (bytes > b.size()-at) throw std::invalid_argument("truncated placement payload");
    std::uint64_t n=0;
    while(bytes--) n=(n<<8)|b[at++];
    return n;
}
}
std::vector<std::uint8_t> encode_placement(const core::BalancedPlacement& p) {
    p.validate();
    std::vector<std::uint8_t> b;
    b.reserve(8+2+12*p.members().size()+4*256+8);
    put(b,p.epoch(),8);
    const auto members=p.members();
    put(b,members.size(),2);
    for(auto id:members) put(b,id,4);
    for(auto id:members) put(b,p.admission_order(id),8);
    for(auto id:p.owners()) put(b,id,4);
    put(b,p.fingerprint(),8);
    return b;
}
core::BalancedPlacement decode_placement(std::span<const std::uint8_t> b) {
    std::size_t at=0;
    const auto generation=take(b,at,8);
    const auto count=take(b,at,2);
    if(count==0 || count>256 || b.size()!=8+2+count*12+256*4+8)
        throw std::invalid_argument("invalid placement payload length");
    std::vector<core::NodeId> members;
    for(std::size_t i=0;i<count;++i) members.push_back(static_cast<core::NodeId>(take(b,at,4)));
    std::map<core::NodeId,std::uint64_t> admission;
    for(auto id:members) admission[id]=take(b,at,8);
    core::BalancedPlacement::Owners owners{};
    for(auto& owner:owners) owner=static_cast<core::NodeId>(take(b,at,4));
    const auto checksum=take(b,at,8);
    auto result=core::BalancedPlacement::from_snapshot(generation,std::move(members),owners,std::move(admission));
    if (result.fingerprint()!=checksum) throw std::invalid_argument("placement fingerprint mismatch");
    return result;
}
std::vector<std::uint8_t> encode_partition_id(std::uint32_t key) {
    if(key>=256) throw std::invalid_argument("invalid partition key");
    std::vector<std::uint8_t> b; put(b,key,4);return b;
}
std::uint32_t decode_partition_id(std::span<const std::uint8_t> b) {
    if(b.size()!=4) throw std::invalid_argument("invalid partition key payload");
    std::size_t at=0; auto key=take(b,at,4);
    if(key>=256) throw std::invalid_argument("invalid partition key");
    return static_cast<std::uint32_t>(key);
}
}
