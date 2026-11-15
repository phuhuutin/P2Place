#pragma once
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>

namespace p2place::messages {
// JOIN_REQUEST is a seed's referral for a directly connected joiner.
// Exactly 10 bytes: joining ID (u32 BE), IPv4 (u32 BE), TCP port (u16 BE).
// The authority must independently connect and verify the joining peer's HELLO.
struct JoinReferral {
    std::uint32_t node_id;
    std::uint32_t ipv4; // host-order numeric form, serialized network-order
    std::uint16_t tcp_port;
};
inline std::vector<std::uint8_t> encode_join_referral(JoinReferral v) {
    if (!v.node_id || !v.ipv4 || !v.tcp_port)
        throw std::invalid_argument("invalid join referral");
    return {
        static_cast<std::uint8_t>(v.node_id >> 24), static_cast<std::uint8_t>(v.node_id >> 16),
        static_cast<std::uint8_t>(v.node_id >> 8), static_cast<std::uint8_t>(v.node_id),
        static_cast<std::uint8_t>(v.ipv4 >> 24), static_cast<std::uint8_t>(v.ipv4 >> 16),
        static_cast<std::uint8_t>(v.ipv4 >> 8), static_cast<std::uint8_t>(v.ipv4),
        static_cast<std::uint8_t>(v.tcp_port >> 8), static_cast<std::uint8_t>(v.tcp_port)
    };
}
inline JoinReferral decode_join_referral(std::span<const std::uint8_t> b) {
    if (b.size()!=10) throw std::invalid_argument("JOIN_REQUEST requires 10 bytes");
    auto u32=[&](std::size_t i)->std::uint32_t {
        return (std::uint32_t(b[i]) << 24) | (std::uint32_t(b[i+1]) << 16) |
               (std::uint32_t(b[i+2]) << 8) | std::uint32_t(b[i+3]);
    };
    JoinReferral v{u32(0),u32(4),static_cast<std::uint16_t>((std::uint16_t(b[8])<<8)|b[9])};
    if (!v.node_id || !v.ipv4 || !v.tcp_port)
        throw std::invalid_argument("invalid JOIN_REQUEST fields");
    return v;
}
} // namespace p2place::messages
