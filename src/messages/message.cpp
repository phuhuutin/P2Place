/**
 * @file messages/message.cpp
 * @brief Implementation of the 6-byte message envelope codec.
 */
#include "messages/message.hpp"
#include <stdexcept>
namespace p2place::messages {
std::vector<std::uint8_t> encode(const Message& m) {
    std::vector<std::uint8_t> b = {
        static_cast<std::uint8_t>(m.type), 
        m.flags,
        static_cast<std::uint8_t>(m.request_id>>24), static_cast<std::uint8_t>(m.request_id>>16),
        static_cast<std::uint8_t>(m.request_id>>8),static_cast<std::uint8_t>(m.request_id)
    };
    b.insert(b.end(),m.payload.begin(),m.payload.end()); 
    return b;
}
Message decode(std::span<const std::uint8_t> b) {
    if(b.size()<6) throw std::invalid_argument("message envelope shorter than 6 bytes");
    if(b[0]<1 || b[0]>14) throw std::invalid_argument("unknown message type");
    return {static_cast<Type>(b[0]),b[1],
       (std::uint32_t(b[2])<<24)|(std::uint32_t(b[3])<<16)|(std::uint32_t(b[4])<<8)|std::uint32_t(b[5]),
       std::vector<std::uint8_t>(b.begin()+6,b.end())};
}
}
