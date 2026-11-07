#pragma once
#include <cstdint>
#include <span>
#include <vector>
namespace p2place::messages {
enum class Type : std::uint8_t {
    HELLO=1, PING=2, PONG=3, GET=4, GET_OK=5, PUT=6, PUT_OK=7,
    REGION_PUT=8, SYNC_REQ=9, SYNC_RESP=10, NODELIST=11, HINT=12, ERROR=13, BYE=14
};
inline constexpr std::uint8_t IS_RESPONSE = 0x01;
struct Message {
    Type type = Type::ERROR;
    std::uint8_t flags = 0;
    std::uint32_t request_id = 0;
    std::vector<std::uint8_t> payload;
};
std::vector<std::uint8_t> encode(const Message& msg);
Message decode(std::span<const std::uint8_t> bytes);
}
