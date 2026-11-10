#pragma once
#include <array>
#include <cstdint>
#include <string_view>
namespace p2place::util {
    std::array<std::uint8_t,20> sha1(std::string_view text);
    std::string websocket_accept_key(std::string_view client_key);
}
