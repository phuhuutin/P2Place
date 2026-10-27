#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
namespace p2place::net {
inline constexpr std::uint16_t MAX_MESSAGE_SIZE = 16384;
// Body length counts bytes after the two-byte header, NOT total packet length.
std::vector<std::uint8_t> prefix_message(std::string_view message);
// For a BLOCKING socket only. Async writes use TcpConnection's send buffer.
bool sendFramed(int fd, std::string_view message);
} // namespace p2place::net
