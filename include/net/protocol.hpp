/**
 * @file net/protocol.hpp
 * @brief 2-byte big-endian length framing helpers (blocking sockets).
 */
#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

/**
 * @brief Networking: epoll loop, sockets, connections, framing codecs.
 */
namespace p2place::net {

/** Maximum payload size per frame on the peer channel (16 KiB). */
inline constexpr std::uint16_t MAX_MESSAGE_SIZE = 16384;

/**
 * @brief Prefix a body with its 2-byte big-endian length.
 *
 * Body length counts bytes after the two-byte header, NOT the total
 * packet length.
 * @param message The payload to frame.
 * @return [uint16 BE total_len][payload].
 * @throws std::length_error if the body is empty or exceeds
 *         MAX_MESSAGE_SIZE.
 */
std::vector<std::uint8_t> prefix_message(std::string_view message);

/**
 * @brief Send a length-prefixed message on a BLOCKING socket.
 *
 * Async writes use TcpConnection's send buffer instead; never retry
 * this function on a non-blocking socket after a partial send.
 * @param fd The socket descriptor.
 * @param message The payload to frame and send.
 * @return true if every byte was sent, false on error.
 */
bool sendFramed(int fd, std::string_view message);

} // namespace p2place::net
