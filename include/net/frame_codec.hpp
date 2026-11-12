/**
 * @file net/frame_codec.hpp
 * @brief Framing abstraction shared by the two wire channels.
 *
 * One TcpConnection implementation serves two channels by delegating
 * framing to a codec: ProtocolCodec (peer TCP, 2-byte length prefix)
 * and WebSocketCodec (browser, RFC 6455 frames).
 */

#pragma once

#include <cstdint>
#include <span>
#include <vector>

/**
 * @brief Networking: epoll loop, sockets, connections, framing codecs.
 */
namespace p2place::net {

/**
 * @brief Interface that turns payload bytes into wire frames and back.
 *
 * The codec owns all parser state (announced-but-incomplete lengths),
 * so TcpConnection stays framing-agnostic.
 */
class FrameCodec {
public:
    virtual ~FrameCodec() = default;

    /**
     * @brief Encode a payload and append its frame to @p output.
     * @param output Accumulator the encoded frame is appended to.
     * @param body The payload bytes to frame.
     * @return true on success, false if the body is not encodable
     *         (e.g. exceeds the channel's size cap).
     */
    virtual bool append_frame(
        std::vector<std::uint8_t>& output,
        std::span<const std::uint8_t> body
    ) const = 0;

    /**
     * @brief Try to extract one complete frame from the input buffer.
     *
     * On success the consumed bytes are removed from @p input and the
     * payload is stored in @p body.
     * @param input Accumulated received bytes (mutated in place).
     * @param body Receives the payload of the extracted frame.
     * @return true when a complete frame was extracted, false for
     *         incomplete input.
     * @throws std::length_error or std::runtime_error on malformed input.
     */
    virtual bool extract_frame(
        std::vector<std::uint8_t>& input,
        std::vector<std::uint8_t>& body
    ) const = 0;
};

} // namespace p2place::net
