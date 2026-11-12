/**
 * @file net/protocol_codec.hpp
 * @brief 2-byte length-prefixed framing for the peer TCP channel.
 */

#pragma once

#include "net/frame_codec.hpp"

/**
 * @brief Networking: epoll loop, sockets, connections, framing codecs.
 */
namespace p2place::net {

/** Maximum payload per frame on the peer channel (16 KiB). */
constexpr std::size_t MAX_MESSAGE_SIZE = 16384;

/**
 * @brief FrameCodec for the binary peer channel: 2-byte BE length
 *        prefix followed by the payload.
 */
class ProtocolCodec final : public FrameCodec {
public:
    /**
     * @brief Encode a payload and append its frame to @p out.
     *
     * Example: append_frame "ABC" -> 00 03 41 42 43, then
     * append_frame "XY" -> 00 02 58 59; out ends as
     * 00 03 41 42 43 | 00 02 58 59.
     * @param out Accumulator the encoded frame is appended to.
     * @param body The payload bytes to frame.
     * @return true if the frame was appended, false if the body is
     *         empty or exceeds MAX_MESSAGE_SIZE.
     */
    bool append_frame(
        std::vector<std::uint8_t>& out,
        std::span<const std::uint8_t> body
    ) const override;

    /**
     * @brief Extract one complete frame from @p in.
     *
     * Example: extract_frame 00 03 41 42 43 -> body "ABC"; the
     * consumed bytes are removed from in.
     * @param in Accumulated received bytes (mutated in place).
     * @param body Receives the payload of the extracted frame.
     * @return true if a complete frame was extracted, false for
     *         incomplete input.
     * @throws std::length_error if the announced length is 0 or
     *         exceeds MAX_MESSAGE_SIZE.
     */
    bool extract_frame(
        std::vector<std::uint8_t>& in,
        std::vector<std::uint8_t>& body
    ) const override;
};

} // namespace p2place::net
