/**
 * @file net/websocket_codec.hpp
 * @brief Minimal RFC 6455 server-side framing for the browser channel.
 */
#pragma once
#include "net/frame_codec.hpp"
#include <string>
#include <string_view>

/**
 * @brief Networking: epoll loop, sockets, connections, framing codecs.
 */
namespace p2place::net {

/**
 * @brief FrameCodec for the browser UI channel: RFC 6455 subset.
 *
 * Server-to-browser frames are unmasked text frames (opcode 0x1);
 * browser-to-server frames must be masked. No fragmentation, no
 * extensions, no TLS — out of scope by design.
 */
class WebSocketCodec final : public FrameCodec {
public:
    /**
     * @brief Build the HTTP 101 response for a valid Upgrade request.
     *
     * Validates the request line, the Upgrade/Connection/Version/Host/
     * Sec-WebSocket-Key headers and the Base64 key format, then returns
     * "HTTP/1.1 101 Switching Protocols" with the computed
     * Sec-WebSocket-Accept.
     * @param http_request The raw HTTP Upgrade request.
     * @return The full 101 response, or an empty string if the request
     *         is not a valid WebSocket handshake.
     */
    static std::string handshake_response(std::string_view http_request);

    /**
     * @brief Encode server text as an unmasked WebSocket text frame
     *        (the browser receives pixel or update messages).
     * @param out Accumulator the encoded frame is appended to.
     * @param text The text payload to frame.
     * @return true if the frame was appended, false if the payload
     *         exceeds the size caps.
     */
    bool append_frame(std::vector<std::uint8_t>& out,std::span<const std::uint8_t> text) const override;

    /**
     * @brief Extract one masked browser-to-server frame (the browser
     *        sends pixels or snapshot requests).
     *
     * Unmasks the payload with the frame's 4-byte XOR key and removes
     * the consumed bytes from @p input.
     * @param input Accumulated received bytes (mutated in place).
     * @param text Receives the unmasked text payload.
     * @return true if a complete frame was extracted, false for
     *         incomplete input.
     * @throws std::runtime_error on protocol violations (fragmented
     *         frame, non-text opcode, unmasked client frame, oversized
     *         or non-minimal length).
     */
    bool extract_frame(std::vector<std::uint8_t>& input,std::vector<std::uint8_t>& text) const override;
};

} // namespace p2place::net
