/**
 * @file net/tcp_connection.hpp
 * @brief One established connection: receive buffer, send buffer, codec.
 */

#pragma once

#include "net/frame_codec.hpp"
#include "net/socket.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <vector>

/**
 * @brief Networking: epoll loop, sockets, connections, framing codecs.
 */
namespace p2place::net {

/**
 * @brief A single established TCP connection with per-connection
 *        read/send buffers.
 *
 * Framing is delegated to a FrameCodec, so the same class serves both
 * the peer channel (ProtocolCodec) and the UI channel (WebSocketCodec).
 * The class is not thread-safe: it lives on the EventLoop thread.
 */
class TcpConnection final {
public:
    /**
     * @brief A function that handles incoming messages.
     * @param conn The TcpConnection that received the message.
     * @param message The received message as a span of bytes.
     */
    using MessageHandler = std::function<
        void(TcpConnection&, std::span<const std::uint8_t>)
    >;

    /**
     * @brief Construct a connection from an owned socket.
     * @param fd The connected socket (ownership transferred).
     * @param codec The framing codec for this channel.
     * @param handler Invoked once per complete decoded frame.
     * @throws std::invalid_argument if any argument is empty/invalid.
     */
    TcpConnection(
        Socket fd,
        std::unique_ptr<FrameCodec> codec,
        MessageHandler handler
    );

    /**
     * @brief Read available bytes and process complete frames.
     *
     * The receive buffer is capped at 64 KiB; exceeding it closes the
     * connection. Malformed frames also close it.
     * @return false if the connection should close.
     */
    bool drain_and_process();

    /**
     * @brief Attempt to send all currently queued bytes.
     *
     * Updates the epoll interest so EPOLLOUT is armed only while bytes
     * are pending.
     * @param epfd The event loop's epoll descriptor.
     * @return false if the connection should close.
     */
    bool try_flush(int epfd);

    /**
     * @brief Queue a framed message for transmission.
     * @param bytes The payload to frame and append to the send buffer.
     * @throws std::length_error if the frame or the send buffer cap
     *         would be exceeded.
     */
    void send_message_to_buffer(
        std::span<const std::uint8_t> bytes
    );

    /**
     * @brief Shut down both directions of the owned socket.
     */
    void shutdown() noexcept;

    /**
     * @brief The raw descriptor of the owned socket.
     * @return fd_.get().
     */
    int fd();

private:
    /** The owned socket. */
    Socket fd_;
    /** Channel framing (protocol or WebSocket). */
    std::unique_ptr<FrameCodec> codec_;
    /** Called once per complete decoded frame. */
    MessageHandler handler_;

    /** Received but not yet fully parsed bytes (64 KiB cap). */
    std::vector<std::uint8_t> read_buf_;
    /** Framed bytes waiting to go out. */
    std::vector<std::uint8_t> send_buf_;

    /** Number of leading send_buf_ bytes already transmitted. */
    std::size_t send_offset_ = 0;
    /** True while EPOLLOUT is armed in epoll. */
    bool wants_write_ = false;
};

} // namespace p2place::net
