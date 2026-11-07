
#pragma once

#include "net/frame_codec.hpp"
#include "net/socket.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <vector>

namespace p2place::net {

class TcpConnection final {
public:
    /*
     * A function that handles incoming messages.
     * @param conn The TcpConnection that received the message.
     * @param message The received message as a span of bytes.
     */
    using MessageHandler = std::function<
        void(TcpConnection&, std::span<const std::uint8_t>)
    >;

    TcpConnection(
        Socket fd,
        std::unique_ptr<FrameCodec> codec,
        MessageHandler handler
    );

    // Read available bytes and process complete frames.
    // Return false if the connection should close.
    bool drain_and_process();

    // Attempt to send all currently queued bytes.
    bool try_flush(int epfd);

    // Queue a framed message for transmission.
    void send_message_to_buffer(
        std::span<const std::uint8_t> bytes
    );

    // Close the owned socket.
    void shutdown() noexcept;

    //fd getter
    int fd();

private:
    Socket fd_;
    std::unique_ptr<FrameCodec> codec_;
    MessageHandler handler_;

    std::vector<std::uint8_t> read_buf_;
    std::vector<std::uint8_t> send_buf_;

    std::size_t send_offset_ = 0;
    bool wants_write_ = false;
};

} // namespace p2place::net
