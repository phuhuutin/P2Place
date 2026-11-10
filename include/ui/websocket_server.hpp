
#pragma once

#include "net/event_loop.hpp"
#include "net/tcp_server.hpp"
#include "net/websocket_codec.hpp"

#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace p2place::ui {

enum class WebSocketOpcode : std::uint8_t {
    Text  = 0x1,
    Close = 0x8,
    Ping  = 0x9,
    Pong  = 0xA
};

struct WebSocketFrame {
    WebSocketOpcode opcode;
    std::vector<std::uint8_t> payload;
};

enum class FrameResult {
    Incomplete,
    Complete,
    Invalid
};

class WebSocketServer final {

  enum class FrameResult {
    Incomplete,
    Complete,
    Invalid
  };
public:
    using CommandHandler =
        std::function<void(std::string_view)>;

    WebSocketServer(
        net::EventLoop& loop,
        std::uint16_t port,
        CommandHandler on_command
    );

    ~WebSocketServer();

    WebSocketServer(const WebSocketServer&) = delete;
    WebSocketServer& operator=(const WebSocketServer&) = delete;

    void accept_ready();

    // Broadcast JSON to every open browser connection.
    void broadcast(std::string_view json);

private:
    enum class ConnectionState {
        Handshaking,
        Open,
        Closing
    };

    struct BrowserSession {
        ConnectionState state =
            ConnectionState::Handshaking;

        // Used for HTTP handshake and WebSocket frames.
        std::vector<std::uint8_t> input_buffer;

        net::WebSocketCodec codec;
    };

    // Browser connection callbacks.
    void on_client_data(
        int fd,
        std::span<const std::uint8_t> data
    );

    void on_client_disconnect(int fd);

    void process_handshake(
        int fd,
        BrowserSession& session
    );

    void process_frames(
        int fd,
        BrowserSession& session
    );

    void send_text(int fd, std::string_view text);

    net::EventLoop& loop_;
    net::TcpServer listener_;
    CommandHandler on_command_;

    std::unordered_map<int, BrowserSession> sessions_;


    void send_control(
        int fd,
        WebSocketOpcode opcode,
        std::span<const std::uint8_t> payload
    );

    static FrameResult decode_frame(
        std::vector<std::uint8_t>& input,
        WebSocketFrame& frame
    );
};

} // namespace p2place::ui
