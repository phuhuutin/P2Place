/**
 * @file ui/websocket_server.hpp
 * @brief Browser-facing WebSocket server: handshake, frames, JSON ops.
 */

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

/**
 * @brief Browser UI channel: the WebSocket server.
 */
namespace p2place::ui {

/**
 * @brief WebSocket frame opcodes recognized by the server.
 */
enum class WebSocketOpcode : std::uint8_t {
    /** Text frame carrying a JSON command (opcode 0x1). */
    Text  = 0x1,
    /** Connection close (opcode 0x8). */
    Close = 0x8,
    /** Keep-alive ping (opcode 0x9). */
    Ping  = 0x9,
    /** Keep-alive pong (opcode 0xA). */
    Pong  = 0xA
};

/**
 * @brief One decoded browser frame.
 */
struct WebSocketFrame {
    /** The frame's opcode. */
    WebSocketOpcode opcode;
    /** Unmasked payload bytes. */
    std::vector<std::uint8_t> payload;
};

/**
 * @brief Result of one frame-decoding attempt.
 */
enum class FrameResult {
    /** Not enough bytes yet; wait for more input. */
    Incomplete,
    /** One complete frame was extracted. */
    Complete,
    /** Protocol violation; close the connection. */
    Invalid
};

/**
 * @brief One browser-facing listener: HTTP Upgrade + WebSocket frames.
 *
 * Serves the painting UI and routes JSON commands (put, get_canvas)
 * to the PeerNode-provided handler; broadcasts pixel updates to every
 * open tab.
 */
class WebSocketServer final {

  /**
   * @brief Result of decoding one frame (private copy used by the
   *        static decoder).
   */
  enum class FrameResult {
    /** Not enough bytes yet. */
    Incomplete,
    /** One complete frame was extracted. */
    Complete,
    /** Protocol violation. */
    Invalid
  };

public:
    /**
     * @brief Handler for one decoded JSON command from a browser.
     * @param fd The browser connection.
     * @param json The JSON text of the command.
     */
    using CommandHandler =
        std::function<void(int fd, std::string_view json)>;

    /**
     * @brief Bind the WebSocket listener and register it with the
     *        event loop.
     * @param loop The event loop that will drive this server.
     * @param port TCP port for the browser listener.
     * @param on_command Handler for decoded JSON commands.
     */
    WebSocketServer(
        net::EventLoop& loop,
        std::uint16_t port,
        CommandHandler on_command
    );

    /**
     * @brief Unregister the listener from the event loop.
     */
    ~WebSocketServer();

    WebSocketServer(const WebSocketServer&) = delete;
    WebSocketServer& operator=(const WebSocketServer&) = delete;

    /**
     * @brief Accept all pending browser connections.
     */
    void accept_ready();

    /**
     * @brief Broadcast JSON to every open browser connection.
     * @param json The JSON text to send.
     */
    void broadcast(std::string_view json);

    /**
     * @brief Send JSON to one browser connection.
     * @param fd The destination browser connection.
     * @param json The JSON text to send.
     */
    void send_to(int fd, std::string_view json);

private:
    /**
     * @brief Lifecycle of one browser connection.
     */
    enum class ConnectionState {
        /** Reading the HTTP Upgrade request. */
        Handshaking,
        /** WebSocket established; JSON frames flow. */
        Open,
        /** Close frame sent; waiting for the flush. */
        Closing
    };

    /**
     * @brief Per-browser state.
     */
    struct BrowserSession {
        /** Current lifecycle state. */
        ConnectionState state =
            ConnectionState::Handshaking;

        /** Used for HTTP handshake and WebSocket frames. */
        std::vector<std::uint8_t> input_buffer;

        /** Server-side frame encoder. */
        net::WebSocketCodec codec;
    };

    // Browser connection callbacks.

    /**
     * @brief EPOLLIN callback: buffer, handshake, then decode frames.
     * @param fd The browser connection.
     * @param data The bytes just received.
     */
    void on_client_data(
        int fd,
        std::span<const std::uint8_t> data
    );

    /**
     * @brief Disconnect callback: forget the session.
     * @param fd The browser connection.
     */
    void on_client_disconnect(int fd);

    /**
     * @brief Complete the HTTP Upgrade handshake.
     * @param fd The browser connection.
     * @param session The session holding the buffered request.
     */
    void process_handshake(
        int fd,
        BrowserSession& session
    );

    /**
     * @brief Decode and dispatch buffered WebSocket frames.
     * @param fd The browser connection.
     * @param session The session holding the buffered bytes.
     */
    void process_frames(
        int fd,
        BrowserSession& session
    );

    /**
     * @brief Encode and queue one server text frame.
     * @param fd The browser connection.
     * @param text The JSON text to send.
     */
    void send_text(int fd, std::string_view text);

    /** The owning event loop. */
    net::EventLoop& loop_;
    /** The bound TCP listener. */
    net::TcpServer listener_;
    /** PeerNode's JSON command handler. */
    CommandHandler on_command_;

    /** fd -> per-browser session state. */
    std::unordered_map<int, BrowserSession> sessions_;

    /**
     * @brief Encode and queue one unmasked control frame.
     * @param fd The browser connection.
     * @param opcode The control opcode (Close/Ping/Pong).
     * @param payload Control payload (max 125 bytes).
     */
    void send_control(
        int fd,
        WebSocketOpcode opcode,
        std::span<const std::uint8_t> payload
    );

    /**
     * @brief Decode one browser frame from the input buffer.
     * @param input Accumulated bytes (mutated in place).
     * @param frame Receives the decoded frame.
     * @return Complete, Incomplete or Invalid.
     */
    static FrameResult decode_frame(
        std::vector<std::uint8_t>& input,
        WebSocketFrame& frame
    );
};

} // namespace p2place::ui
