/**
 * @file peer/peer_node.hpp
 * @brief PeerNode: the composition root that wires everything together.
 */

#pragma once

#include "core/peer_table.hpp"
#include "messages/message.hpp"
#include "net/event_loop.hpp"
#include "net/tcp_server.hpp"
#include "peer/config.hpp"
#include "ui/websocket_server.hpp"
#include "core/region_store.hpp"
#include "core/lamport_clock.hpp"
#include "ui/websocket_server.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>

namespace p2place::net { class TcpConnection; }

/**
 * @brief The peer application: PeerNode composition root and Config.
 */
namespace p2place::peer {

/**
 * @brief One P2Place peer process: mesh member + canvas server.
 *
 * The composition root. It owns the configuration, peer table, event
 * loop, region store, Lamport clock, the peer TCP listener and the
 * WebSocket UI server, and dispatches HELLO/PING/PONG plus browser
 * JSON commands.
 */
class PeerNode final {
public:
    /**
     * @brief Construct a peer and register its message handlers.
     * @param config Parsed command-line configuration.
     */
    explicit PeerNode(Config config);

    /**
     * @brief Start the peer TCP listener, the WebSocket listener,
     *        connect to the seed peers and run the event loop.
     */
    void run();

private:
    /**
     * @brief Signature of a message handler.
     * @param conn The connection the message arrived on.
     * @param msg The decoded message.
     * @param sender The sender's NodeId (0 before HELLO).
     */
    using Handler = std::function<void(
        net::TcpConnection&, const messages::Message&, core::NodeId)>;

    /**
     * @brief Install the HELLO/PING/PONG handlers.
     */
    void register_handlers();

    /**
     * @brief Periodic maintenance: PING all peers, mark SUSPECT.
     */
    void on_tick();

    /**
     * @brief Handle a newly accepted inbound TCP connection.
     * @param client The accepted socket (ownership transferred).
     */
    void on_accepted(net::Socket client);

    /**
     * @brief Decode one frame and dispatch it to the handler registry.
     * @param conn The connection the bytes arrived on.
     * @param body The received frame payload.
     */
    void on_message(net::TcpConnection& conn, std::span<const std::uint8_t> body);

    /**
     * @brief Encode and queue a message on a peer connection.
     * @param fd The destination connection.
     * @param msg The message to send.
     */
    void send_message(int fd, const messages::Message& msg);

    /**
     * @brief Send this peer's HELLO (id + ports) on a connection.
     * @param fd The destination connection.
     */
    void send_hello(int fd);

    /**
     * @brief Drop all bookkeeping for a destroyed connection.
     * @param fd The connection the loop just destroyed.
     */
    void forget_connection(int fd);

    /** This peer's configuration. */
    Config config_;
    /** Membership table of known peers. */
    core::PeerTable peers_;
    /** The single-threaded event loop. */
    net::EventLoop loop_;
    /** The peer TCP listener (created in run()). */
    std::unique_ptr<net::TcpServer> server_;

    /** fd -> NodeId for established peer connections. */
    std::map<int, core::NodeId> node_by_fd_;
    /** NodeId -> fd for established peer connections. */
    std::map<core::NodeId, int> fd_by_node_;
    /** NodeId -> time of the last PONG received. */
    std::map<core::NodeId, std::chrono::steady_clock::time_point> last_pong_;
    /** Message type -> handler (). */
    std::map<messages::Type, Handler> handlers_;
    /** Monotonic id source for PING request_ids. */
    std::uint32_t next_request_id_ = 1;

    /** The memory-only canvas. */
    core::RegionStore region_store_;
    /** Stamps every accepted paint. */
    core::LamportClock lamport_clock_{
        core::LamportClock::restart_seed()
    };
    /** The browser WebSocket server (created in run()). */
    std::unique_ptr<ui::WebSocketServer> ws_server_;

    /**
     * @brief Handle one JSON command from a browser tab.
     * @param fd The browser connection.
     * @param message The JSON text of the command.
     */
    void on_browser_command(
        int fd,
        std::string_view message
    );

    /**
     * @brief Apply one paint: LWW store update + pixel broadcast.
     * @param fd The browser connection (for error replies).
     * @param x Canvas x coordinate.
     * @param y Canvas y coordinate.
     * @param color 0xRRGGBB color.
     */
    void handle_put(
        int fd,
        std::uint16_t x,
        std::uint16_t y,
        std::uint32_t color
    );

    /**
     * @brief Send the full canvas snapshot as chunked JSON.
     * @param fd The requesting browser connection.
     */
    void handle_get_canvas(int fd);
};

} // namespace p2place::peer
