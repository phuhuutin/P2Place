#pragma once

#include "core/peer_table.hpp"
#include "messages/message.hpp"
#include "net/event_loop.hpp"
#include "net/tcp_server.hpp"
#include "peer/config.hpp"
#include "ui/websocket_server.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>

namespace p2place::net { class TcpConnection; }

namespace p2place::peer {

class PeerNode final {
public:
    explicit PeerNode(Config config);
    void run();

private:
    using Handler = std::function<void(
        net::TcpConnection&, const messages::Message&, core::NodeId)>;

    void register_handlers();
    void on_tick();
    void on_accepted(net::Socket client);
    void on_message(net::TcpConnection& conn, std::span<const std::uint8_t> body);
    void send_message(int fd, const messages::Message& msg);
    void send_hello(int fd);
    void forget_connection(int fd);

    Config config_;
    core::PeerTable peers_;
    net::EventLoop loop_;
    std::unique_ptr<net::TcpServer> server_;

    // Non-owning connection bookkeeping: EventLoop owns TcpConnection objects.
    std::unique_ptr<ui::WebSocketServer> ws_server_;
    std::map<int, core::NodeId> node_by_fd_;
    std::map<core::NodeId, int> fd_by_node_;
    std::map<core::NodeId, std::chrono::steady_clock::time_point> last_pong_;
    std::map<messages::Type, Handler> handlers_;
    std::uint32_t next_request_id_ = 1;
};

} // namespace p2place::peer