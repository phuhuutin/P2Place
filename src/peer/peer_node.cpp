#include "peer/peer_node.hpp"

#include "net/protocol_codec.hpp"
#include "net/tcp_client.hpp"
#include "net/tcp_connection.hpp"

#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

namespace p2place::peer {
namespace {
    using namespace std::chrono_literals;
    //16bits to  8 bits each. big-endian: big | small
    void put_u16(std::vector<std::uint8_t>& b, std::uint16_t v) {
        b.push_back(static_cast<std::uint8_t>(v >> 8));
        b.push_back(static_cast<std::uint8_t>(v));
    }
    void put_u32(std::vector<std::uint8_t>& b, std::uint32_t v) {
        b.push_back(static_cast<std::uint8_t>(v >> 24));
        b.push_back(static_cast<std::uint8_t>(v >> 16));
        b.push_back(static_cast<std::uint8_t>(v >> 8));
        b.push_back(static_cast<std::uint8_t>(v));
    }
std::uint16_t get_u16(const std::vector<std::uint8_t>& b, std::size_t off) {
        return (std::uint16_t(b[off]) << 8) | b[off + 1];
    }
    std::uint32_t get_u32(const std::vector<std::uint8_t>& b, std::size_t off) {
        return (std::uint32_t(b[off]) << 24) |
            (std::uint32_t(b[off + 1]) << 16) |
            (std::uint32_t(b[off + 2]) << 8) | b[off + 3];
    }
} // namespace

PeerNode::PeerNode(Config config) : config_(std::move(config)) {
    register_handlers();

    // Keep runtime tables in sync when EventLoop destroys a
    // connection (EOF, RST, protocol error, failed flush, ...).
    // Without this, on_tick() would keep PINGing dead fds and the
    // first "connection not found" send would kill the run loop.
    loop_.set_on_disconnect([this](int fd) {
        forget_connection(fd);
    });
}

void PeerNode::send_message(int fd, const messages::Message& msg) {
    // REQUIRED small EventLoop addition, shown in INTEGRATION.md.
    loop_.send(fd, messages::encode(msg));
}

void PeerNode::send_hello(int fd) {
    messages::Message m;
    m.type = messages::Type::HELLO;
    put_u32(m.payload, config_.id);
    put_u16(m.payload, config_.port);
    put_u16(m.payload, config_.ws_port);
    send_message(fd, m);
}

void PeerNode::forget_connection(int fd) {
    const auto it = node_by_fd_.find(fd);
    if (it == node_by_fd_.end()) return;

    const auto id = it->second;
    node_by_fd_.erase(it);
    if (auto f = fd_by_node_.find(id); f != fd_by_node_.end() && f->second == fd) {
        fd_by_node_.erase(f);
        peers_.mark(id, core::NodeStatus::Dead);
        last_pong_.erase(id);
        std::cout << "Peer " << id << " disconnected\n";
    }
}

void PeerNode::on_message(net::TcpConnection& conn,
                          std::span<const std::uint8_t> body) {
    const auto msg = messages::decode(body);
    const auto it = node_by_fd_.find(conn.fd());
    const core::NodeId sender = (it == node_by_fd_.end()) ? 0 : it->second;

    // First message on a new connection must identify the peer.
    if (sender == 0 && msg.type != messages::Type::HELLO)
        throw std::runtime_error("expected HELLO before any other message");

    if (auto handler = handlers_.find(msg.type); handler != handlers_.end()) {
        handler->second(conn, msg, sender);
    }
}
//summary:registers callback functions
// for different message types so PeerNode knows what to do when a message arrives.
void PeerNode::register_handlers() {
    handlers_[messages::Type::HELLO] = [this](
        net::TcpConnection& conn, const messages::Message& msg, core::NodeId sender) {
        if (sender != 0 || msg.payload.size() != 8)
            throw std::runtime_error("invalid or duplicate HELLO");

        const auto id = get_u32(msg.payload, 0);
        const auto port = get_u16(msg.payload, 4);
        const auto ws_port = get_u16(msg.payload, 6);
        if (id == 0 || id == config_.id || port == 0)
            throw std::runtime_error("invalid peer identity");

        // Use the configured seed host when available. The HELLO payload
        // intentionally carries ID/ports but no host string.
        core::NodeInfo info;
        info.id = id;
        info.peer_port = port;
        info.ws_port = ws_port;
        info.status = core::NodeStatus::Alive;
        for (const auto& seed : config_.peers) {
            if (seed.id == id) { info.host = seed.host; break; }
        }

        // This minimal Lab 02 version keeps one active connection per NodeId.
        if (const auto old = fd_by_node_.find(id);
            old != fd_by_node_.end() && old->second != conn.fd()) {
            throw std::runtime_error("duplicate peer connection");
        }
        peers_.upsert(std::move(info));
        node_by_fd_[conn.fd()] = id;
        fd_by_node_[id] = conn.fd();
        last_pong_[id] = std::chrono::steady_clock::now();
        std::cout << "HELLO from peer " << id << " (fd=" << conn.fd() << ")\n";
    };

    handlers_[messages::Type::PING] = [this](
        net::TcpConnection& conn, const messages::Message& msg, core::NodeId sender) {
        if (!msg.payload.empty())
            throw std::runtime_error("unexpected PING payload");
        messages::Message pong;
        pong.type = messages::Type::PONG;
        pong.flags = messages::IS_RESPONSE;
        pong.request_id = msg.request_id;
        send_message(conn.fd(), pong);
        peers_.mark(sender, core::NodeStatus::Alive);
    };

    handlers_[messages::Type::PONG] = [this](
        net::TcpConnection&, const messages::Message& msg, core::NodeId sender) {
        if (!msg.payload.empty())
            throw std::runtime_error("unexpected PONG payload");
        last_pong_[sender] = std::chrono::steady_clock::now();
        peers_.mark(sender, core::NodeStatus::Alive);
    };
}
//this is called when a new connection is accepted. 
//It creates a TcpConnection object, registers it with the EventLoop, 
//and sends a HELLO message to the new peer.
void PeerNode::on_accepted(net::Socket client) {
    auto conn = std::make_unique<net::TcpConnection>(
        std::move(client),
        std::make_unique<net::ProtocolCodec>(),
        [this](net::TcpConnection& c, std::span<const std::uint8_t> bytes) {
            on_message(c, bytes);
        }
    );
    const int fd = conn->fd();
    loop_.add(std::move(conn));
    send_hello(fd);
}

void PeerNode::on_tick() {
    const auto now = std::chrono::steady_clock::now();
    // Iterate a snapshot in case callbacks or disconnect handling change membership.
    const auto connected = fd_by_node_;
    for (const auto& [id, fd] : connected) {
        auto seen = last_pong_.find(id);
        if (seen != last_pong_.end() && now - seen->second > 3s)
            peers_.mark(id, core::NodeStatus::Suspect);

        messages::Message ping;
        ping.type = messages::Type::PING;
        ping.request_id = next_request_id_++;
        send_message(fd, ping);
    }
}

void PeerNode::run() {
    server_ = std::make_unique<net::TcpServer>(config_.port);
    // register this function to eventloop,
    // which will be called when Incoming connections on listener.
    loop_.register_listener(server_->fd(), [this] {

        // Accept all pending connections in tcp_server.cpp, 
        //then transfer the ownership back to the PeerNode.
        server_->accept_ready([this](net::Socket socket) {
            on_accepted(std::move(socket));
        });
    });

         // 3. Connect to configured peers
    for (const auto& peer : config_.peers) {
        if (peer.id == config_.id)
            continue;

        try {
            auto result = net::TcpClient::connect_to(
                peer.host,
                peer.peer_port
            );

            if (result.status == net::ConnectStatus::Connected) {
                // TCP connection established immediately
                on_accepted(std::move(result.socket));
            }
            else {
                // EINPROGRESS: let EventLoop finish connecting
                loop_.add_pending_connection(
                    std::move(result.socket),

                    // Success callback
                    [this](net::Socket connected) {
                        on_accepted(std::move(connected));
                    },

                    // Failure callback
                    [id = peer.id](int error) {
                        std::cerr
                            << "Connection to peer " << id
                            << " failed: "
                            << std::system_category().message(error)
                            << '\n';
                    }
                );
            }
        }
        catch (const std::exception& e) {
            std::cerr
                << "Failed to connect to peer "
                << peer.id << ": "
                << e.what() << '\n';
        }
    }

    std::cout << "Peer " << config_.id << " listening on port "
              << config_.port << '\n';
    loop_.run([this] { on_tick(); });
}

} // namespace p2place::peer
