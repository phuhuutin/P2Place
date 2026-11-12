/**
 * @file peer/peer_node.cpp
 * @brief Implementation of the PeerNode composition root.
 */
#include "peer/peer_node.hpp"

#include "net/protocol_codec.hpp"
#include "net/tcp_client.hpp"
#include "net/tcp_connection.hpp"
#include <nlohmann/json.hpp>
#include "core/versioned_pixel.hpp"
#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>
#include <charconv>

using json = nlohmann::json;

namespace p2place::peer {
constexpr auto CANVAS_SIZE = core::RegionStore::CANVAS_SIDE;
namespace {
    using namespace std::chrono_literals;

    /**
     * @brief Append a 16-bit value in big-endian byte order.
     * @param b Output byte vector.
     * @param v The value to append.
     */
    void put_u16(std::vector<std::uint8_t>& b, std::uint16_t v) {
        b.push_back(static_cast<std::uint8_t>(v >> 8));
        b.push_back(static_cast<std::uint8_t>(v));
    }
    /**
     * @brief Append a 32-bit value in big-endian byte order.
     * @param b Output byte vector.
     * @param v The value to append.
     */
    void put_u32(std::vector<std::uint8_t>& b, std::uint32_t v) {
        b.push_back(static_cast<std::uint8_t>(v >> 24));
        b.push_back(static_cast<std::uint8_t>(v >> 16));
        b.push_back(static_cast<std::uint8_t>(v >> 8));
        b.push_back(static_cast<std::uint8_t>(v));
    }
    /**
     * @brief Read a 16-bit big-endian value from a byte vector.
     * @param b The byte vector.
     * @param off Offset of the value's first byte.
     * @return The decoded value.
     */
    std::uint16_t get_u16(const std::vector<std::uint8_t>& b, std::size_t off) {
        return (std::uint16_t(b[off]) << 8) | b[off + 1];
    }
    /**
     * @brief Read a 32-bit big-endian value from a byte vector.
     * @param b The byte vector.
     * @param off Offset of the value's first byte.
     * @return The decoded value.
     */
    std::uint32_t get_u32(const std::vector<std::uint8_t>& b, std::size_t off) {
        return (std::uint32_t(b[off]) << 24) |
            (std::uint32_t(b[off + 1]) << 16) |
            (std::uint32_t(b[off + 2]) << 8) | b[off + 3];
    }

    /**
     * @brief Parse a "#RRGGBB" color string into 0xRRGGBB.
     * @param color The color text.
     * @return The 24-bit color value.
     * @throws std::invalid_argument if the format is invalid.
     */
    std::uint32_t parse_color(std::string_view color) {
        if (color.size() != 7 || color[0] != '#') {
            throw std::invalid_argument(
                "Expected #RRGGBB color"
            );
        }

        std::uint32_t value = 0;

        const auto* first = color.data() + 1;
        const auto* last = color.data() + color.size();

        auto [ptr, ec] = std::from_chars(
            first, last, value, 16
        );

        if (ec != std::errc{} || ptr != last) {
            throw std::invalid_argument(
                "Invalid hexadecimal color"
            );
        }

        return value;
    }
    
    /**
     * @brief Format a 24-bit color as "#RRGGBB" text.
     * @param color The color value (upper 8 bits ignored).
     * @return The hex color string.
     */
    std::string color_to_hex(std::uint32_t color) {
        constexpr char digits[] = "0123456789ABCDEF";

        std::string result = "#000000";

        for (int i = 6; i >= 1; --i) {
            result[i] = digits[color & 0xF];
            color >>= 4;
        }

        return result;
    }
} // anonymous namespace 

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
/**
 * @brief Registers callback functions for different message types so
 *        PeerNode knows what to do when a message arrives.
 */
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
/**
 * @brief Called when a new connection is accepted: creates a
 *        TcpConnection object, registers it with the EventLoop, and
 *        sends a HELLO message to the new peer.
 */
void PeerNode::on_accepted(net::Socket client) {
    auto conn = std::make_unique<net::TcpConnection>(
        std::move(client),
        std::make_unique<net::ProtocolCodec>(),
        //this callback is called inside drain_and_process()
        //drain and extract one frame callback(this, bytes )
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
    loop_.register_listener(server_->fd(),

        [this] {
        // Accept all pending connections in tcp_server.cpp, 
        //then transfer the ownership back to the PeerNode.
        //this callback will be called when the listener is readable.
        //a new connection is accepted
        // accept and create a new client socket, then transfer ownership to PeerNode.
        //via on_accepted
        server_->accept_ready([this](net::Socket socket) {
            on_accepted(std::move(socket));
        });
    });
    // New Lab 03 browser listener.
    // inside ws_port will be registered to eventloop,
    // which will be called when Incoming connections on listener.
    // WebSocketServer 
    ws_server_ = std::make_unique<ui::WebSocketServer>(
        loop_,
        config_.ws_port,
        [this](int fd, std::string_view json) {
            on_browser_command(fd, json);
        }
    );


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


void PeerNode::on_browser_command(
    int fd,
    std::string_view message
) {
    try {
        const auto command = json::parse(message);

        if (!command.is_object()) {
            throw std::invalid_argument(
                "Command must be a JSON object"
            );
        }

        const std::string op =
            command.at("op").get<std::string>();

        if (op == "put") {
            // Parse into signed/wider integers first
            // so out-of-range values cannot wrap.
            const auto x =
                command.at("x").get<std::int64_t>();

            const auto y =
                command.at("y").get<std::int64_t>();

            const auto color_string =
                command.at("color").get<std::string>();

            if (x < 0 || x >= CANVAS_SIZE  ||
                y < 0 || y >= CANVAS_SIZE ) {
                throw std::out_of_range(
                    "Pixel coordinates out of range"
                );
            }

            const auto color =
                parse_color(color_string);

            handle_put(
                fd,
                static_cast<std::uint16_t>(x),
                static_cast<std::uint16_t>(y),
                color
            );
        }
        else if (op == "get_canvas") {
            handle_get_canvas(fd);
        }
        else {
            ws_server_->send_to(
                fd,
                json{
                    {"op", "error"},
                    {"message", "Unsupported operation"}
                }.dump()
            );
        }
    }
    catch (const std::exception& e) {
        ws_server_->send_to(
            fd,
            json{
                {"op", "error"},
                {"message", e.what()}
            }.dump()
        );
    }
}


    void PeerNode::handle_put(
        int fd,
        std::uint16_t x,
        std::uint16_t y,
        std::uint32_t color
    ) {
        // 1. Find the region containing this pixel.
        const auto region_key =
            core::RegionStore::region_key(x, y);

        // 2. Find its index inside the 16×16 region.
        const auto pixel_index =
            core::RegionStore::pixel_index(x, y);

        // 3. Generate the new pixel version.
        core::VersionedPixel pixel{
            color,
            lamport_clock_.tick(),
            config_.id
        };

        // 4. Apply using Last-Write-Wins.
        const bool accepted =
            region_store_.apply_pixel(
                region_key,
                pixel_index,
                pixel
            );

        if (!accepted) {
            ws_server_->send_to(
                fd,
                json{
                    {"op", "error"},
                    {"message", "Pixel update rejected"}
                }.dump()
            );

            return;
        }

        // 5. Create the browser update message.
        const json update = {
            {"op", "pixel"},
            {"x", x},
            {"y", y},
            {"color", color_to_hex(color)}
        };

        // 6. Broadcast to every connected browser.
        ws_server_->broadcast(update.dump());
    }


    void PeerNode::handle_get_canvas(int fd) {
        constexpr std::uint32_t REGIONS_PER_ROW =
            core::RegionStore::REGIONS_SIDE;

        constexpr std::size_t CHUNK_SIZE = 100;

        // Tell browser to reset the canvas.
        ws_server_->send_to(
            fd,
            R"({"op":"canvas_begin"})"
        );

        json pixels = json::array();

        // Enumerate regions currently stored locally.
        for (const auto region_key :
            region_store_.all_keys()) {

            const auto region =
                region_store_.get_region(region_key);

            if (!region) {
                continue;
            }

            const std::uint32_t rx =
                region_key % REGIONS_PER_ROW;

            const std::uint32_t ry =
                region_key / REGIONS_PER_ROW;

            for (std::size_t i = 0;
                i < core::Region::COUNT;
                ++i) {

                const auto& pixel = region->pixel(i);

                // Lamport 0 means no write has occurred.
                if (pixel.lamport == 0) {
                    continue;
                }

                const auto x =
                    rx * core::Region::SIDE +
                    i % core::Region::SIDE;

                const auto y =
                    ry * core::Region::SIDE +
                    i / core::Region::SIDE;

                pixels.push_back({
                    {"x", x},
                    {"y", y},
                    {"color", color_to_hex(pixel.color)}
                });

                // Send one bounded chunk.
                if (pixels.size() >= CHUNK_SIZE) {
                    ws_server_->send_to(
                        fd,
                        json{
                            {"op", "canvas_chunk"},
                            {"pixels", pixels}
                        }.dump()
                    );

                    pixels = json::array();
                }
            }
        }

        // Send any remaining pixels.
        if (!pixels.empty()) {
            ws_server_->send_to(
                fd,
                json{
                    {"op", "canvas_chunk"},
                    {"pixels", pixels}
                }.dump()
            );
        }

        // Snapshot complete.
        ws_server_->send_to(
            fd,
            R"({"op":"canvas_end"})"
        );
    }




} // namespace p2place::peer


