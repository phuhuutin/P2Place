#include "peer/peer_node.hpp"

#include "net/protocol_codec.hpp"
#include "net/tcp_client.hpp"
#include "net/tcp_connection.hpp"
#include <nlohmann/json.hpp>
#include "core/versioned_pixel.hpp"
#include "core/membership_gossip.hpp"
#include "messages/payloads.hpp"
#include "messages/placement_protocol.hpp"
#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>
#include <charconv>
#include <algorithm>
#include <limits>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <cstring>
#include <filesystem>
#include <set>

using json = nlohmann::json;

namespace p2place::peer {
constexpr auto CANVAS_SIZE = core::RegionStore::CANVAS_SIDE;
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

PeerNode::PeerNode(Config config)
    : config_(std::move(config)), placement_manager_(config_.state_file) {
    // Node 1 bootstraps only the first cluster. Joiners receive committed
    // placement from the authority after coordinated data handoff.
    if (config_.id == 1 ||
        std::filesystem::exists(config_.state_file)) {
        placement_manager_.bootstrap(config_.id);
        set_committed(placement_manager_.active());
        // A restarted former leader must not immediately resume authority:
        // peers may have elected a replacement while it was offline.
        if (config_.id == 1 && placement_.members().size() > 1) {
            authority_id_ = 0; // unknown until election/reconciliation
            std::cout << "[election] recovered node 1; authority unknown, waiting for ring election\n";
        }
    }
    // A crashed follower with a pending/committed-but-not-released
    // transition must not resume serving its old placement on restart.
    const auto stage_file=config_.state_file+".pending";
    if(std::filesystem::exists(stage_file)) {
        auto stage=core::BalancedPlacement::load(stage_file);
        if(placement_ready_ && stage.epoch()<placement_.epoch())
            throw std::runtime_error("stale transition marker; manual recovery required");
        frozen_=true;
        if(!placement_ready_ || stage.epoch()>placement_.epoch())staged_=stage;
        std::cout << "[membership] restarting FROZEN with transition marker\n";
    }
    replication_strategy_=std::make_unique<replication::FixedReplicationStrategy>(
        replication::QuorumConfig{config_.replication_n,config_.read_r,config_.write_w});
    replicator_ = std::make_unique<replication::ReplicationManager>(
        config_.id,ring_,region_store_,*replication_strategy_,
        [this](core::NodeId peer,const messages::Message& m)->bool {
            const auto fd=owner_fd(peer);
            if(!fd)return false;
            send_message(*fd,m);
            return true;
        },
        [this](std::function<void()> cb) {
            return loop_.schedule_timer(net::TimerQueue::Clock::now()+std::chrono::seconds(3),std::move(cb));
        },
        [this](net::TimerQueue::Token token) {loop_.cancel_timer(token);}
    );
    register_handlers();
    // The local process participates in placement even when no peers are online.
    refresh_ring();
    next_gossip_ = std::chrono::steady_clock::now() + std::chrono::seconds(5);

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
    // Cluster placement identity; reject routing to peers with divergent maps.
    const auto fp = placement_ready_ ? placement_.fingerprint() : 0;
    put_u32(m.payload, static_cast<std::uint32_t>(fp >> 32));
    put_u32(m.payload, static_cast<std::uint32_t>(fp));
    // 1 = prepared but not committed, 2 = committed but not released.
    m.payload.push_back(staged_ ? 1 : (frozen_ ? 2 : 0));
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
        handle_peer_disconnect(id);
        if(config_.id==authority_id_ &&
           phase_!=Phase::Idle && transition_peers_.contains(id))
            abort_transition("participant disconnected");
        std::cout << "Peer " << id << " disconnected\n";
        membership_dirty_ = true;
        refresh_ring();
    }
}

std::vector<core::NodeInfo> PeerNode::current_membership() const {
    core::NodeInfo self;
    self.id = config_.id;
    self.peer_port = config_.port;
    self.ws_port = config_.ws_port;
    self.status = core::NodeStatus::Alive;
    return core::live_membership(peers_, self);
}

void PeerNode::refresh_ring() {
    // Canonical ownership never depends on HELLO/PING/PONG/NODELIST liveness.
    // Manifest changes require explicit offline handoff and restart (for now).
    if (!placement_ready_) return;
    if (ring_.all_nodes() == placement_.members()) return;
    ring_.rebuild(placement_);
    std::cout << "[placement] node=" << config_.id
              << " epoch=" << placement_.epoch()
              << " partitions=" << core::BalancedPlacement::PARTITIONS
              << " fingerprint=" << placement_.fingerprint()
              << " members=" << placement_.members().size() << '\n';
}

void PeerNode::send_nodelist(int fd) {
    messages::Message msg;
    msg.type = messages::Type::NODELIST;
    msg.payload = messages::encode_nodelist(
        messages::NodeListPayload{current_membership()}
    );
    send_message(fd, msg);
}

void PeerNode::broadcast_nodelist() {
    const auto connected = fd_by_node_;
    for (const auto& [id, fd] : connected) {
        (void)id;
        send_nodelist(fd);
    }
}

std::optional<int> PeerNode::owner_fd(core::NodeId id) const {
    if (const auto it = fd_by_node_.find(id); it != fd_by_node_.end())
        return it->second;
    return std::nullopt;
}

std::uint32_t PeerNode::new_request_id() {
    if(next_request_id_==0 || next_request_id_>=0x40000000)next_request_id_=1;
    return next_request_id_++;
}

void PeerNode::report_browser_error(int fd, const std::string& error) {
    try { ws_server_->send_to(fd, json{{"op","error"},{"message",error}}.dump()); }
    catch (const std::exception&) { /* browser already disconnected */ }
}

void PeerNode::broadcast_pixel(std::uint32_t key, std::uint16_t index,
                               std::uint32_t color) {
    const auto x = (key % core::RegionStore::REGIONS_SIDE) * core::Region::SIDE
                 + index % core::Region::SIDE;
    const auto y = (key / core::RegionStore::REGIONS_SIDE) * core::Region::SIDE
                 + index / core::Region::SIDE;
    ws_server_->broadcast(json{{"op","pixel"},{"x",x},{"y",y},
                               {"color",color_to_hex(color)}}.dump());
}

void PeerNode::handle_peer_disconnect(core::NodeId id) {
    if(replicator_)replicator_->on_disconnect(id);
}

void PeerNode::finish_snapshot(int browser_fd, const std::string& error) {
    if (!snapshots_.erase(browser_fd)) return;
    if (!error.empty()) report_browser_error(browser_fd, error);
    try { ws_server_->send_to(browser_fd,R"({"op":"canvas_end"})"); }
    catch (const std::exception&) { /* browser closed */ }
}

void PeerNode::deliver_region(int browser_fd, std::uint32_t key,
                              const core::Region& region) {
    constexpr std::size_t CHUNK_SIZE = 100;
    json pixels = json::array();
    for (std::size_t index=0; index<core::Region::COUNT; ++index) {
        const auto& px = region.pixel(index);
        if (px.lamport == 0) continue;
        const auto x = (key % core::RegionStore::REGIONS_SIDE) * core::Region::SIDE
                     + index % core::Region::SIDE;
        const auto y = (key / core::RegionStore::REGIONS_SIDE) * core::Region::SIDE
                     + index / core::Region::SIDE;
        pixels.push_back({{"x",x},{"y",y},{"color",color_to_hex(px.color)}});
        if (pixels.size() >= CHUNK_SIZE) {
            ws_server_->send_to(browser_fd,json{{"op","canvas_chunk"},{"pixels",pixels}}.dump());
            pixels = json::array();
        }
    }
    if (!pixels.empty())
        ws_server_->send_to(browser_fd,json{{"op","canvas_chunk"},{"pixels",pixels}}.dump());
}

void PeerNode::start_snapshot(int browser_fd) {
    if(snapshots_.contains(browser_fd)) {
        report_browser_error(browser_fd,"snapshot already in progress");
        return;
    }
    snapshots_[browser_fd]=Snapshot{};
    ws_server_->send_to(browser_fd,R"({"op":"canvas_begin"})");
    request_next_region(browser_fd);
}
void PeerNode::request_next_region(int browser_fd) {
    auto it=snapshots_.find(browser_fd);
    if(it==snapshots_.end())return;
    if(it->second.next_key>=core::RegionStore::REGION_COUNT) {
        finish_snapshot(browser_fd);return;
    }
    const auto key=it->second.next_key++;
    try {
        replicator_->get(key,[this,browser_fd,key](bool ok,const core::Region& region,const std::string& error){
            if(!snapshots_.contains(browser_fd))return;
            if(!ok){finish_snapshot(browser_fd,error);return;}
            if(region.version()!=0)lamport_clock_.update(region.version());
            try {deliver_region(browser_fd,key,region);}
            catch(const std::exception&){finish_snapshot(browser_fd,"browser snapshot delivery failed");return;}
            request_next_region(browser_fd);
        });
    } catch(const std::exception& e) {finish_snapshot(browser_fd,e.what());}
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
        if (sender != 0 || (msg.payload.size() != 16 && msg.payload.size()!=17))
            throw std::runtime_error("invalid or duplicate HELLO");

        const auto remote_stage=msg.payload.size()==17 ? msg.payload[16] : 0;
        const auto id = get_u32(msg.payload, 0);
        const auto port = get_u16(msg.payload, 4);
        const auto ws_port = get_u16(msg.payload, 6);
        const auto remote_fingerprint =
            (static_cast<std::uint64_t>(get_u32(msg.payload,8)) << 32) |
            get_u32(msg.payload,12);
        // Never accept a different committed ring silently. A fresh joiner
        // announces fingerprint 0; it must receive a prepared snapshot from
        // the authority before it can serve any pixel request.
        const auto members = placement_ready_ ? placement_.members() : std::vector<core::NodeId>{};
        const bool candidate = std::find(members.begin(),members.end(),id)==members.end()
            && remote_fingerprint == 0;
        const bool joining_self = !placement_ready_;
        if (!candidate && !joining_self &&
            (!placement_ready_ || remote_fingerprint != placement_.fingerprint()))
            throw std::runtime_error("committed placement mismatch; peer is stale");
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
        // A peer that dialed us provides its IP through the TCP socket.
        // HELLO only provides the peer's listening PORT.
        sockaddr_in address{};
        socklen_t length=sizeof(address);
        if (::getpeername(conn.fd(), reinterpret_cast<sockaddr*>(&address), &length)==0) {
            char ip[INET_ADDRSTRLEN]{};
            if (::inet_ntop(AF_INET, &address.sin_addr, ip, sizeof(ip)))
                info.host = ip;
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
        membership_dirty_ = true;
        refresh_ring();
        // HELLO precedes NODELIST in each connection's outgoing byte stream.
        send_nodelist(conn.fd());
        // Synchronize the last election result with a recovered former leader.
        // Without this, Node 1 might resume acting as authority after reconnect.
        if (!last_elected_token_.empty() && !candidate &&
            remote_fingerprint == placement_.fingerprint())
            send_control(id, messages::Type::RING_ELECTED, last_elected_token_);
        if (config_.id == authority_id_) {
            broadcast_endpoints();
            if (remote_stage==1 && phase_==Phase::Idle &&
                (remote_fingerprint==0 ||
                 (placement_ready_ && remote_fingerprint==placement_.fingerprint()))) {
                // Authority restarted before commit; previously prepared
                // participants can safely abort to the last committed view.
                send_control(id,messages::Type::PLACEMENT_ABORT);
            }
            if (candidate) schedule_join(id);
        }
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
        refresh_ring();
    };

    handlers_[messages::Type::PONG] = [this](
        net::TcpConnection&, const messages::Message& msg, core::NodeId sender) {
        if (!msg.payload.empty())
            throw std::runtime_error("unexpected PONG payload");
        last_pong_[sender] = std::chrono::steady_clock::now();
        peers_.mark(sender, core::NodeStatus::Alive);
        refresh_ring();
    };

    handlers_[messages::Type::NODELIST] = [this](
        net::TcpConnection&, const messages::Message& msg, core::NodeId sender) {
        if (sender == 0 || (msg.flags & messages::IS_RESPONSE)) {
            throw std::runtime_error("invalid NODELIST sender or flags");
        }
        const auto reported = messages::decode_nodelist(msg.payload);
        if (core::merge_gossip(peers_, config_.id, reported.peers)) {
            membership_dirty_ = true;
            refresh_ring();
        }
    };

    handlers_[messages::Type::RING_ELECTION] = [this](
        net::TcpConnection&, const messages::Message& msg, core::NodeId sender) {
        on_ring_election(msg, sender);
    };
    handlers_[messages::Type::RING_ELECTED] = [this](
        net::TcpConnection&, const messages::Message& msg, core::NodeId sender) {
        on_ring_elected(msg, sender);
    };

    // Requests are always sent by an authenticated neighboring peer.
    for (auto type : {messages::Type::PLACEMENT_PREPARE, messages::Type::PLACEMENT_READY,
                      messages::Type::PLACEMENT_COMMIT, messages::Type::PLACEMENT_COMMITTED,
                      messages::Type::PLACEMENT_RELEASE, messages::Type::JOIN_REQUEST,
                      messages::Type::LEAVE_REQUEST, messages::Type::PLACEMENT_ABORT,
                      messages::Type::PEER_ENDPOINTS}) {
        handlers_[type] = [this](net::TcpConnection& conn,
               const messages::Message& msg, core::NodeId sender) {
            on_placement_message(conn,msg,sender);
        };
    }
    for(auto type : {messages::Type::HANDOFF_GET, messages::Type::HANDOFF_DATA,
                     messages::Type::HANDOFF_PUT, messages::Type::HANDOFF_OK}) {
        handlers_[type] = [this](net::TcpConnection& conn,
               const messages::Message& msg, core::NodeId sender) {
            on_handoff_message(conn,msg,sender);
        };
    }

    for(auto type:{messages::Type::PUT,messages::Type::PUT_OK,
                   messages::Type::GET,messages::Type::GET_OK,
                   messages::Type::REGION_PUT,messages::Type::ERROR}) {
        handlers_[type]=[this](net::TcpConnection&,const messages::Message& msg,core::NodeId sender){
            if(frozen_ || !placement_ready_) {
                // Allow outstanding replies while draining a transition.
                if(!(msg.flags & messages::IS_RESPONSE)) {
                    messages::Message error;
                    error.type=messages::Type::ERROR;
                    error.flags=messages::IS_RESPONSE;
                    error.request_id=msg.request_id;
                    if(const auto fd=owner_fd(sender))send_message(*fd,error);
                    return;
                }
            }
            if(msg.type==messages::Type::PUT && !(msg.flags & messages::IS_RESPONSE)) {
                const auto payload=messages::decode_put(msg.payload);
                lamport_clock_.update(payload.pixel.lamport);
            }
            replicator_->on_message(sender,msg);
        };
    }

}
//this is called when a new connection is accepted. 
//It creates a TcpConnection object, registers it with the EventLoop, 
//and sends a HELLO message to the new peer.
void PeerNode::on_accepted(net::Socket client) {
    // Avoid 40ms delayed-ACK/Nagle stalls in the serial region handoff RPCs.
    int no_delay=1;
    ::setsockopt(client.get(),IPPROTO_TCP,TCP_NODELAY,&no_delay,sizeof(no_delay));
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
        if (seen != last_pong_.end() && now - seen->second > 3s &&
            peers_.find(id)->status != core::NodeStatus::Suspect) {
            peers_.mark(id, core::NodeStatus::Suspect);
            membership_dirty_ = true;
            refresh_ring();
        }

        messages::Message ping;
        ping.type = messages::Type::PING;
        ping.request_id = next_request_id_++;
        send_message(fd, ping);
    }

    // Send promptly following a membership change and at least every 5 seconds.
    // Do not reset the periodic deadline on a change-triggered broadcast.
    const bool periodic = now >= next_gossip_;
    if (membership_dirty_ || periodic) {
        broadcast_nodelist();
        if (config_.id == authority_id_)
            broadcast_endpoints();
        membership_dirty_ = false;
        if (periodic) {
            next_gossip_ = now + 5s;
        }
    }
    connect_discovered_peers();
    election_tick();
    if(config_.leave_after_seconds>0 && !leave_requested_ && placement_ready_ &&
       config_.id!=authority_id_ &&
       now-started_>=std::chrono::seconds(config_.leave_after_seconds) &&
       owner_fd(authority_id_)) {
        send_control(authority_id_,messages::Type::LEAVE_REQUEST);
        leave_requested_=true;
    }
    if (config_.id == authority_id_) progress_transition();
    else send_ready_if_quiescent();
}

void PeerNode::run() {
    started_=std::chrono::steady_clock::now();
    server_ = std::make_unique<net::TcpServer>(config_.port);
    // register this function to eventloop,
    // which will be called when Incoming connections on listener.
    loop_.register_listener(server_->fd(),

        [this] {
        // Accept all pending connections in tcp_server.cpp, 
        //then transfer the ownership back to the PeerNode.
        server_->accept_ready([this](net::Socket socket) {
            on_accepted(std::move(socket));
        });
    });
    // New Lab 03 browser listener.
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
            else if (op == "leave") {
                // Development-only administrative operation. Restrict access
                // to trusted local clients before deploying publicly.
                if (!placement_ready_) throw std::runtime_error("not a member");
                if (config_.id == authority_id_)
                    schedule_leave(config_.id);
                else send_control(authority_id_, messages::Type::LEAVE_REQUEST);
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


    void PeerNode::handle_put(int fd,std::uint16_t x,std::uint16_t y,std::uint32_t color) {
        if(frozen_ || !placement_ready_) {
            report_browser_error(fd,"placement transition or join in progress");return;
        }
        const auto key=core::RegionStore::region_key(x,y);
        const auto index=core::RegionStore::pixel_index(x,y);
        const core::VersionedPixel pixel{color,lamport_clock_.tick(),config_.id};
        try {
            replicator_->put(key,index,pixel,[this,fd,key,index,color](bool ok,const std::string& error){
                if(ok)broadcast_pixel(key,index,color);
                else report_browser_error(fd,error);
            });
        }catch(const std::exception& e){report_browser_error(fd,e.what());}
    }

    void PeerNode::handle_get_canvas(int fd) {
        if(frozen_ || !placement_ready_) {
            report_browser_error(fd,"placement transition or join in progress");
            return;
        }
        start_snapshot(fd);
    }


} // namespace p2place::peer


