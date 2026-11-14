#pragma once

#include "core/peer_table.hpp"
#include "core/consistent_hash_ring.hpp"
#include "core/balanced_placement.hpp"
#include "core/automatic_placement.hpp"
#include "messages/message.hpp"
#include "net/event_loop.hpp"
#include "net/tcp_server.hpp"
#include "peer/config.hpp"
#include "core/region_store.hpp"
#include "core/lamport_clock.hpp"
#include "ui/websocket_server.hpp"
#include "replication/replication_manager.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <set>
#include <deque>

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

    // Lab 04: membership gossip and deterministic ring refresh.
    [[nodiscard]] std::vector<core::NodeInfo> current_membership() const;
    void refresh_ring();
    void send_nodelist(int fd);
    void broadcast_nodelist();
    void broadcast_endpoints();
    void connect_discovered_peers();
    // Single-writer membership protocol. All methods execute on EventLoop thread.
    // Ring election: oldest admitted participating member wins, using a clockwise token.
    // This course-level protocol is NOT a substitute for consensus/fencing.
    void election_tick();
    void start_ring_election();
    void on_ring_election(const messages::Message&, core::NodeId sender);
    void on_ring_elected(const messages::Message&, core::NodeId sender);
    [[nodiscard]] std::optional<core::NodeId> next_ring_peer() const;
    [[nodiscard]] std::size_t reachable_voters() const;
    [[nodiscard]] bool has_majority() const;
    [[nodiscard]] core::NodeId preferred_initiator() const;
    void schedule_join(core::NodeId id);
    void schedule_leave(core::NodeId id);
    void begin_transition(core::NodeId id, bool joining);
    void progress_transition();
    void send_ready_if_quiescent();
    void next_handoff();
    void send_transition_message(messages::Type type, const std::vector<std::uint8_t>& payload = {});
    void abort_transition(const std::string& why);
    bool connected_to_all(const std::vector<core::NodeId>& ids) const;
    void set_committed(const core::BalancedPlacement& candidate);
    void send_control(core::NodeId id, messages::Type type, const std::vector<std::uint8_t>& payload = {});
    void on_placement_message(net::TcpConnection&, const messages::Message&, core::NodeId);
    void on_handoff_message(net::TcpConnection&, const messages::Message&, core::NodeId);
    void connect_peer(core::NodeId id, const std::string& host, std::uint16_t port);

    // Lab05: single quorum coordinator replaces old primary-only pending maps.
    struct Snapshot { std::uint32_t next_key=0; };
    std::map<int,Snapshot> snapshots_;
    std::unique_ptr<replication::ReplicationStrategy> replication_strategy_;
    std::unique_ptr<replication::ReplicationManager> replicator_;

    void start_snapshot(int browser_fd);
    void request_next_region(int browser_fd);
    void deliver_region(int browser_fd, std::uint32_t key,
                        const core::Region& region);
    void finish_snapshot(int browser_fd, const std::string& error = {});
    void broadcast_pixel(std::uint32_t key, std::uint16_t index,
                         std::uint32_t color);
    void report_browser_error(int browser_fd, const std::string& error);
    void handle_peer_disconnect(core::NodeId id);
    [[nodiscard]] std::optional<int> owner_fd(core::NodeId id) const;
    [[nodiscard]] std::uint32_t new_request_id();

    Config config_;
    core::PeerTable peers_;
    core::AutomaticPlacement placement_manager_;
    core::BalancedPlacement placement_;
    core::NodeId authority_id_ = 1;
    std::uint64_t election_round_ = 0;
    std::vector<std::uint8_t> last_elected_token_;
    bool election_in_progress_ = false;
    std::chrono::steady_clock::time_point election_deadline_{};
    std::chrono::steady_clock::time_point next_election_attempt_{};
    bool placement_ready_ = false;
    bool frozen_ = false;
    std::optional<core::BalancedPlacement> staged_;
    enum class Phase { Idle, Preparing, Fetching, Installing, Committing };
    Phase phase_ = Phase::Idle;
    struct MovedRegion { std::uint32_t key; core::NodeId old_owner; core::NodeId new_owner; };
    std::vector<MovedRegion> moves_;
    std::map<core::NodeId,std::set<std::uint32_t>> transition_keys_;
    std::size_t next_move_ = 0;
    std::set<core::NodeId> waiting_;
    std::set<core::NodeId> transition_peers_;
    bool ready_sent_ = false;
    std::deque<std::pair<core::NodeId, bool>> membership_queue_;
    std::map<core::NodeId, std::chrono::steady_clock::time_point> next_dial_;
    std::set<core::NodeId> connecting_;
    std::chrono::steady_clock::time_point transition_deadline_{};
    std::chrono::steady_clock::time_point next_endpoint_broadcast_{};
    core::ConsistentHashRing ring_;
    bool membership_dirty_ = false;
    std::chrono::steady_clock::time_point started_{};
    bool leave_requested_ = false;
    std::chrono::steady_clock::time_point next_gossip_{};
    net::EventLoop loop_;
    std::unique_ptr<net::TcpServer> server_;

  

    std::map<int, core::NodeId> node_by_fd_;
    std::map<core::NodeId, int> fd_by_node_;
    std::map<core::NodeId, std::chrono::steady_clock::time_point> last_pong_;
    std::map<messages::Type, Handler> handlers_;
    std::uint32_t next_request_id_ = 1;

    core::RegionStore region_store_;
    core::LamportClock lamport_clock_{
        core::LamportClock::restart_seed()
    };
    std::unique_ptr<ui::WebSocketServer> ws_server_;
    void on_browser_command(
        int fd,
        std::string_view message
    );

    void handle_put(
        int fd,
        std::uint16_t x,
        std::uint16_t y,
        std::uint32_t color
    );

    void handle_get_canvas(int fd);
};

} // namespace p2place::peer