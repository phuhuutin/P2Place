#pragma once

#include "core/peer_table.hpp"
#include "core/failure_detector.hpp"
#include "core/consistent_hash_ring.hpp"
#include "core/balanced_placement.hpp"
#include "core/automatic_placement.hpp"
#include "core/peer_reliability.hpp"
#include "core/replication_policy.hpp"
#include "messages/message.hpp"
#include "net/event_loop.hpp"
#include "net/tcp_server.hpp"
#include "peer/config.hpp"
#include "peer/metrics.hpp"
#include "peer/pixel_event.hpp"
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
    void schedule_leave(core::NodeId id, const std::string& reason = {});
    void schedule_auto_removal_tick(std::chrono::steady_clock::time_point now);
    void reset_to_joiner(const std::string& why);
    void log_peer_health(core::NodeId id, core::NodeStatus status);
    // Authority-message gate: members require sender==authority_id_; a
    // joiner without a committed placement adopts the sender (no election
    // token exists when the authority never failed).
    bool accept_authority_message(core::NodeId sender);
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
    void forward_join_referrals();

    // Lab05: single quorum coordinator replaces old primary-only pending maps.
    struct Snapshot { std::uint32_t next_key=0; };
    std::map<int,Snapshot> snapshots_;

    // Strategy B state. policy_table_ must be constructed BEFORE
    // replication_strategy_ (the churn strategy holds a reference to it).
    core::PolicyTable policy_table_{3};
    core::PeerReliabilityTracker reliability_tracker_{std::chrono::seconds(120)};
    std::unique_ptr<core::PolicyEvaluator> policy_evaluator_;
    std::chrono::steady_clock::time_point next_policy_eval_{};
    std::chrono::steady_clock::time_point process_started_{};
    std::unique_ptr<Metrics> metrics_;

    std::unique_ptr<replication::ReplicationStrategy> replication_strategy_;
    std::unique_ptr<replication::ReplicationManager> replicator_;

    void evaluate_and_broadcast_policy(std::chrono::steady_clock::time_point now,
                                       bool placement_changed);
    // After a real policy/placement change: re-queue every owned region that
    // lacks data (absent or empty) so backfill re-pulls from the max-N
    // preference superset, which may include former holders stranded by a
    // shrink (N4->N3->N2).
    void reconsider_owned_backfills(const core::PolicySnapshot& snap);

    void start_snapshot(int browser_fd);
    void request_next_region(int browser_fd);
    void deliver_region(int browser_fd, std::uint32_t key,
                        const core::Region& region);
    void finish_snapshot(int browser_fd, const std::string& error = {});
    void broadcast_pixel(std::uint32_t key, std::uint16_t index,
                         std::uint32_t color);
    void announce_committed_pixel(std::uint32_t key, std::uint16_t index,
                                 const core::VersionedPixel& pixel);
    void on_pixel_event(const messages::Message& msg,core::NodeId sender);
    void relay_pixel_event(const messages::Message& msg,core::NodeId except);
    void report_browser_error(int browser_fd, const std::string& error);
    void handle_peer_disconnect(core::NodeId id);
    [[nodiscard]] std::optional<int> owner_fd(core::NodeId id) const;
    [[nodiscard]] std::uint32_t new_request_id();

    Config config_;
    core::PeerTable peers_;
    core::FailureDetector failure_detector_{{}};
    std::set<core::NodeId> reported_unavailable_;
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
    // Membership queue: one change at a time. AutoRemove entries are
    // re-validated at dequeue (a peer may recover while queued).
    struct MembershipOp {
        core::NodeId id = 0;
        enum class Kind : std::uint8_t { Join, Leave, AutoRemove } kind = Kind::Join;
        bool operator==(const MembershipOp& o) const { return id==o.id && kind==o.kind; }
    };
    std::deque<MembershipOp> membership_queue_;
    std::map<core::NodeId, std::chrono::steady_clock::time_point> next_dial_;
    // Directly verified joiners waiting for the elected authority to discover them.
    std::map<core::NodeId, core::NodeInfo> join_referrals_;
    std::chrono::steady_clock::time_point next_join_referral_{};
    std::set<core::NodeId> connecting_;
    // Outbound connections this process initiated. Used to resolve duplicate
    // mutual dials deterministically: both ends keep the connection whose
    // initiator has the smaller node id.
    std::set<int> dialed_fds_;
    // Per-peer outbound dial attempt counter (seed reconnects are logged).
    std::map<core::NodeId, std::uint64_t> dial_attempts_;
    // Deduplication of diagnostic logs (log only when the wait reason changes).
    std::string last_ready_wait_;
    std::string last_transition_wait_;
    bool commit_uncertain_logged_ = false;
    std::chrono::steady_clock::time_point transition_deadline_{};
    // Automatic failed-member removal bookkeeping (authority only).
    std::set<core::NodeId> reported_suspect_;
    std::map<core::NodeId, std::chrono::steady_clock::time_point> unavailable_since_;
    std::map<core::NodeId, core::NodeStatus> last_peer_health_status_;
    std::map<core::NodeId, std::chrono::steady_clock::time_point> last_dial_error_log_;
    bool removal_transition_ = false;
    std::set<core::NodeId> removed_peers_;
    core::NodeId transition_candidate_ = 0; // join candidate of the running
                                            // transition (re-enqueued on abort)
    bool timeline_initial_emitted_ = false;
    std::chrono::steady_clock::time_point transition_started_{};
    std::string last_blocked_log_;
    std::chrono::steady_clock::time_point next_endpoint_broadcast_{};
    core::ConsistentHashRing ring_;
    bool membership_dirty_ = false;
    std::chrono::steady_clock::time_point started_{};
    bool leave_requested_ = false;
    std::chrono::steady_clock::time_point next_gossip_{};
    std::chrono::steady_clock::time_point next_repair_{};
    net::EventLoop loop_;
    std::unique_ptr<net::TcpServer> server_;

  

    std::map<int, core::NodeId> node_by_fd_;
    std::map<core::NodeId, int> fd_by_node_;
    std::map<core::NodeId, std::chrono::steady_clock::time_point> last_pong_;
    std::map<messages::Type, Handler> handlers_;
    std::uint32_t next_request_id_ = 1;
    std::uint64_t next_pixel_event_ = 0;
    PixelEventDedup seen_pixel_events_;
    std::map<std::uint32_t,core::VersionedPixel> last_browser_pixel_versions_;

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