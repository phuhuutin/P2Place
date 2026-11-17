#pragma once
#include "core/node_info.hpp"
#include <chrono>
#include <deque>
#include <map>
#include <vector>

namespace p2place::core {
// Rolling-window peer availability for Strategy B.
//
// A_i(t) = T_HEALTHY / T_observed over [t-W, t], where W is the configured
// observation window. Only actual HEALTH<->UNHEALTHY *transitions* are
// recorded: repeated same-state events do not double-count. Before the
// first confirmed observation a peer reports neutral score 1.0 with
// sampled=false. History for a NodeId is preserved across removal and
// rejoin for the lifetime of the process.
class PeerReliabilityTracker final {
public:
    using Clock = std::chrono::steady_clock;

    struct Snapshot {
        NodeId id = 0;
        double score = 1.0;          // [0,1]; neutral 1.0 when not sampled
        double observed_seconds = 0; // exposure inside the current window
        bool sampled = false;
    };

    explicit PeerReliabilityTracker(std::chrono::seconds window);

    // Record a state transition (call once per HEALTHY<->UNHEALTHY change).
    void observe(NodeId id, bool healthy, Clock::time_point now);

    // Committed removal: the peer counts unhealthy until re-observed healthy.
    void mark_removed(NodeId id, Clock::time_point now);

    [[nodiscard]] Snapshot get(NodeId id, Clock::time_point now) const;
    [[nodiscard]] std::vector<Snapshot> snapshot(Clock::time_point now) const;

private:
    struct Interval {
        Clock::time_point start{}, end{};
        bool healthy = true;
    };
    struct Peer {
        std::deque<Interval> intervals;
        bool current_healthy = false;
        bool observed = false;
    };

    std::chrono::seconds window_;
    std::map<NodeId, Peer> peers_;
};
}
