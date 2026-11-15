#pragma once
#include "core/node_info.hpp"
#include <chrono>
#include <map>
#include <optional>
#include <vector>

namespace p2place::core {
// Connectivity is NOT membership: Removed is only a recommendation to the
// membership authority. Never rebuild placement from this detector alone.
class FailureDetector final {
public:
    using Clock=std::chrono::steady_clock;
    enum class State {Healthy, Suspect, Unavailable};
    struct Config {
        std::chrono::milliseconds suspect_after{3000};
        std::chrono::milliseconds unavailable_after{15000};
        std::chrono::milliseconds recovery_stable_for{2000};
    };
    explicit FailureDetector(Config cfg);
    void observe(NodeId id, Clock::time_point at);
    void disconnected(NodeId id, Clock::time_point at);
    [[nodiscard]] State state(NodeId id, Clock::time_point now) const;
    [[nodiscard]] std::vector<NodeId> unavailable(Clock::time_point now) const;
private:
    struct Peer {Clock::time_point last; std::optional<Clock::time_point> disconnected_at;
                 std::optional<Clock::time_point> recovery_started;};
    Config cfg_;
    std::map<NodeId, Peer> peers_;
};
}
