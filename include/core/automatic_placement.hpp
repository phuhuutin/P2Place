#pragma once
#include "core/balanced_placement.hpp"
#include <filesystem>
#include <optional>
#include <string>

namespace p2place::core {
// Membership coordination is separate from the failure detector.
// The active placement never changes until an external handoff protocol
// explicitly calls activate(). No manual epoch files are required.
class AutomaticPlacement final {
public:
    explicit AutomaticPlacement(std::filesystem::path state_file);

    // Start a fresh one-node cluster or recover a previously committed state.
    void bootstrap(NodeId self);

    const BalancedPlacement& active() const;
    const std::optional<BalancedPlacement>& pending() const noexcept { return pending_; }
    std::uint64_t generation() const;

    // Only the membership authority may call propose_*.
    // A disconnected node is NOT a permanent departure.
    std::size_t propose_join(NodeId id);
    std::size_t propose_leave(NodeId id);
    void abort() noexcept;

    // Call ONLY AFTER peers accepted the proposal, affected region snapshots
    // have been transferred, writes have been reconciled, and cutover is safe.
    void activate();

    // For follower nodes: install an already committed, validated snapshot,
    // received from an authenticated membership authority.
    void install_committed(const BalancedPlacement& incoming);

private:
    void require_ready() const;
    void persist(const BalancedPlacement& value) const;
    std::filesystem::path state_file_;
    std::optional<BalancedPlacement> active_;
    std::optional<BalancedPlacement> pending_;
};
} // namespace p2place::core
