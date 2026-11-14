#pragma once

#include "core/node_info.hpp"
#include "core/balanced_placement.hpp"

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <vector>

namespace p2place::core {

class ConsistentHashRing final {
public:
    static constexpr int VIRTUAL_NODES = 64;

    // Production: canonical fixed partition placement.
    void rebuild(const BalancedPlacement& placement);

    // LEGACY only, retained so old unit tests remain usable.
    // Rebuild deterministically from physical peers; only Alive peers participate.
    void rebuild(const std::vector<NodeInfo>& peers);

    // First vnode clockwise from hash(to_string(key)), wrapping if necessary.
    [[nodiscard]] std::optional<NodeId> coordinator(std::uint32_t key) const;

    // Up to n DISTINCT physical owners in clockwise order (coordinator first).
    [[nodiscard]] std::vector<NodeId> successors(std::uint32_t key,
                                                 std::size_t n) const;

    // Sorted unique IDs of all physical owners on the ring.
    [[nodiscard]] std::vector<NodeId> all_nodes() const;

private:
    // Virtual token -> physical NodeId.
    std::map<std::uint32_t, NodeId> ring_;
    std::optional<BalancedPlacement> balanced_;
};

} // namespace p2place::core
