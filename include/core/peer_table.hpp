
#pragma once

#include "core/node_info.hpp"

#include <map>
#include <optional>
#include <vector>

namespace p2place::core {

class PeerTable final {
public:
    // Insert a new peer or update an existing one.
    void upsert(const NodeInfo& info);

    // Find a peer by NodeId.
    // Returns std::nullopt if the peer does not exist.
    [[nodiscard]]
    std::optional<NodeInfo> find(NodeId id) const;

    // Change the status of an existing peer.
    void mark(NodeId id, NodeStatus status);

    // Return all Alive peers, sorted by NodeId.
    [[nodiscard]]
    std::vector<NodeInfo> alive_set() const;

private:
    // Maps NodeId -> NodeInfo.
    // std::map keeps keys sorted automatically.
    std::map<NodeId, NodeInfo> nodes_;
};

} // namespace p2place::core
