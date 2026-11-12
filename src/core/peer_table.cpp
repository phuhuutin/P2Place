
/**
 * @file core/peer_table.cpp
 * @brief Implementation of the membership table.
 */
#include "core/peer_table.hpp"

namespace p2place::core {

// Insert a new peer or update information for an existing one.
void PeerTable::upsert(const NodeInfo& info) {
    auto it = nodes_.find(info.id);

    // New peer: insert the complete record.
    if (it == nodes_.end()) {
        nodes_.emplace(info.id, info);
        return;
    }

    // Existing peer: update the status.
    it->second.status = info.status;

    // Preserve known host and port information when
    // the incoming update does not provide replacements.
    if (!info.host.empty()) {
        it->second.host = info.host;
    }

    if (info.peer_port != 0) {
        it->second.peer_port = info.peer_port;
    }

    if (info.ws_port != 0) {
        it->second.ws_port = info.ws_port;
    }
}

// Find a peer by its identifier.
std::optional<NodeInfo> PeerTable::find(NodeId id) const {
    auto it = nodes_.find(id);

    if (it == nodes_.end()) {
        return std::nullopt;
    }

    return it->second;
}

// Update an existing peer's membership status.
void PeerTable::mark(NodeId id, NodeStatus status) {
    auto it = nodes_.find(id);

    if (it == nodes_.end()) {
        return;
    }

    it->second.status = status;
}

// Collect all peers currently marked Alive.
std::vector<NodeInfo> PeerTable::alive_set() const {
    std::vector<NodeInfo> result;

    for (const auto& [id, info] : nodes_) {
        if (info.status == NodeStatus::Alive) {
            result.push_back(info);
        }
    }

    return result;
}

} // namespace p2place::core
