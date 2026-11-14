#pragma once

#include "core/peer_table.hpp"
#include <vector>

namespace p2place::core {

// Merge *indirect* NODELIST reports. Direct HELLO/PING/PONG and disconnect
// status remain authoritative. NODELIST has no incarnation/version number,
// therefore it cannot safely resurrect known Suspect/Dead peers.
// Returns true if any previously unknown Alive peer was learned.
[[nodiscard]] bool merge_gossip(
    PeerTable& table,
    NodeId self_id,
    const std::vector<NodeInfo>& reported
);

// Deterministic Alive membership for gossip and ring placement. PeerTable
// stores remote peers; self is supplied explicitly.
[[nodiscard]] std::vector<NodeInfo> live_membership(
    const PeerTable& table,
    const NodeInfo& self
);

} // namespace p2place::core
