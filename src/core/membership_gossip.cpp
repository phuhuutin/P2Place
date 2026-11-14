#include "core/membership_gossip.hpp"

#include <algorithm>
#include <stdexcept>

namespace p2place::core {

bool merge_gossip(PeerTable& table, NodeId self_id,
                  const std::vector<NodeInfo>& reported) {
    bool discovered = false;
    for (auto peer : reported) {
        // Reject invalid records and never trust hearsay about our own status.
        if (peer.id == 0 || peer.id == self_id || peer.peer_port == 0 ||
            peer.status != NodeStatus::Alive) {
            continue;
        }
        // Known peers are owned by direct connection/heartbeat state. Indirect
        // gossip must not rewrite their status, host, or port.
        if (table.find(peer.id).has_value()) {
            continue;
        }
        // The Lab 04 NODELIST wire format does not carry IP/hostnames.
        // Do NOT use NodeInfo's default 127.0.0.1 as a discovered address.
        peer.host.clear();
        table.upsert(peer);
        discovered = true;
    }
    return discovered;
}

std::vector<NodeInfo> live_membership(const PeerTable& table,
                                      const NodeInfo& self) {
    if (self.id == 0 || self.peer_port == 0) {
        throw std::invalid_argument("invalid local NodeInfo");
    }
    auto peers = table.alive_set();
    peers.erase(std::remove_if(peers.begin(), peers.end(),
                               [&](const NodeInfo& p) { return p.id == self.id; }),
                peers.end());
    auto local = self;
    local.status = NodeStatus::Alive;
    peers.push_back(std::move(local));
    std::sort(peers.begin(), peers.end(),
              [](const NodeInfo& a, const NodeInfo& b) { return a.id < b.id; });
    return peers;
}

} // namespace p2place::core
