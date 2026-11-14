#include "core/consistent_hash_ring.hpp"
#include "util/hash.hpp"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

namespace p2place::core {

void ConsistentHashRing::rebuild(const BalancedPlacement& placement) {
    placement.validate();
    balanced_ = placement;
    ring_.clear();
}

void ConsistentHashRing::rebuild(const std::vector<NodeInfo>& peers) {
    balanced_.reset();
    // Build into a temporary map: if allocation fails, keep the old ring.
    std::map<std::uint32_t, NodeId> next;

    std::vector<NodeId> ids;
    ids.reserve(peers.size());
    for (const auto& peer : peers) {
        if (peer.status == NodeStatus::Alive) {
            ids.push_back(peer.id);
        }
    }

    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

    for (const NodeId id : ids) {
        for (int vnode = 0; vnode < VIRTUAL_NODES; ++vnode) {
            // Canonical scheme from Lab 04 guide: "node_id#vnode".
            const std::string name = std::to_string(id) + "#" +
                                     std::to_string(vnode);
            std::uint32_t token = util::fnv1a_32(name);

            // Extremely rare 32-bit token collisions must resolve identically
            // across peers. Sorted IDs make insertion order deterministic.
            // Collision salt starts at 1; all peers use the same retry rule.
            for (std::uint64_t salt = 1; next.contains(token); ++salt) {
                if (salt > UINT32_MAX) {
                    throw std::runtime_error("ConsistentHashRing: token space exhausted");
                }
                token = util::fnv1a_32(name + "#collision" +
                                        std::to_string(salt));
            }
            next.emplace(token, id);
        }
    }
    ring_.swap(next);
}

std::optional<NodeId> ConsistentHashRing::coordinator(std::uint32_t key) const {
    if (balanced_) return balanced_->primary(key);
    if (ring_.empty()) {
        return std::nullopt;
    }
    auto it = ring_.lower_bound(util::fnv1a_32(std::to_string(key)));
    if (it == ring_.end()) {
        it = ring_.begin();
    }
    return it->second;
}

std::vector<NodeId> ConsistentHashRing::successors(
    std::uint32_t key, std::size_t n) const {
    if (balanced_) return balanced_->preference(key, n);
    std::vector<NodeId> owners;
    if (n == 0 || ring_.empty()) {
        return owners;
    }

    const std::size_t target = std::min(n, all_nodes().size());
    owners.reserve(target);
    std::set<NodeId> seen;

    auto it = ring_.lower_bound(util::fnv1a_32(std::to_string(key)));
    if (it == ring_.end()) {
        it = ring_.begin();
    }

    // At most one full lap is needed to encounter every physical owner.
    for (std::size_t visited = 0;
         visited < ring_.size() && owners.size() < target;
         ++visited) {
        if (seen.insert(it->second).second) {
            owners.push_back(it->second);
        }
        ++it;
        if (it == ring_.end()) {
            it = ring_.begin();
        }
    }
    return owners;
}

std::vector<NodeId> ConsistentHashRing::all_nodes() const {
    if (balanced_) return balanced_->members();
    std::set<NodeId> ids;
    for (const auto& [token, id] : ring_) {
        (void)token;
        ids.insert(id);
    }
    return {ids.begin(), ids.end()};
}

} // namespace p2place::core
