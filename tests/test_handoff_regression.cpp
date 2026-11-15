// Lab 06 regression: replica handoff must merge every OLD replica into a
// newly added replica, not only the old primary. A W=2 write can be
// acknowledged by a secondary without ever reaching the primary, so the
// handoff source authorization must accept any legitimate former replica,
// and the destination must keep the newer per-pixel version.
//
// Mirrors the exact planning loop in PeerNode::progress_transition() and the
// authorization predicate in PeerNode::on_handoff_message() (HANDOFF_GET).
#include "core/balanced_placement.hpp"
#include "core/region_store.hpp"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <map>
#include <set>
#include <vector>

namespace {
using p2place::core::BalancedPlacement;
using p2place::core::NodeId;
using p2place::core::Region;
using p2place::core::RegionStore;
using p2place::core::VersionedPixel;

constexpr std::size_t N = 3; // N=3 / R=2 / W=2 per the lab baseline

// Authorization predicate, extracted from on_handoff_message() HANDOFF_GET:
// the responding replica must belong to the OLD committed replica set of the
// requested region. This is the fixed rule (was: primary() == self).
bool handoff_get_allowed(const BalancedPlacement& old_placement,
                         NodeId self, std::uint32_t key) {
    const auto replicas = old_placement.preference(
        key, std::min<std::size_t>(N, old_placement.members().size()));
    return std::find(replicas.begin(), replicas.end(), self) != replicas.end();
}

// The planning loop from progress_transition(): for every destination newly
// entering the canonical replica set, plan one move per OLD replica that
// holds the key.
std::vector<std::pair<NodeId, NodeId>> plan_moves(
    const BalancedPlacement& before, const BalancedPlacement& after,
    std::uint32_t key, const std::map<NodeId, std::set<std::uint32_t>>& held) {
    const auto old_list = before.preference(
        key, std::min<std::size_t>(N, before.members().size()));
    const auto new_list = after.preference(
        key, std::min<std::size_t>(N, after.members().size()));
    std::vector<std::pair<NodeId, NodeId>> moves; // (source, dest)
    for (auto dest : new_list) {
        if (std::find(old_list.begin(), old_list.end(), dest) != old_list.end())
            continue;
        for (auto source : old_list) {
            auto it = held.find(source);
            if (it != held.end() && it->second.contains(key))
                moves.emplace_back(source, dest);
        }
    }
    return moves;
}
}

int main() {
    // ---- 1. Old secondary is a legitimate handoff source ----------------
    auto old_placement = BalancedPlacement::create({1, 2, 3});
    bool saw_non_primary_source = false;
    for (std::uint32_t key = 0; key < BalancedPlacement::PARTITIONS; ++key) {
        const auto old_list = old_placement.preference(
            key, std::min<std::size_t>(N, old_placement.members().size()));
        assert(old_list.size() == N);
        assert(old_list.front() == old_placement.primary(key));
        for (auto replica : old_list) {
            assert(handoff_get_allowed(old_placement, replica, key));
            if (replica != old_placement.primary(key))
                saw_non_primary_source = true;
        }
        // Nodes outside the old replica set are rejected.
        for (auto id : {4, 5, 6})
            if (std::find(old_list.begin(), old_list.end(), id) == old_list.end())
                assert(!handoff_get_allowed(old_placement, id, key));
    }
    assert(saw_non_primary_source);
    std::cout << "PASS: every old replica (primary and secondary) is an "
              << "authorized handoff source; outsiders are rejected\n";

    // ---- 2. Join plan routes through every holder, not only primaries ----
    auto next = old_placement;
    next.plan_join(4);
    assert(next.epoch() == old_placement.epoch() + 1);
    assert(next.contains(4));
    // Simulate: region 7's data lives on the two NON-primary old replicas
    // (e.g., a W=2 write acknowledged there), the primary never saw it.
    const std::uint32_t key = 7;
    const auto old_list = old_placement.preference(
        key, std::min<std::size_t>(N, old_placement.members().size()));
    assert(old_list.size() == 3);
    const NodeId primary = old_list.front();
    std::map<NodeId, std::set<std::uint32_t>> held;
    for (auto source : old_list)
        if (source != primary) held[source].insert(key);
    auto moves = plan_moves(old_placement, next, key, held);
    assert(!moves.empty());
    // Every destination that needs the region has a move from EVERY holder,
    // including non-primary holders, so the newer secondary copy is merged.
    const auto new_list = next.preference(
        key, std::min<std::size_t>(N, next.members().size()));
    for (auto dest : new_list) {
        if (std::find(old_list.begin(), old_list.end(), dest) != old_list.end())
            continue;
        std::size_t from_this_dest = 0;
        for (auto [source, d] : moves)
            if (d == dest) {
                ++from_this_dest;
                assert(source != primary);
                assert(handoff_get_allowed(old_placement, source, key));
            }
        assert(from_this_dest == held.size());
    }
    std::cout << "PASS: join handoff plan merges non-primary holders into "
              << "every new replica destination\n";

    // ---- 3. Per-pixel LWW merge: stale primary + newer secondary ---------
    RegionStore destination;
    Region stale_primary;   // old primary's acknowledged-but-old copy
    Region newer_secondary; // secondary's newer acknowledged copy
    const std::uint16_t index = 5;
    stale_primary.set_pixel(index, VersionedPixel{0xFF0000, 100, primary});
    newer_secondary.set_pixel(index, VersionedPixel{0x00FF00, 200, old_list[1]});
    // Merge in handoff order (preference order: primary first, then the
    // secondary), exactly as the authority streams HANDOFF_DATA/HANDOFF_PUT.
    destination.merge_region(key, stale_primary);
    destination.merge_region(key, newer_secondary);
    const auto merged = destination.get_region(key);
    assert(merged.has_value());
    assert(merged->pixel(index).color == 0x00FF00);
    assert(merged->pixel(index).lamport == 200);
    // Reverse order must give the same result (LWW is order-independent).
    RegionStore destination2;
    destination2.merge_region(key, newer_secondary);
    destination2.merge_region(key, stale_primary);
    const auto merged2 = destination2.get_region(key);
    assert(merged2.has_value());
    assert(merged2->pixel(index).color == 0x00FF00);
    assert(merged2->pixel(index).lamport == 200);
    std::cout << "PASS: newer secondary pixel survives destination merge "
              << "regardless of source order (no lost acknowledged write)\n";

    std::cout << "PASS handoff regression (stale primary / newer secondary)\n";
    return 0;
}
