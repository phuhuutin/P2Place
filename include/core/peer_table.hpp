/**
 * @file core/peer_table.hpp
 * @brief In-memory membership table of all known peers.
 */

#pragma once

#include "core/node_info.hpp"

#include <map>
#include <optional>
#include <vector>

/**
 * @brief Core data model: Lamport clock, canvas geometry, membership.
 */
namespace p2place::core {

/**
 * @brief Membership registry: maps NodeId to NodeInfo.
 *
 * The table is the single source of truth for who is in the network and
 * in what state. Lab 4's consistent hash ring will be a pure function of
 * the alive set returned here, which is why alive_set() returns the
 * entries sorted by id.
 */
class PeerTable final {
public:
    /**
     * @brief Insert a new peer or update an existing one.
     *
     * For existing peers only non-empty fields of @p info are copied in.
     * @param info The peer record to insert or merge.
     */
    void upsert(const NodeInfo& info);

    /**
     * @brief Find a peer by NodeId.
     * @param id The peer identifier to look up.
     * @return A copy of the peer record, or std::nullopt if the peer
     *         does not exist.
     */
    [[nodiscard]]
    std::optional<NodeInfo> find(NodeId id) const;

    /**
     * @brief Change the membership status of an existing peer.
     *
     * Does nothing if the peer is not present.
     * @param id The peer identifier.
     * @param status The new membership status.
     */
    void mark(NodeId id, NodeStatus status);

    /**
     * @brief Return all peers currently marked Alive, sorted by NodeId.
     * @return Vector of alive peer records in ascending id order.
     */
    [[nodiscard]]
    std::vector<NodeInfo> alive_set() const;

private:
    /**
     * @brief Maps NodeId -> NodeInfo.
     *
     * std::map keeps keys sorted automatically.
     */
    std::map<NodeId, NodeInfo> nodes_;
};

} // namespace p2place::core
