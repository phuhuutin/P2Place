/**
 * @file core/node_info.hpp
 * @brief Peer identity, status, and contact information.
 */

#pragma once

#include <cstdint>
#include <string>

/**
 * @brief Core data model: Lamport clock, canvas geometry, membership.
 */
namespace p2place::core {

/**
 * @brief Unique identifier assigned to each peer.
 */
using NodeId = std::uint32_t;

/**
 * @brief Current membership status of a peer.
 */
enum class NodeStatus : std::uint8_t {
    /** Peer is reachable and responding to heartbeats. */
    Alive = 1,
    /** Peer has missed heartbeats; failure not yet confirmed. */
    Suspect = 2,
    /** Peer is confirmed dead or disconnected. */
    Dead = 3
};

/**
 * @brief Information about a peer in the P2Place network.
 */
struct NodeInfo {
    /** Unique peer identifier. */
    NodeId id{};

    /** IP address or hostname. */
    std::string host{"127.0.0.1"};

    /** TCP port used for peer-to-peer communication. */
    std::uint16_t peer_port{};

    /** WebSocket port for the browser UI. */
    std::uint16_t ws_port{};

    /** Current membership status. */
    NodeStatus status{NodeStatus::Alive};
};

} // namespace p2place::core
