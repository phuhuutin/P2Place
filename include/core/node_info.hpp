
#pragma once

#include <cstdint>
#include <string>

namespace p2place::core {

// Unique identifier assigned to each peer.
using NodeId = std::uint32_t;

// Current membership status of a peer.
enum class NodeStatus : std::uint8_t {
    Alive = 1,
    Suspect = 2,
    Dead = 3
};

// Information about a peer in the P2Place network.
struct NodeInfo {
    NodeId id{};

    // IP address or hostname.
    std::string host{"127.0.0.1"};

    // TCP port used for peer-to-peer communication.
    std::uint16_t peer_port{};

    // WebSocket port for the browser UI.
    std::uint16_t ws_port{};

    // Current membership status.
    NodeStatus status{NodeStatus::Alive};
};

} // namespace p2place::core
