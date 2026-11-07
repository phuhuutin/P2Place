#pragma once

#include "core/node_info.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace p2place::peer {

struct Config {
    core::NodeId id = 0;

    // Port used for peer-to-peer TCP communication.
    std::uint16_t port = 0;

    // Reserved for the browser WebSocket interface.
    std::uint16_t ws_port = 0;

    // Addresses of other known peers.
    std::vector<core::NodeInfo> peers;

    static Config parse_args(int argc, char* argv[]);
};

} // namespace p2place::peer
