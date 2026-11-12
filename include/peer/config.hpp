/**
 * @file peer/config.hpp
 * @brief Command-line configuration for a peer process.
 */

#pragma once

#include "core/node_info.hpp"

#include <cstdint>
#include <string>
#include <vector>

/**
 * @brief The peer application: PeerNode composition root and Config.
 */
namespace p2place::peer {

/**
 * @brief All configuration needed to run one peer.
 */
struct Config {
    /** This peer's unique id. */
    core::NodeId id = 0;

    /** Port used for peer-to-peer TCP communication. */
    std::uint16_t port = 9000;

    /** Reserved for the browser WebSocket interface. */
    std::uint16_t ws_port = 9001;

    /** Addresses of other known peers (the seed list). */
    std::vector<core::NodeInfo> peers;

    /**
     * @brief Parse command-line arguments into a Config.
     *
     * Recognized options: --id N, --port N, --ws-port N and
     * --peers id:host:port[,id:host:port...]. --id and --port are
     * required.
     * @param argc Argument count from main().
     * @param argv Argument vector from main().
     * @return The validated configuration.
     * @throws std::invalid_argument on any malformed argument.
     */
    static Config parse_args(int argc, char* argv[]);
};

} // namespace p2place::peer
