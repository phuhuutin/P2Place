
#include "peer/config.hpp"

#include <charconv>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>

namespace p2place::peer {

namespace {

// Convert an argument to a positive unsigned integer.
// Reject invalid input, overflow, and trailing characters.
std::uint32_t parse_number(
    std::string_view text,
    std::uint32_t min_value,
    std::uint32_t max_value
) {
    std::uint32_t value = 0;

    const auto [ptr, ec] = std::from_chars(
        text.data(),
        text.data() + text.size(),
        value
    );

    if (ec != std::errc{} ||
        ptr != text.data() + text.size() ||
        value < min_value ||
        value > max_value) {
        throw std::invalid_argument(
            "Invalid numeric argument: " + std::string(text)
        );
    }

    return value;
}

// Parse a peer specification:
// NodeId:IPv4:Port
//
// Example:
// 2:127.0.0.1:9002
core::NodeInfo parse_peer(std::string_view spec) {
    const auto first = spec.find(':');
    const auto last = spec.rfind(':');

    if (first == std::string_view::npos ||
        last == std::string_view::npos ||
        first == last) {
        throw std::invalid_argument(
            "Invalid peer specification: " + std::string(spec)
        );
    }

    const auto id_text = spec.substr(0, first);
    const auto host = spec.substr(first + 1, last - first - 1);
    const auto port_text = spec.substr(last + 1);

    if (host.empty()) {
        throw std::invalid_argument("Peer host cannot be empty");
    }

    core::NodeInfo info;

    info.id = parse_number(
        id_text, 1, std::numeric_limits<std::uint32_t>::max()
    );

    info.host = std::string(host);

    info.peer_port = static_cast<std::uint16_t>(
        parse_number(port_text, 1, 65535)
    );

    // WebSocket port not specified by --peers.
    info.ws_port = 0;

    // Bootstrap address is known, but availability
    // has not yet been confirmed by HELLO.
    info.status = core::NodeStatus::Suspect;

    return info;
}

} // namespace

Config Config::parse_args(int argc, char* argv[]) {
    Config config;

    bool has_id = false;
    bool has_port = false;

    for (int i = 1; i < argc; ++i) {
        const std::string_view option(argv[i]);

        if (i + 1 >= argc) {
            throw std::invalid_argument(
                "Missing value for argument: " +
                std::string(option)
            );
        }

        const std::string_view value(argv[++i]);

        if (option == "--id") {
            if (has_id) {
                throw std::invalid_argument("Duplicate --id");
            }

            config.id = parse_number(
                value, 1, std::numeric_limits<std::uint32_t>::max()
            );
            has_id = true;
        }
        else if (option == "--port") {
            if (has_port) {
                throw std::invalid_argument("Duplicate --port");
            }

            config.port = static_cast<std::uint16_t>(
                parse_number(value, 1, 65535)
            );
            has_port = true;
        }
        else if (option == "--ws-port") {
            if (config.port == config.ws_port) {
                throw std::invalid_argument(
                    "Peer port and WebSocket port must differ"
                );
            }

            config.ws_port = static_cast<std::uint16_t>(
                parse_number(value, 1, 65535)
            );
        }
        else if (option == "--peers") {
            std::size_t start = 0;

            while (start < value.size()) {
                const auto comma = value.find(',', start);

                const auto spec = value.substr(
                    start,
                    comma == std::string_view::npos
                        ? std::string_view::npos
                        : comma - start
                );

                config.peers.push_back(parse_peer(spec));

                if (comma == std::string_view::npos) {
                    break;
                }

                start = comma + 1;
            }

            if (value.empty() || value.back() == ',') {
                throw std::invalid_argument(
                    "Invalid --peers list"
                );
            }
        }
        else {
            throw std::invalid_argument(
                "Unknown argument: " + std::string(option)
            );
        }
    }

    if (!has_id || !has_port) {
        throw std::invalid_argument(
            "Required arguments: --id and --port"
        );
    }

    std::unordered_set<core::NodeId> seen;

    for (const auto& peer : config.peers) {
        if (peer.id == config.id) {
            throw std::invalid_argument(
                "A node cannot bootstrap to itself"
            );
        }

        if (!seen.insert(peer.id).second) {
            throw std::invalid_argument(
                "Duplicate peer ID in --peers"
            );
        }
    }

    return config;
}

} // namespace p2place::peer
