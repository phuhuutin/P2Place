
/**
 * @file peer/config.cpp
 * @brief Implementation of command-line argument parsing.
 */
#include "peer/config.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>

namespace p2place::peer {

namespace {

/**
 * @brief Convert an argument to a positive unsigned integer.
 *
 * Rejects invalid input, overflow, and trailing characters.
 * @param text The argument text.
 * @param min_value Smallest allowed value.
 * @param max_value Largest allowed value.
 * @return The parsed value.
 * @throws std::invalid_argument if the text is not a valid integer in
 *         [min_value, max_value].
 */
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

/**
 * @brief Parse a peer specification of the form NodeId:IPv4:Port.
 *
 * Example: 2:127.0.0.1:9002
 * @param spec The seed specification text.
 * @return The peer record (status Suspect: address known, but
 *         availability not yet confirmed by HELLO).
 * @throws std::invalid_argument if the specification is malformed.
 */
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

/**
 * @brief Convert an argument to a finite double within (0,1].
 */
double parse_double(std::string_view text) {
    std::string s(text);
    char* end=nullptr;
    const double v=std::strtod(s.c_str(),&end);
    if(end!=s.c_str()+s.size() || !std::isfinite(v) || v<=0.0 || v>1.0)
        throw std::invalid_argument("Invalid double argument: "+s);
    return v;
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

        if (option == "--replicas") config.replication_n=parse_number(value,1,256);
        else if (option == "--read-quorum") config.read_r=parse_number(value,1,256);
        else if (option == "--write-quorum") config.write_w=parse_number(value,1,256);
        else if (option == "--suspect-after") config.suspect_after_seconds=parse_number(value,1,3600);
        else if (option == "--unavailable-after") config.unavailable_after_seconds=parse_number(value,1,3600);
        else if (option == "--recovery-stabilization") config.recovery_stabilization_seconds=parse_number(value,0,3600);
        else if (option == "--strategy") {
            if(value=="A")config.strategy=Config::Strategy::A;
            else if(value=="B")config.strategy=Config::Strategy::B;
            else throw std::invalid_argument("--strategy must be A or B");
        }
        else if (option == "--min-replicas") config.min_replicas=parse_number(value,1,256);
        else if (option == "--max-replicas") config.max_replicas=parse_number(value,1,256);
        else if (option == "--reliability-window") config.reliability_window_seconds=parse_number(value,1,86400);
        else if (option == "--policy-interval") config.policy_interval_seconds=parse_number(value,1,3600);
        else if (option == "--policy-cooldown") config.policy_cooldown_seconds=parse_number(value,0,86400);
        else if (option == "--policy-threshold-high") config.policy_threshold_high=parse_double(value);
        else if (option == "--policy-threshold-low") config.policy_threshold_low=parse_double(value);
        else if (option == "--metrics-dir") {
            config.metrics_dir=std::string(value);
            if(config.metrics_dir.empty())throw std::invalid_argument("empty --metrics-dir");
        }
        else if (option == "--id") {
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
        else if (option == "--state-file") {
            config.state_file = std::string(value);
            if(config.state_file.empty()) throw std::invalid_argument("empty --state-file");
        }
        else if (option == "--placement") {
            throw std::invalid_argument("--placement is removed; use automatic state persistence");
        }
        else if (option == "--leave-after") {
            config.leave_after_seconds=parse_number(value,1,86400);
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

    if(config.state_file.empty())
        config.state_file = "state/node" + std::to_string(config.id) + "/placement.state";
    if(config.id != Config::MEMBERSHIP_AUTHORITY && config.peers.empty())
        throw std::invalid_argument("non-authority must seed --peers 1:HOST:PORT");
    if(config.read_r>config.replication_n || config.write_w>config.replication_n)
        throw std::invalid_argument("R/W must be <= N");
    if(config.unavailable_after_seconds<=config.suspect_after_seconds)
        throw std::invalid_argument("--unavailable-after must exceed --suspect-after");
    if(config.strategy==Config::Strategy::B) {
        // Min N = 2: Strategy B may raise replicas under churn (mid tier
        // and N4) but must not shrink below the N2 baseline.
        if(config.min_replicas<2)
            throw std::invalid_argument("Strategy B requires --min-replicas >= 2");
        if(config.max_replicas<config.min_replicas)
            throw std::invalid_argument("--max-replicas must be >= --min-replicas");
        if(!(config.policy_threshold_low>0 &&
             config.policy_threshold_low<config.policy_threshold_high &&
             config.policy_threshold_high<=1.0))
            throw std::invalid_argument("require 0 < policy-threshold-low < policy-threshold-high <= 1");
    }
    return config;
}

} // namespace p2place::peer
