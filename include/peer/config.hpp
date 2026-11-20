/**
 * @file peer/config.hpp
 * @brief Command-line configuration for a peer process.
 */

#pragma once

#include "core/node_info.hpp"

#include <cstdint>
#include <cstddef>
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
    enum class Strategy { A, B, C };

    /** This peer's unique id. */
    core::NodeId id = 0;

    /** Port used for peer-to-peer TCP communication. */
    std::uint16_t port = 9000;

    /** Reserved for the browser WebSocket interface. */
    std::uint16_t ws_port = 9001;

    /** Path to the same committed fixed-partition manifest on every peer. */
    // The placement state is maintained automatically; NOT an epoch manifest.
    std::string state_file;
    // Node 1 is the initial bootstrap authority; Ring election may change it.
    static constexpr core::NodeId MEMBERSHIP_AUTHORITY = 1;
    // Optional graceful-leave trigger for reproducible churn experiments.
    // Zero disables automatic departure.
    std::uint32_t leave_after_seconds = 0;

    std::size_t replication_n = 3;
    std::size_t read_r = 2;
    std::size_t write_w = 2;

    // Failure-detection thresholds for automatic failed-member removal
    // (accelerated values allowed for lab experiments).
    std::uint32_t suspect_after_seconds = 3;
    std::uint32_t unavailable_after_seconds = 15;
    std::uint32_t recovery_stabilization_seconds = 2;

    // Replication policy (Strategy A = fixed, B = churn-adaptive).
    Strategy strategy = Strategy::A;
    std::uint32_t min_replicas = 2;              // B lower bound (min N = 2)
    std::uint32_t max_replicas = 4;              // B upper bound
    std::uint32_t reliability_window_seconds = 120;
    std::uint32_t policy_interval_seconds = 10;
    std::uint32_t policy_cooldown_seconds = 30;
    double policy_threshold_high = 0.95;         // min score >= high -> N2
    double policy_threshold_low = 0.80;          // min score <  low -> N4
    // Strategy C: cluster-wide per-region client PUT rate policy.
    std::uint32_t write_window_seconds = 60;       // rolling write-rate window
    std::uint32_t write_report_interval_seconds = 10; // per-peer delta cadence
    double write_threshold_mid = 0.20;             // rate >= mid  -> N3
    double write_threshold_high = 1.00;            // rate >= high -> N4
    std::string metrics_dir;                     // empty = no CSV metrics

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
