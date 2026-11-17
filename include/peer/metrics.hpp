#pragma once
#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>
namespace p2place::peer {
// Minimal CSV/JSONL metrics sinks (one file per series, per node). Used only
// when --metrics-dir is configured; benchmarks run without it to avoid
// file-I/O skew.
//
// Series:
//   peer_health_node<N>.csv, policy_node<N>.csv, operations_node<N>.csv
//   (existing, per-transition/per-interval rows)
//   region_replication_events_node<N>.csv   (replication N timeline events;
//       initial 256-row snapshot + changed-region rows + compact placement
//       change rows; buffered, flushed periodically and at shutdown)
//   placement_snapshots_node<N>.jsonl       (committed placement versions)
//   replica_backfill_events_node<N>.csv     (backfill completions/failures)
//
// Timeline events carry absolute CLOCK_MONOTONIC nanoseconds so the
// experiment runner can align every incarnation onto one workload clock.
class Metrics final {
public:
    Metrics(std::string run_id, std::filesystem::path dir, std::uint32_t node);
    ~Metrics();

    void health(double time_s, std::uint32_t peer, std::string_view state,
                double observed_seconds, double availability_score);
    void policy(double time_s, std::uint64_t gen, std::uint64_t epoch,
                std::uint32_t region, char strategy, double risk,
                std::uint8_t desired_n, std::uint8_t assigned_n,
                std::size_t confirmed_live_copies);
    void operation(double time_s, std::string_view op, std::uint32_t region,
                   bool success, double latency_ms, std::string_view error);

    // Replication-N timeline (event-driven; accepted state only).
    void replication_initial(const std::array<std::uint8_t, 256>& target_n,
                             std::uint64_t gen, std::uint64_t fp,
                             std::uint64_t epoch, std::uint32_t authority);
    void replication_n_change(std::uint32_t region, std::uint8_t old_n,
                              std::uint8_t new_n, std::uint64_t gen,
                              std::uint64_t fp, std::uint64_t epoch,
                              std::uint32_t authority, std::string_view reason);
    void replication_placement_change(std::uint64_t gen, std::uint64_t fp,
                                      std::uint64_t epoch, std::uint32_t authority,
                                      std::string_view reason);
    void placement_snapshot(std::uint64_t gen, std::uint64_t fp,
                            std::uint32_t authority,
                            const std::vector<std::uint32_t>& members);
    void backfill(std::uint32_t region, bool ok, std::uint64_t gen,
                  std::uint64_t epoch, std::string_view reason);

    [[nodiscard]] static std::string csv_escape(std::string_view s);
    [[nodiscard]] static std::uint64_t monotonic_ns();

private:
    // Small buffered sink: flush every N rows and at destruction; never
    // blocks the event loop on a per-row fsync/flush.
    struct BufferedSink {
        std::ofstream out;
        std::uint32_t rows_since_flush = 0;
        void flush() { if (out) { out.flush(); rows_since_flush = 0; } }
        ~BufferedSink() { if (out) out.flush(); }
    };

    std::string run_id_;
    std::uint32_t node_;
    std::ofstream health_, policy_, ops_;
    BufferedSink replication_events_;
    BufferedSink placement_snapshots_;
    BufferedSink backfill_events_;
};
}
