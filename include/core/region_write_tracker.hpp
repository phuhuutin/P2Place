/**
 * @file core/region_write_tracker.hpp
 * @brief Strategy C local write-frequency tracker and delta-report codec.
 *
 * Counts CLIENT/browser PUT attempts per 16x16 region over a rolling time
 * window using a fixed ring of 10-second buckets (O(1) per attempt, no
 * allocation, no I/O). Peer-to-peer forwards, repairs, backfill and snapshot
 * reads must never be recorded here. Rates use a FIXED window divisor
 * (conservative warm-up: a fresh process reports low rates instead of
 * spurious hot regions).
 *
 * Also defines the wire codec for the per-peer delta report
 * (REGION_WRITE_COUNTS): each peer sends the attempts accumulated since its
 * previous report; the authority aggregates non-overlapping deltas.
 */
#pragma once
#include <array>
#include <chrono>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace p2place::core {

/** Per-region ring of 10-second buckets (head_abs = absolute bucket number
 *  stored in buckets_[0]; slots are zeroed lazily as time advances). */
struct RegionBucketRing {
    std::array<std::uint32_t, 6> buckets{};
    std::uint64_t head_abs = 0;
};

class RegionWriteTracker {
public:
    using Clock = std::chrono::steady_clock;
    static constexpr std::size_t REGIONS = 256;
    static constexpr std::size_t BUCKETS = 6;      // 6 x 10 s = 60 s window
    static constexpr std::chrono::seconds BUCKET_WIDE{10};

    struct DeltaReport {
        std::uint64_t start_ns = 0;   // CLOCK_MONOTONIC interval start
        std::uint64_t end_ns = 0;     // CLOCK_MONOTONIC interval end
        std::array<std::uint16_t, REGIONS> counts{};  // saturating deltas
    };

    explicit RegionWriteTracker(std::chrono::seconds window =
                                    std::chrono::seconds{60},
                                Clock::time_point start = Clock::now());

    /** Count one valid browser PUT attempt for region (idempotent-safe). */
    void record_put_attempt(std::uint32_t region, Clock::time_point now);

    /** Total attempts inside the rolling window (buckets expired lazily). */
    std::uint64_t attempts_in_window(std::uint32_t region,
                                     Clock::time_point now);

    /** attempts_in_window / window_seconds (fixed divisor; conservative). */
    double rate_per_sec(std::uint32_t region, Clock::time_point now);

    /**
     * Per-region attempts accumulated since the previous drain; clears the
     * delta counters and stamps the interval. The authority aggregates
     * NON-overlapping intervals, so overlapping rolling totals are never
     * summed across reports.
     */
    DeltaReport drain_delta(Clock::time_point now);

    std::chrono::seconds window_seconds() const { return window_; }

private:
    std::chrono::seconds window_;
    std::uint64_t bucket_count_;
    std::array<RegionBucketRing, REGIONS> regions_{};
    std::array<std::uint32_t, REGIONS> delta_{};
    Clock::time_point epoch_;       // fixed bucket-epoch anchor (construction)
    Clock::time_point last_drain_;  // delta-interval stamping only
};

/**
 * Wire codec for REGION_WRITE_COUNTS (control/metrics only; never a
 * replication write). Layout (little-endian):
 *   u8  version=1 | u32 node | u32 incarnation | u32 sequence
 *   u64 start_ns | u64 end_ns | u16[256] saturating per-region deltas
 * Total = 3 + 12 + 16 + 512 = 543 bytes (well under MAX_MESSAGE_SIZE).
 */
namespace write_report {
inline constexpr std::size_t PAYLOAD_BYTES = 541; // 1+12+16+512
struct Decoded {
    std::uint32_t node = 0, incarnation = 0, sequence = 0;
    std::uint64_t start_ns = 0, end_ns = 0;
    std::array<std::uint16_t, RegionWriteTracker::REGIONS> counts{};
};
std::vector<std::uint8_t> encode(std::uint32_t node, std::uint32_t incarnation,
                                 std::uint32_t sequence,
                                 const RegionWriteTracker::DeltaReport& d);
bool decode(const std::vector<std::uint8_t>& payload, Decoded& out);
} // namespace write_report

/**
 * Authority-side aggregation of per-peer delta reports into per-region
 * rolling totals. Deduplicates (node, incarnation, sequence); drops stale
 * sequences and old incarnations; expires intervals older than the window;
 * tracks observation completeness (present vs expected intervals).
 */
class WriteReportAggregate {
public:
    using Clock = std::chrono::steady_clock;
    static constexpr std::size_t REGIONS = RegionWriteTracker::REGIONS;

    struct Summary {
        std::array<std::uint64_t, REGIONS> attempts{};
        std::array<std::uint8_t, REGIONS> observation_complete{}; // 0..100 %
        std::uint64_t total_reports = 0, accepted = 0, dropped = 0;
        std::uint64_t report_gaps = 0;
    };

    explicit WriteReportAggregate(std::chrono::seconds window,
                                  std::chrono::seconds interval);

    /** Accept one decoded report; returns false when dropped (dedup/stale). */
    bool accept(const write_report::Decoded& r, Clock::time_point now,
                std::string* reason = nullptr);

    /** Drop everything (authority change: fresh warm-up). */
    void clear();

    Summary summarize(Clock::time_point now) const;

private:
    struct Interval {
        std::uint32_t node = 0, incarnation = 0, sequence = 0;
        std::uint64_t end_ns = 0;
        std::array<std::uint16_t, REGIONS> counts{};
    };
    std::chrono::seconds window_;
    std::chrono::seconds interval_;
    std::vector<Interval> intervals_;
    std::map<std::pair<std::uint32_t, std::uint32_t>, std::uint32_t> seen_seq_; // (node, incarnation) -> last seq
    std::uint64_t total_reports_ = 0, accepted_ = 0, dropped_ = 0, gaps_ = 0;
};

} // namespace p2place::core
