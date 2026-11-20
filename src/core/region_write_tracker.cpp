/**
 * @file core/region_write_tracker.cpp
 * @brief Implementation of the Strategy C write tracker and report codec.
 */
#include "core/region_write_tracker.hpp"
#include <stdexcept>

namespace p2place::core {

namespace {
constexpr std::uint64_t NS_PER_SEC = 1'000'000'000ull;
std::uint64_t to_ns(std::chrono::steady_clock::duration d) {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(d).count());
}
void put16(std::vector<std::uint8_t>& v, std::uint16_t n) {
    v.push_back(static_cast<std::uint8_t>(n & 0xFF));
    v.push_back(static_cast<std::uint8_t>(n >> 8));
}
void put32(std::vector<std::uint8_t>& v, std::uint32_t n) {
    for (int i = 0; i < 4; ++i)
        v.push_back(static_cast<std::uint8_t>(n >> (i * 8)));
}
void put64(std::vector<std::uint8_t>& v, std::uint64_t n) {
    for (int i = 0; i < 8; ++i)
        v.push_back(static_cast<std::uint8_t>(n >> (i * 8)));
}
std::uint32_t get32(const std::vector<std::uint8_t>& b, std::size_t& at) {
    std::uint32_t n = 0;
    for (int i = 0; i < 4; ++i) n |= static_cast<std::uint32_t>(b.at(at++)) << (i * 8);
    return n;
}
std::uint64_t get64(const std::vector<std::uint8_t>& b, std::size_t& at) {
    std::uint64_t n = 0;
    for (int i = 0; i < 8; ++i) n |= static_cast<std::uint64_t>(b.at(at++)) << (i * 8);
    return n;
}
std::uint16_t get16(const std::vector<std::uint8_t>& b, std::size_t& at) {
    std::uint16_t n = static_cast<std::uint16_t>(b.at(at++));
    n |= static_cast<std::uint16_t>(b.at(at++)) << 8;
    return n;
}
} // namespace

RegionWriteTracker::RegionWriteTracker(std::chrono::seconds window,
                                       Clock::time_point start)
    : window_(window) {
    if (window_.count() < BUCKET_WIDE.count())
        throw std::invalid_argument("write window must be at least 10s");
    if (window_.count() % BUCKET_WIDE.count() != 0)
        throw std::invalid_argument("write window must be a multiple of 10s");
    bucket_count_ = static_cast<std::uint64_t>(
        window_.count() / BUCKET_WIDE.count());
    epoch_ = start;
    last_drain_ = start;
}

namespace {
void advance_ring(RegionBucketRing& r, std::uint64_t abs,
                  std::uint64_t bucket_count) {
    // Walk the ring from the stored head to the current absolute bucket,
    // zeroing slots as their data ages out. A jump >= bucket_count means the
    // whole ring is stale.
    if (abs == r.head_abs) return;
    const auto steps = abs - r.head_abs; // monotonic time: never negative
    if (steps >= bucket_count) {
        for (auto& b : r.buckets) b = 0;
    } else {
        for (std::uint64_t i = 0; i < steps; ++i) {
            r.head_abs += 1;
            r.buckets[r.head_abs % bucket_count] = 0;
        }
    }
    r.head_abs = abs;
}
} // namespace

void RegionWriteTracker::record_put_attempt(std::uint32_t region,
                                            Clock::time_point now) {
    const auto elapsed = now < epoch_ ? std::chrono::nanoseconds{0} : now - epoch_;
    const std::uint64_t abs = to_ns(elapsed) /
        (BUCKET_WIDE.count() * NS_PER_SEC);
    auto& r = regions_[region];
    advance_ring(r, abs, bucket_count_);
    const auto slot = abs % bucket_count_;
    if (r.buckets[slot] != 0xFFFFFFFFu) ++r.buckets[slot];
    if (delta_[region] != 0xFFFFu) ++delta_[region]; // saturating delta
}

std::uint64_t RegionWriteTracker::attempts_in_window(std::uint32_t region,
                                                     Clock::time_point now) {
    const auto elapsed = now < epoch_ ? std::chrono::nanoseconds{0} : now - epoch_;
    const std::uint64_t abs = to_ns(elapsed) / (BUCKET_WIDE.count() * NS_PER_SEC);
    auto& r = regions_[region];
    advance_ring(r, abs, bucket_count_);
    std::uint64_t total = 0;
    for (auto c : r.buckets) total += c;
    return total;
}

double RegionWriteTracker::rate_per_sec(std::uint32_t region,
                                        Clock::time_point now) {
    return static_cast<double>(attempts_in_window(region, now)) /
           static_cast<double>(window_.count());
}

RegionWriteTracker::DeltaReport RegionWriteTracker::drain_delta(
    Clock::time_point now) {
    DeltaReport d;
    d.start_ns = to_ns(last_drain_.time_since_epoch());
    d.end_ns = to_ns(now.time_since_epoch());
    for (std::size_t r = 0; r < REGIONS; ++r) {
        d.counts[r] = static_cast<std::uint16_t>(delta_[r]);
        delta_[r] = 0;
    }
    last_drain_ = now;
    return d;
}

namespace write_report {

std::vector<std::uint8_t> encode(std::uint32_t node, std::uint32_t incarnation,
                                 std::uint32_t sequence,
                                 const RegionWriteTracker::DeltaReport& d) {
    std::vector<std::uint8_t> v;
    v.reserve(PAYLOAD_BYTES);
    v.push_back(1); // version
    put32(v, node);
    put32(v, incarnation);
    put32(v, sequence);
    put64(v, d.start_ns);
    put64(v, d.end_ns);
    for (auto c : d.counts) put16(v, c);
    return v;
}

bool decode(const std::vector<std::uint8_t>& payload, Decoded& out) {
    if (payload.size() != PAYLOAD_BYTES) return false;
    if (payload[0] != 1) return false;
    std::size_t at = 1;
    out.node = get32(payload, at);
    out.incarnation = get32(payload, at);
    out.sequence = get32(payload, at);
    out.start_ns = get64(payload, at);
    out.end_ns = get64(payload, at);
    for (auto& c : out.counts) c = get16(payload, at);
    return true;
}

} // namespace write_report

WriteReportAggregate::WriteReportAggregate(std::chrono::seconds window,
                                           std::chrono::seconds interval)
    : window_(window), interval_(interval) {
    if (window_.count() <= 0 || interval_.count() <= 0 ||
        interval_ > window_)
        throw std::invalid_argument("invalid write aggregate window/interval");
}

bool WriteReportAggregate::accept(const write_report::Decoded& r,
                                  Clock::time_point now,
                                  std::string* reason) {
    ++total_reports_;
    const auto fail = [&](const char* why) {
        ++dropped_;
        if (reason) *reason = why;
        return false;
    };
    if (r.node == 0) return fail("zero node id");
    if (r.end_ns < r.start_ns) return fail("negative interval");
    if (r.end_ns > to_ns(now.time_since_epoch())) return fail("interval end in the future");
    // Dedup per (node, incarnation): a restart (new incarnation) starts a
    // fresh sequence space and must not be mistaken for a duplicate.
    auto& last = seen_seq_[{r.node, r.incarnation}];
    if (last != 0 && r.sequence <= last)
        return fail("duplicate or stale sequence");
    last = r.sequence;
    // Expire old intervals (also bounds the dedup history).
    const auto cutoff = to_ns(now.time_since_epoch()) - window_.count() * NS_PER_SEC;
    std::vector<Interval> live;
    live.reserve(intervals_.size() + 1);
    for (auto& iv : intervals_)
        if (iv.end_ns >= cutoff) live.push_back(std::move(iv));
        else ++gaps_;
    intervals_ = std::move(live);
    Interval iv;
    iv.node = r.node;
    iv.incarnation = r.incarnation;
    iv.sequence = r.sequence;
    iv.end_ns = r.end_ns;
    iv.counts = r.counts;
    intervals_.push_back(std::move(iv));
    ++accepted_;
    return true;
}

void WriteReportAggregate::clear() {
    intervals_.clear();
    seen_seq_.clear();
    total_reports_ = accepted_ = dropped_ = gaps_ = 0;
}

WriteReportAggregate::Summary WriteReportAggregate::summarize(
    Clock::time_point now) const {
    Summary s{};
    const auto cutoff = to_ns(now.time_since_epoch()) - window_.count() * NS_PER_SEC;
    std::size_t live = 0;
    for (const auto& iv : intervals_) {
        if (iv.end_ns < cutoff) continue;
        ++live;
        for (std::size_t r = 0; r < REGIONS; ++r)
            s.attempts[r] += iv.counts[r];
    }
    // Observation completeness: live intervals vs expected (window/interval).
    const std::size_t expected = static_cast<std::size_t>(
        window_.count() / interval_.count());
    for (std::size_t r = 0; r < REGIONS; ++r) {
        const auto pct = expected ? (live * 100) / expected : 100;
        s.observation_complete[r] = static_cast<std::uint8_t>(
            pct > 100 ? 100 : pct);
    }
    s.total_reports = total_reports_;
    s.accepted = accepted_;
    s.dropped = dropped_;
    s.report_gaps = gaps_;
    return s;
}

} // namespace p2place::core
