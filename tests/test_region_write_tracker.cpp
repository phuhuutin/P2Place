// Strategy C unit test: RegionWriteTracker bucket/window/drain behavior and
// the WriteReportAggregate dedup/expiration/observation rules.
#include "core/region_write_tracker.hpp"
#include <chrono>
#include <iostream>
#include <stdexcept>
using namespace p2place::core;
using Clock = RegionWriteTracker::Clock;
namespace {
void check(bool ok, const char* why) {
    if (!ok) throw std::runtime_error(why);
}
}

int main() {
    try {
        const auto t0 = Clock::now();
        const auto sec = [](long n) {
            return std::chrono::seconds(n);
        };

        // ---- 1. Counting, fixed 60s divisor, idle expiration -------------
        {
            RegionWriteTracker t(sec(60), t0);
            // 12 attempts in the first 10s bucket.
            for (int i = 0; i < 12; ++i) t.record_put_attempt(163, t0);
            check(t.attempts_in_window(163, t0) == 12, "12 attempts must be counted");
            check(t.rate_per_sec(163, t0) == 12.0 / 60.0, "fixed 60s divisor");
            // Advance 5s: same bucket, no loss.
            check(t.attempts_in_window(163, t0 + sec(5)) == 12, "same bucket must persist");
            // Advance 15s: the first bucket is still inside the 60s window.
            check(t.attempts_in_window(163, t0 + sec(15)) == 12, "15s-old bucket must persist");
            // Advance 65s: the bucket aged out of the window.
            check(t.attempts_in_window(163, t0 + sec(65)) == 0, "65s-old bucket must expire");
            check(t.rate_per_sec(163, t0 + sec(65)) == 0.0, "expired rate must be 0");
            // Region independence: region 0 was never touched.
            check(t.attempts_in_window(0, t0 + sec(65)) == 0, "untouched region must be 0");
        }

        // ---- 2. Threshold crossing: 12 and 60 observed attempts/60s ------
        {
            RegionWriteTracker t(sec(60), t0);
            // 60 attempts -> exactly 1.0 writes/s (>= high threshold).
            for (int i = 0; i < 60; ++i) t.record_put_attempt(7, t0);
            check(t.rate_per_sec(7, t0) == 1.0, "60 attempts must be 1.0/s");
            // 12 attempts -> 0.2 writes/s (>= mid threshold).
            RegionWriteTracker t2(sec(60), t0);
            for (int i = 0; i < 12; ++i) t2.record_put_attempt(7, t0);
            check(t2.rate_per_sec(7, t0) == 0.2, "12 attempts must be 0.2/s");
        }

        // ---- 3. Delta drain: non-overlapping intervals, saturating -------
        {
            RegionWriteTracker t(sec(60), t0);
            for (int i = 0; i < 40; ++i) t.record_put_attempt(163, t0);
            for (int i = 0; i < 5; ++i) t.record_put_attempt(9, t0);
            auto d1 = t.drain_delta(t0 + sec(10));
            check(d1.counts[163] == 40 && d1.counts[9] == 5,
                  "first delta must carry the accumulated counts");
            check(d1.start_ns < d1.end_ns, "delta interval must be ordered");
            // Second interval only carries NEW attempts: overlapping rolling
            // totals must never be summed across reports.
            for (int i = 0; i < 3; ++i) t.record_put_attempt(163, t0 + sec(11));
            auto d2 = t.drain_delta(t0 + sec(20));
            check(d2.counts[163] == 3, "second delta must exclude the first interval");
            check(d2.counts[9] == 0, "untouched region delta must be 0");
        }

        // ---- 4. Wire codec roundtrip + rejection -------------------------
        {
            RegionWriteTracker t(sec(60), t0);
            t.record_put_attempt(163, t0);
            auto d = t.drain_delta(t0 + sec(10));
            auto bytes = write_report::encode(7, 0xABCDEF, 42, d);
            check(bytes.size() == write_report::PAYLOAD_BYTES, "codec size");
            write_report::Decoded out;
            check(write_report::decode(bytes, out), "codec roundtrip");
            check(out.node == 7 && out.incarnation == 0xABCDEF && out.sequence == 42,
                  "codec identity fields");
            check(out.counts[163] == 1 && out.counts[162] == 0, "codec counts");
            bytes[0] = 9; // corrupt version
            check(!write_report::decode(bytes, out), "bad version must be rejected");
            bytes.resize(10);
            check(!write_report::decode(bytes, out), "short payload must be rejected");
        }

        // ---- 5. Aggregate: sum across gateways, no double count ----------
        {
            WriteReportAggregate a(sec(60), sec(10));
            write_report::Decoded r1{}, r2{};
            r1.node = 2; r1.incarnation = 100; r1.sequence = 1;
            r1.start_ns = 1000000000ull; r1.end_ns = 2000000000ull;
            r1.counts[163] = 40;
            r2.node = 3; r2.incarnation = 200; r2.sequence = 1;
            r2.start_ns = 1000000000ull; r2.end_ns = 2000000000ull;
            r2.counts[163] = 40;
            const auto now = Clock::now() + sec(1); // end_ns is wall-monotonic ns
            // Use timestamps relative to now for expiry correctness.
            const auto now_ns = static_cast<std::uint64_t>(
                now.time_since_epoch().count());
            r1.start_ns = now_ns - 10000000000ull;
            r1.end_ns = now_ns - 1000000000ull;
            r2.start_ns = r1.start_ns; r2.end_ns = r1.end_ns;
            check(a.accept(r1, now), "report from node 2 must be accepted");
            check(a.accept(r2, now), "report from node 3 must be accepted");
            // Rate: 80/60 = 1.333... per sec (not 40/60, not 160/60).
            auto s = a.summarize(now);
            check(s.attempts[163] == 80, "two gateways x 40 attempts must sum to 80");
            check(s.attempts[0] == 0, "untouched region aggregate must be 0");
            {
                const double rate = static_cast<double>(s.attempts[163]) / 60.0;
                check(rate > 1.33 && rate < 1.34,
                      "combined rate must be 80/60 = 1.333..., not double-counted");
            }
            // Duplicate (node, incarnation, sequence) must be dropped.
            std::string reason;
            check(!a.accept(r1, now, &reason) && reason == "duplicate or stale sequence",
                  "duplicate report must be dropped");
            // Stale sequence dropped; newer sequence accepted.
            r2.sequence = 0;
            check(!a.accept(r2, now), "stale sequence must be dropped");
            r2.sequence = 2;
            r2.end_ns = now_ns - 1000ull; // a later interval, still in the past
            check(a.accept(r2, now), "newer sequence must be accepted");
            // New incarnation is a fresh sequence space (restart, not dup).
            write_report::Decoded r3 = r1;
            r3.incarnation = 101; r3.sequence = 1;
            r3.end_ns = now_ns - 1000ull;
            check(a.accept(r3, now), "new incarnation with same sequence must be accepted");
            // The rolling window accumulates every accepted interval.
            s = a.summarize(now);
            check(s.attempts[163] == 160,
                  "rolling window must accumulate every accepted interval");
        }

        // ---- 6. Aggregate: expiry and observation completeness -----------
        {
            WriteReportAggregate a(sec(60), sec(10));
            const auto now = Clock::now();
            const auto now_ns = static_cast<std::uint64_t>(
                now.time_since_epoch().count());
            write_report::Decoded r{};
            r.node = 2; r.incarnation = 7; r.sequence = 1;
            r.start_ns = now_ns - 11000000000ull;  // 11s ago
            r.end_ns = now_ns - 10000000000ull;    // 10s ago (just inside)
            r.counts[5] = 20;
            check(a.accept(r, now), "in-window report must be accepted");
            write_report::Decoded old = r;
            old.sequence = 2; // newer sequence, but the interval is 71s old
            old.start_ns = now_ns - 72000000000ull;
            old.end_ns = now_ns - 71000000000ull;
            check(a.accept(old, now), "old-interval report must be accepted (then expired)");
            auto s = a.summarize(now);
            check(s.attempts[5] == 20, "expired interval must not count");
            check(s.observation_complete[5] > 0, "observation must reflect live intervals");
            // End in the future -> malformed.
            write_report::Decoded fut = r;
            fut.sequence = 3;
            fut.end_ns = now_ns + 5000000000ull;
            check(!a.accept(fut, now), "future interval must be rejected");
        }

        std::cout << "PASS strategy C tracker/aggregate: fixed-divisor rates, "
                     "idle expiration, non-overlapping deltas, codec, "
                     "multi-gateway summation, dedup/incarnation rules\n";
    } catch (const std::exception& e) {
        std::cerr << "FAIL strategy C tracker: " << e.what() << '\n';
        return 1;
    }
    return 0;
}
