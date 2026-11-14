/**
 * @file core/lamport_clock.hpp
 * @brief Lamport logical clock used to stamp canvas writes.
 *
 * Each peer keeps one LamportClock. Every local paint event calls tick()
 * so concurrent writes receive comparable timestamps; incoming remote
 * timestamps are merged via update() (max + 1). The clock is seeded with
 * restart_seed() so a restarted peer's stamps beat its pre-restart stamps
 * (restart safety).
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <chrono>

/**
 * @brief Core data model: Lamport clock, canvas geometry, membership.
 */
namespace p2place::core {

/**
 * @brief Lamport logical clock for totally ordering distributed events.
 *
 * The clock is a single monotonically increasing counter. Two peers can
 * stamp the same Lamport value, so P2Place breaks ties with the
 * originating peer id (see core::VersionedPixel).
 */
class LamportClock {
public:
    /** The integer type used for logical timestamps. */
    using Timestamp = std::uint64_t;

    /**
     * @brief Construct a clock with an initial value.
     * @param initial Starting timestamp (default 0).
     */
    explicit LamportClock(Timestamp initial = 0)
        : clock_(initial) {}

    /**
     * @brief Record a local event and return the new timestamp.
     * @return clock + 1.
     */
    Timestamp tick() {
        return ++clock_;
    }

    /**
     * @brief Merge a remote timestamp into the clock.
     *
     * clock = max(local, remote) + 1 — the standard Lamport receive rule.
     * @param remote The timestamp observed from another peer.
     * @return The updated clock value.
     */
    Timestamp update(Timestamp remote) {
        clock_ = std::max(clock_, remote) + 1;
        return clock_;
    }

    /**
     * @brief Read the current logical time without incrementing it.
     * @return The current clock value.
     */
    Timestamp now() const noexcept {
        return clock_;
    }

    /**
     * @brief Seed value for a freshly (re)started peer.
     *
     * The lab spec intends this to be the wall-clock time in
     * microseconds, so restarted peers usually stamp writes newer than their
     * pre-crash writes. This is a course-level heuristic, not durable logical
     * clock persistence (clock skew or rapid restart can still cause issues).
     * @return The initial clock value for this process.
     */
    static Timestamp restart_seed() noexcept {
        using namespace std::chrono;
        return static_cast<Timestamp>(duration_cast<microseconds>(
            system_clock::now().time_since_epoch()).count());
    }

private:
    /** The current logical time. */
    Timestamp clock_{0};
};

} // namespace p2place::core
