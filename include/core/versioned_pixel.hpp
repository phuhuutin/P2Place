/**
 * @file core/versioned_pixel.hpp
 * @brief One canvas pixel plus the version used for LWW comparison.
 */
#pragma once
#include <cstdint>
#include <tuple>
#include "core/node_info.hpp"

/**
 * @brief Core data model: Lamport clock, canvas geometry, membership.
 */
namespace p2place::core {

/**
 * @brief A pixel value together with its last-write-wins version.
 *
 * LWW order is the (lamport, node) lexicographic pair — a total order,
 * because two nodes never stamp the same pair.
 */
struct VersionedPixel {
    /** Pixel color as 0x00RRGGBB; 0 means untouched. */
    std::uint32_t color = 0;

    /** Lamport timestamp of the writing peer. */
    std::uint64_t lamport = 0;

    /** Originating peer id — breaks Lamport ties. */
    p2place::core::NodeId node = 0;

    /**
     * @brief True if this pixel is strictly newer than @p other.
     * @param other The pixel to compare against.
     * @return true if (lamport, node) > (other.lamport, other.node).
     */
    [[nodiscard]]
    bool newer_than(const VersionedPixel& other) const noexcept {
        return std::tie(lamport, node) >
            std::tie(other.lamport, other.node);
    }
};

} // namespace p2place::core
