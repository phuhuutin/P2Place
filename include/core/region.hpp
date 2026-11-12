/**
 * @file core/region.hpp
 * @brief A 16x16 block of the canvas and its LWW merge logic.
 */
#pragma once
#include "core/versioned_pixel.hpp"
#include <array>
#include <cstddef>
#include <cstdint>

/**
 * @brief Core data model: Lamport clock, canvas geometry, membership.
 */
namespace p2place::core {

/**
 * @brief One 16x16 region of the canvas: 256 versioned pixels.
 *
 * The canvas is 16x16 regions of 16x16 pixels. A region is the unit of
 * replication and anti-entropy: merge() keeps the newer pixel at every
 * index, which makes two divergent copies converge.
 */
class Region final {
public:
    /** Region side length in pixels (locked: 16). */
    static constexpr std::size_t SIDE=16;
    /** Number of pixels per region (SIDE*SIDE = 256). */
    static constexpr std::size_t COUNT=SIDE*SIDE;

    /**
     * @brief Store a pixel if it is newer than the current one (LWW).
     * @param index Pixel index within the region, 0..COUNT-1.
     * @param incoming The candidate pixel.
     * @return true if the pixel was applied, false if it lost the
     *         (lamport, node) comparison.
     * @throws std::out_of_range if index >= COUNT.
     */
    bool set_pixel(std::size_t index,const VersionedPixel& incoming);

    /**
     * @brief Read the pixel at an index.
     * @param index Pixel index within the region, 0..COUNT-1.
     * @return Const reference to the stored pixel.
     * @throws std::out_of_range if index >= COUNT.
     */
    const VersionedPixel& pixel(std::size_t index)const;

    /**
     * @brief The freshest Lamport timestamp anywhere in the region.
     * @return max pixel lamport, or 0 for an untouched region.
     */
    std::uint64_t version()const noexcept;

    /**
     * @brief Merge another copy of the region into this one.
     *
     * Per-pixel last-write-wins: every index keeps the newer of the two
     * pixels. This is the anti-entropy primitive used by read-repair,
     * sync and hinted handoff in later labs.
     * @param remote The other copy of the region.
     * @return Number of pixels that changed.
     */
    std::size_t merge(const Region& remote);

    /**
     * @brief Raw access to all 256 pixels.
     * @return Const reference to the pixel array.
     */
    const std::array<VersionedPixel,COUNT>& pixels()const noexcept{return pixels_;}

private:
    /** The 256 pixels, row-major within the region. */
    std::array<VersionedPixel,COUNT> pixels_{};
};

} // namespace p2place::core
