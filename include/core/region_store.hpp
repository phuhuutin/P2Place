/**
 * @file core/region_store.hpp
 * @brief Memory-only store of all canvas regions.
 */
#pragma once
#include "core/region.hpp"
#include <cstdint>
#include <map>
#include <optional>
#include <vector>

/**
 * @brief Core data model: Lamport clock, canvas geometry, membership.
 */
namespace p2place::core {

/**
 * @brief Memory-only canvas: maps region keys to Region copies.
 *
 * A missing key means an untouched region. There is deliberately no
 * disk persistence; restarts recover through hinted handoff and
 * anti-entropy in later labs.
 */
class RegionStore final {
public:
    /** Canvas side length in pixels (locked: 256). */
    static constexpr std::uint32_t CANVAS_SIDE=256;
    /** Number of regions per canvas row (locked: 16). */
    static constexpr std::uint32_t REGIONS_SIDE=16;
    /** Total number of regions (locked: 256). */
    static constexpr std::uint32_t REGION_COUNT=256;

    /**
     * @brief Map a canvas pixel to its region key (row-major).
     * @param x Canvas x coordinate, 0..CANVAS_SIDE-1.
     * @param y Canvas y coordinate, 0..CANVAS_SIDE-1.
     * @return key = (y/16)*REGIONS_SIDE + x/16.
     * @throws std::out_of_range if x or y is outside the canvas.
     */
    static std::uint32_t region_key(std::uint16_t x,std::uint16_t y);

    /**
     * @brief Map a canvas pixel to its index inside its region.
     * @param x Canvas x coordinate, 0..CANVAS_SIDE-1.
     * @param y Canvas y coordinate, 0..CANVAS_SIDE-1.
     * @return index = (y%16)*16 + x%16.
     * @throws std::out_of_range if x or y is outside the canvas.
     */
    static std::uint16_t pixel_index(std::uint16_t x,std::uint16_t y);

    /**
     * @brief Apply a pixel to a region using last-write-wins.
     * @param region The region key (0..REGION_COUNT-1).
     * @param index Pixel index within the region (0..Region::COUNT-1).
     * @param px The candidate pixel.
     * @return true if the pixel was applied.
     * @throws std::out_of_range for an invalid key or index.
     */
    bool apply_pixel(std::uint32_t region,std::uint16_t index,const VersionedPixel& px);

    /**
     * @brief Fetch a snapshot copy of a region.
     * @param region The region key (0..REGION_COUNT-1).
     * @return The region copy, or std::nullopt if never written.
     * @throws std::out_of_range for an invalid key.
     */
    std::optional<Region> get_region(std::uint32_t region)const;

    /**
     * @brief Merge an incoming region copy into the store (LWW).
     * @param region The region key (0..REGION_COUNT-1).
     * @param incoming The other copy of the region.
     * @throws std::out_of_range for an invalid key.
     */
    void merge_region(std::uint32_t region,const Region& incoming);

    /**
     * @brief All region keys currently present in the store.
     * @return Vector of keys in ascending order (std::map order).
     */
    std::vector<std::uint32_t> all_keys()const;

    /**
     * @brief Approximate storage footprint in bytes.
     * @return regions_.size() * 4096 (one encoded region = 4096 bytes).
     */
    std::size_t replica_bytes()const noexcept {return regions_.size()*4096;}

private:
    /** region key -> region content. Missing key = untouched region. */
    std::map<std::uint32_t,Region> regions_;
};

} // namespace p2place::core
