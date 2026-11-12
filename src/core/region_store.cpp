/**
 * @file core/region_store.cpp
 * @brief Implementation of the memory-only canvas store.
 */
#include "core/region_store.hpp"
#include <stdexcept>
namespace p2place::core {

    std::uint32_t RegionStore::region_key(std::uint16_t x,std::uint16_t y){
        if(x>=CANVAS_SIDE||y>=CANVAS_SIDE)
            throw std::out_of_range("canvas coordinate");
        return (y/16)*REGIONS_SIDE+x/16;
    }

    std::uint16_t RegionStore::pixel_index(std::uint16_t x,std::uint16_t y){
        if(x>=CANVAS_SIDE||y>=CANVAS_SIDE)
            throw std::out_of_range("canvas coordinate");
        return (y%16)*16+x%16;
    }

    bool RegionStore::apply_pixel(
        std::uint32_t key,
        std::uint16_t index,
        const VersionedPixel& px
    ) {
        if (key >= REGION_COUNT) {
            throw std::out_of_range("region key");
        }

        if (index >= Region::COUNT) {
            throw std::out_of_range("pixel index");
        }

        return regions_[key].set_pixel(index, px);
    }
    std::optional<Region> RegionStore::get_region(
        std::uint32_t key
    ) const {
        if (key >= REGION_COUNT) {
            throw std::out_of_range("region key");
        }

        auto it = regions_.find(key);

        if (it == regions_.end()) {
            return std::nullopt;
        }

        return it->second; //returns a snapshot, not a reference into internal storage.
    }

    void RegionStore::merge_region(
        std::uint32_t key,
        const Region& incoming
    ) {
        if (key >= REGION_COUNT) {
            throw std::out_of_range("region key");
        }

        regions_[key].merge(incoming);
    }
    std::vector<std::uint32_t> RegionStore::all_keys() const {
        std::vector<std::uint32_t> keys;

        keys.reserve(regions_.size());

        for (const auto& [key, region] : regions_) {
            keys.push_back(key);
        }

        return keys;
    }
}
