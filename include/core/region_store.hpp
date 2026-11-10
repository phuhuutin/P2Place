#pragma once
#include "core/region.hpp"
#include <cstdint>
#include <map>
#include <optional>
#include <vector>
namespace p2place::core {
    class RegionStore final {
        public:
            static constexpr std::uint32_t CANVAS_SIDE=256,REGIONS_SIDE=16,REGION_COUNT=256;
            static std::uint32_t region_key(std::uint16_t x,std::uint16_t y);
            static std::uint16_t pixel_index(std::uint16_t x,std::uint16_t y);
            bool apply_pixel(std::uint32_t region,std::uint16_t index,const VersionedPixel& px);
            std::optional<Region> get_region(std::uint32_t region)const;
            void merge_region(std::uint32_t region,const Region& incoming);
            std::vector<std::uint32_t> all_keys()const;
            std::size_t replica_bytes()const noexcept {return regions_.size()*4096;}
        private:
            std::map<std::uint32_t,Region> regions_;
    };
}
