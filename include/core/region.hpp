#pragma once
#include "core/versioned_pixel.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
namespace p2place::core {

    class Region final {
    public:
        static constexpr std::size_t SIDE=16,COUNT=SIDE*SIDE;
        bool set_pixel(std::size_t index,const VersionedPixel& incoming);
        const VersionedPixel& pixel(std::size_t index)const;
        std::uint64_t version()const noexcept;
        std::size_t merge(const Region& remote);
        const std::array<VersionedPixel,COUNT>& pixels()const noexcept{return pixels_;}
    private:
        std::array<VersionedPixel,COUNT> pixels_{};
    };
}// namespace p2place::core