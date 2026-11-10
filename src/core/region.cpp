#include "core/region.hpp"
#include <stdexcept>
#include <algorithm>
namespace p2place::core {
    bool Region::set_pixel(
        std::size_t index,
        const VersionedPixel& incoming
    ) {
        if (index >= COUNT) {
            throw std::out_of_range("pixel index");
        }

        auto& current = pixels_[index];

        if (!incoming.newer_than(current)) {
            return false;
        }

        current = incoming;
        return true;
    }
    const VersionedPixel& Region::pixel(std::size_t index)const {
    if(index>=COUNT) throw std::out_of_range("pixel index");
    return pixels_[index];
    }
    std::uint64_t Region::version()const noexcept {
        std::uint64_t latest = 0;

        for (const auto& pixel : pixels_) {
            latest = std::max(latest, pixel.lamport);
        }

        return latest;
    }
    std::size_t Region::merge(const Region& remote) {
        std::size_t changed = 0;

        for (std::size_t i = 0; i < COUNT; ++i) {
            if (set_pixel(i, remote.pixel(i))) {
                ++changed;
            }
        }

        return changed;
    }
}// namespace p2place::core
