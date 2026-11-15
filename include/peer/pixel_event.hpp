#pragma once
#include "core/versioned_pixel.hpp"
#include <cstdint>
#include <cstddef>
#include <deque>
#include <set>
#include <span>
#include <vector>

namespace p2place::peer {
// Dissemination is best effort, distinct from quorum success. Events are
// deduplicated by (origin, sequence) and bound to a committed placement.
struct PixelEvent {
    std::uint32_t origin=0;
    std::uint64_t sequence=0;
    std::uint64_t placement_fingerprint=0;
    std::uint32_t key=0;
    std::uint16_t index=0;
    core::VersionedPixel pixel;
};
std::vector<std::uint8_t> encode_pixel_event(const PixelEvent& event);
PixelEvent decode_pixel_event(std::span<const std::uint8_t> bytes);
class PixelEventDedup {
public:
    explicit PixelEventDedup(std::size_t limit=8192):limit_(limit){}
    bool accept(std::uint32_t origin,std::uint64_t sequence);
private:
    std::size_t limit_;
    std::set<std::pair<std::uint32_t,std::uint64_t>> seen_;
    std::deque<std::pair<std::uint32_t,std::uint64_t>> order_;
};
} // namespace p2place::peer
