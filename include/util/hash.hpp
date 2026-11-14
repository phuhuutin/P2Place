#pragma once
#include <cstdint>
#include <string_view>

namespace p2place::util {
// Stable cross-process, cross-platform hash; do not replace with std::hash.
inline std::uint32_t fnv1a_32(std::string_view s) noexcept {
    std::uint32_t hash = 2166136261u;
    for (unsigned char byte : s) {
        hash ^= byte;
        hash *= 16777619u;
    }
    return hash;
}
} // namespace p2place::util
