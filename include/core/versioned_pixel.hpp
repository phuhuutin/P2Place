#pragma once
#include <cstdint>
#include <tuple>
#include "core/node_info.hpp"
struct VersionedPixel {
    std::uint32_t color = 0;
    std::uint64_t lamport = 0;
    p2place::core::NodeId node = 0;

    [[nodiscard]]
    bool newer_than(const VersionedPixel& other) const noexcept {
        return std::tie(lamport, node) >
               std::tie(other.lamport, other.node);
    }
};