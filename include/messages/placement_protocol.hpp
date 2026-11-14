#pragma once
#include "core/balanced_placement.hpp"
#include <cstdint>
#include <span>
#include <vector>
namespace p2place::messages {
// Binary BE. Versioned snapshots: u64 generation, u16 memberCount,
// memberCount*u32 IDs, 256*u32 owners, u64 fingerprint.
std::vector<std::uint8_t> encode_placement(const core::BalancedPlacement& p);
core::BalancedPlacement decode_placement(std::span<const std::uint8_t> bytes);
std::vector<std::uint8_t> encode_partition_id(std::uint32_t key);
std::uint32_t decode_partition_id(std::span<const std::uint8_t> bytes);
}
