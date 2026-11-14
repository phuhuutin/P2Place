#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "core/node_info.hpp"
#include "core/region.hpp"
#include "core/versioned_pixel.hpp"

namespace p2place::messages {

void append_u16(std::vector<std::uint8_t>& out, std::uint16_t value);
void append_u32(std::vector<std::uint8_t>& out, std::uint32_t value);
void append_u64(std::vector<std::uint8_t>& out, std::uint64_t value);

std::uint16_t read_u16(std::span<const std::uint8_t> data, std::size_t& offset);
std::uint32_t read_u32(std::span<const std::uint8_t> data, std::size_t& offset);
std::uint64_t read_u64(std::span<const std::uint8_t> data, std::size_t& offset);

struct PutPayload {
    std::uint32_t key{};
    std::uint16_t pixel_index{};
    core::VersionedPixel pixel{};
};

struct GetPayload {
    std::uint32_t key{};
};

struct PutOkPayload {
    std::uint32_t key{};
    std::uint64_t applied_version{};
};

struct RegionPayload {
    std::uint32_t key{};
    core::Region region{};
};

struct NodeListPayload {
    std::vector<core::NodeInfo> peers;
};

struct SyncReqPayload {
    std::uint32_t key{};
    std::uint64_t my_version{};
};

std::vector<std::uint8_t> encode_put(const PutPayload& payload);
PutPayload decode_put(std::span<const std::uint8_t> data);

std::vector<std::uint8_t> encode_get(GetPayload payload);
GetPayload decode_get(std::span<const std::uint8_t> data);

std::vector<std::uint8_t> encode_put_ok(PutOkPayload payload);
PutOkPayload decode_put_ok(std::span<const std::uint8_t> data);

std::vector<std::uint8_t> encode_region(const RegionPayload& payload);
RegionPayload decode_region(std::span<const std::uint8_t> data);

std::vector<std::uint8_t> encode_nodelist(const NodeListPayload& payload);
NodeListPayload decode_nodelist(std::span<const std::uint8_t> data);

std::vector<std::uint8_t> encode_sync_req(SyncReqPayload payload);
SyncReqPayload decode_sync_req(std::span<const std::uint8_t> data);

} // namespace p2place::messages
