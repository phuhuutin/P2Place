#include "messages/payloads.hpp"

#include <limits>
#include <stdexcept>
#include <string>

namespace p2place::messages {
namespace {

constexpr std::size_t PUT_BYTES = 22;
constexpr std::size_t REGION_BYTES = 4 + core::Region::COUNT * 16;
constexpr std::size_t NODELIST_ENTRY_BYTES = 9;
// Peer protocol limit: 16,384-byte message body including a 6-byte envelope.
constexpr std::size_t MAX_NODELIST_PEERS = (16384 - 6 - 2) / NODELIST_ENTRY_BYTES;

void require_size(std::span<const std::uint8_t> data,
                  std::size_t expected,
                  const char* name) {
    if (data.size() != expected) {
        throw std::invalid_argument(std::string(name) + ": incorrect payload length");
    }
}

void require_available(std::span<const std::uint8_t> data,
                       std::size_t offset,
                       std::size_t count) {
    if (offset > data.size() || count > data.size() - offset) {
        throw std::out_of_range("truncated integer field");
    }
}

} // namespace

void append_u16(std::vector<std::uint8_t>& out, std::uint16_t value) {
    for (int shift = 8; shift >= 0; shift -= 8) {
        out.push_back(static_cast<std::uint8_t>(value >> shift));
    }
}

void append_u32(std::vector<std::uint8_t>& out, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        out.push_back(static_cast<std::uint8_t>(value >> shift));
    }
}

void append_u64(std::vector<std::uint8_t>& out, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<std::uint8_t>(value >> shift));
    }
}

std::uint16_t read_u16(std::span<const std::uint8_t> data, std::size_t& offset) {
    require_available(data, offset, 2);
    std::uint16_t value = 0;
    for (int i = 0; i < 2; ++i) {
        value = static_cast<std::uint16_t>((value << 8) | data[offset++]);
    }
    return value;
}

std::uint32_t read_u32(std::span<const std::uint8_t> data, std::size_t& offset) {
    require_available(data, offset, 4);
    std::uint32_t value = 0;
    for (int i = 0; i < 4; ++i) {
        value = (value << 8) | data[offset++];
    }
    return value;
}

std::uint64_t read_u64(std::span<const std::uint8_t> data, std::size_t& offset) {
    require_available(data, offset, 8);
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value = (value << 8) | data[offset++];
    }
    return value;
}

std::vector<std::uint8_t> encode_put(const PutPayload& payload) {
    if (payload.pixel_index >= core::Region::COUNT) {
        throw std::invalid_argument("PUT pixel index out of range");
    }
    std::vector<std::uint8_t> out;
    out.reserve(PUT_BYTES);
    append_u32(out, payload.key);
    append_u16(out, payload.pixel_index);
    append_u32(out, payload.pixel.color);
    append_u64(out, payload.pixel.lamport);
    append_u32(out, payload.pixel.node);
    return out;
}

PutPayload decode_put(std::span<const std::uint8_t> data) {
    require_size(data, PUT_BYTES, "PUT");
    std::size_t offset = 0;
    PutPayload payload;
    payload.key = read_u32(data, offset);
    payload.pixel_index = read_u16(data, offset);
    payload.pixel.color = read_u32(data, offset);
    payload.pixel.lamport = read_u64(data, offset);
    payload.pixel.node = read_u32(data, offset);
    if (payload.pixel_index >= core::Region::COUNT) {
        throw std::invalid_argument("PUT pixel index out of range");
    }
    return payload;
}

std::vector<std::uint8_t> encode_get(GetPayload payload) {
    std::vector<std::uint8_t> out;
    out.reserve(4);
    append_u32(out, payload.key);
    return out;
}

GetPayload decode_get(std::span<const std::uint8_t> data) {
    require_size(data, 4, "GET");
    std::size_t offset = 0;
    return {read_u32(data, offset)};
}

std::vector<std::uint8_t> encode_put_ok(PutOkPayload payload) {
    std::vector<std::uint8_t> out;
    out.reserve(12);
    append_u32(out, payload.key);
    append_u64(out, payload.applied_version);
    return out;
}

PutOkPayload decode_put_ok(std::span<const std::uint8_t> data) {
    require_size(data, 12, "PUT_OK");
    std::size_t offset = 0;
    return {read_u32(data, offset), read_u64(data, offset)};
}

std::vector<std::uint8_t> encode_region(const RegionPayload& payload) {
    std::vector<std::uint8_t> out;
    out.reserve(REGION_BYTES);
    append_u32(out, payload.key);
    for (const auto& pixel : payload.region.pixels()) {
        append_u32(out, pixel.color);
        append_u64(out, pixel.lamport);
        append_u32(out, pixel.node);
    }
    return out;
}

RegionPayload decode_region(std::span<const std::uint8_t> data) {
    require_size(data, REGION_BYTES, "REGION");
    std::size_t offset = 0;
    RegionPayload payload;
    payload.key = read_u32(data, offset);
    for (std::size_t index = 0; index < core::Region::COUNT; ++index) {
        core::VersionedPixel pixel;
        pixel.color = read_u32(data, offset);
        pixel.lamport = read_u64(data, offset);
        pixel.node = read_u32(data, offset);
        // Decode into Region using its public LWW API, rather than writing private storage.
        payload.region.set_pixel(index, pixel);
    }
    return payload;
}

std::vector<std::uint8_t> encode_nodelist(const NodeListPayload& payload) {
    if (payload.peers.size() > MAX_NODELIST_PEERS) {
        throw std::invalid_argument("NODELIST exceeds peer protocol frame limit");
    }
    std::vector<std::uint8_t> out;
    out.reserve(2 + NODELIST_ENTRY_BYTES * payload.peers.size());
    append_u16(out, static_cast<std::uint16_t>(payload.peers.size()));
    for (const auto& peer : payload.peers) {
        const auto status = static_cast<std::uint8_t>(peer.status);
        if (status < 1 || status > 3) {
            throw std::invalid_argument("invalid NODELIST peer status");
        }
        append_u32(out, peer.id);
        append_u16(out, peer.peer_port);
        append_u16(out, peer.ws_port);
        out.push_back(status);
    }
    return out;
}

NodeListPayload decode_nodelist(std::span<const std::uint8_t> data) {
    if (data.size() < 2) {
        throw std::invalid_argument("NODELIST too short");
    }
    std::size_t offset = 0;
    const auto count = read_u16(data, offset);
    if (count > MAX_NODELIST_PEERS || data.size() != 2 + count * NODELIST_ENTRY_BYTES) {
        throw std::invalid_argument("NODELIST incorrect count or length");
    }
    NodeListPayload payload;
    payload.peers.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        core::NodeInfo peer;
        peer.id = read_u32(data, offset);
        peer.peer_port = read_u16(data, offset);
        peer.ws_port = read_u16(data, offset);
        const auto status = data[offset++];
        if (status < 1 || status > 3) {
            throw std::invalid_argument("invalid NODELIST peer status");
        }
        peer.status = static_cast<core::NodeStatus>(status);
        // No host is encoded in this Lab 04 wire format; NodeInfo uses its default host.
        payload.peers.push_back(std::move(peer));
    }
    return payload;
}

std::vector<std::uint8_t> encode_sync_req(SyncReqPayload payload) {
    std::vector<std::uint8_t> out;
    out.reserve(12);
    append_u32(out, payload.key);
    append_u64(out, payload.my_version);
    return out;
}

SyncReqPayload decode_sync_req(std::span<const std::uint8_t> data) {
    require_size(data, 12, "SYNC_REQ");
    std::size_t offset = 0;
    return {read_u32(data, offset), read_u64(data, offset)};
}

} // namespace p2place::messages
