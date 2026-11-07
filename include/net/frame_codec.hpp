
#pragma once

#include <cstdint>
#include <span>
#include <vector>

namespace p2place::net {

class FrameCodec {
public:
    virtual ~FrameCodec() = default;

    // Encode a payload and append its frame to output.
    virtual bool append_frame(
        std::vector<std::uint8_t>& output,
        std::span<const std::uint8_t> body
    ) const = 0;

    // Return true when a complete frame is extracted.
    // Return false for incomplete input.
    // Malformed input should throw std::length_error
    // or std::runtime_error.
    virtual bool extract_frame(
        std::vector<std::uint8_t>& input,
        std::vector<std::uint8_t>& body
    ) const = 0;
};

} // namespace p2place::net
