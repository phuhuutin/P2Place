#include "net/protocol_codec.hpp"
#include <stdexcept>
bool p2place::net::ProtocolCodec::append_frame(
    std::vector<std::uint8_t> &out, 
    std::span<const std::uint8_t> body) const
{   
    // TODO(L2.1):
    // Validate 1..MAX_MESSAGE_SIZE.
    size_t body_size = body.size();
    if (body_size == 0 || body_size > MAX_MESSAGE_SIZE) {
        return false;
    }

    // Append 2-byte big-endian length followed by body. (high_byte << 8) | low_byte
    // body_size is  64 bits = 8 bytes, but we only need the lower 16 bits for the length.
    out.push_back(static_cast<std::uint8_t>(body_size >> 8));
    out.push_back(static_cast<std::uint8_t>(body_size & 0xFF));
    out.insert(out.end(), body.begin(), body.end());
    return true;
}

bool p2place::net::ProtocolCodec::extract_frame(std::vector<std::uint8_t> &in, std::vector<std::uint8_t> &body) const
{   
    //get the length of the frame from the first two bytes
    if (in.size() < 2) {
        return false;
    }
    std::uint16_t length = (static_cast<std::uint16_t>(in[0]) << 8) | static_cast<std::uint16_t>(in[1]);

    if (length == 0 || length > MAX_MESSAGE_SIZE) {
        throw std::length_error("Invalid frame length");
    }

    if (in.size() < 2u + length) {
        return false;
    }

    body.assign(in.begin() + 2, in.begin() + 2u + length);
    in.erase(in.begin(), in.begin() + 2u + length);
    return true;
}
