#pragma once
#include "net/frame_codec.hpp"
#include <string>
#include <string_view>
namespace p2place::net {
class WebSocketCodec final : public FrameCodec {
public:
    static std::string handshake_response(std::string_view http_request);
    //Browser sends pixel
    // Encode a payload and append its frame to output.
    // Return false if the payload is too large.
    bool append_frame(std::vector<std::uint8_t>& out,std::span<const std::uint8_t> text) const override;
    //Server sends pixel or update
    bool extract_frame(std::vector<std::uint8_t>& input,std::vector<std::uint8_t>& text) const override;
};
}
