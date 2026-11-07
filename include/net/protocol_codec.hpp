
#pragma once

#include "net/frame_codec.hpp"

namespace p2place::net {
constexpr std::size_t MAX_MESSAGE_SIZE = 16384;
class ProtocolCodec final : public FrameCodec {
public:


    /**
     *@brief Encode a payload and append its frame to output.
     *example: append_frame "ABC" -> 00 03 41 42 43
     *and then append_frame "XY" -> 00 02 58 59
     *out == 00 03 41 42 43 | 00 02 58 59
     *@return true if the frame is successfully appended, false otherwise.
     */
    bool append_frame(
        std::vector<std::uint8_t>& out,
        std::span<const std::uint8_t> body
    ) const override;
    /*
     * @brief Extract a frame from the input and store its payload in the output.
     * example: extract_frame 00 03 41 42 43 -> "ABC"
     * in == 00 03 41 42 43
     * body == "ABC"
     * @return true if a complete frame is extracted, false for incomplete input.
     */
    bool extract_frame(
        std::vector<std::uint8_t>& in,
        std::vector<std::uint8_t>& body
    ) const override;
};

} // namespace p2place::net
