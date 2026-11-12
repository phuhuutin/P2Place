
/**
 * @file net/websocket_codec.cpp
 * @brief Implementation of the RFC 6455 handshake and framing.
 */
#include "net/websocket_codec.hpp"
#include "util/sha1.hpp"

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>
#include "net/protocol.hpp"
namespace p2place::net {

namespace {

/**
 * @brief Trim spaces and tabs from both ends of a string view.
 * @param value The text to trim.
 * @return The trimmed substring (possibly empty).
 */
std::string trim(std::string_view value) {
    const auto first = value.find_first_not_of(" \t");
    if (first == std::string_view::npos)
        return {};

    const auto last = value.find_last_not_of(" \t");
    return std::string(value.substr(first, last - first + 1));
}

/**
 * @brief Lowercase a string.
 * @param value The text to lowercase.
 * @return The lowercased copy.
 */
std::string lowercase(std::string_view value) {
    std::string result(value);

    std::transform(
        result.begin(), result.end(), result.begin(),
        [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        }
    );

    return result;
}

/**
 * @brief Check whether a comma-separated header value contains a token
 *        (case-insensitive).
 * @param value The header value.
 * @param expected The token to look for.
 * @return true if the token is present.
 */
bool contains_token(
    std::string_view value,
    std::string_view expected
) {
    while (!value.empty()) {
        const auto comma = value.find(',');
        const auto token = trim(value.substr(0, comma));

        if (lowercase(token) == expected)
            return true;

        if (comma == std::string_view::npos)
            break;

        value.remove_prefix(comma + 1);
    }

    return false;
}

/**
 * @brief Validate a Sec-WebSocket-Key value.
 *
 * RFC 6455: the Base64 encoding of exactly 16 bytes, canonical (the
 * final data character's 4 unused bits must be zero).
 * @param key The header value.
 * @return true if the key is well-formed.
 */
bool valid_websocket_key(std::string_view key) {
    // RFC 6455: Base64 encoding of exactly 16 bytes.
    if (key.size() != 24 || key.substr(22) != "==")
        return false;

    constexpr std::string_view alphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789+/";

    for (std::size_t i = 0; i < 22; ++i) {
        if (alphabet.find(key[i]) == std::string_view::npos)
            return false;
    }

    // The final Base64 data character has 4 unused bits.
    // A canonical encoding must have those bits set to zero.
    return alphabet.find(key[21]) % 16 == 0;
}

} // anonymous namespace

std::string WebSocketCodec::handshake_response(
    std::string_view request
) {
    // Require a complete HTTP header.
    const auto header_end = request.find("\r\n\r\n");

    if (header_end == std::string_view::npos ||
        header_end > 8192) {
        return {};
    }

    request = request.substr(0, header_end + 2);

    // 1. Validate request line.
    const auto first_end = request.find("\r\n");

    if (first_end == std::string_view::npos)
        return {};

    const auto request_line = request.substr(0, first_end);

    const auto first_space = request_line.find(' ');
    const auto last_space = request_line.rfind(' ');

    if (first_space == std::string_view::npos ||
        first_space == last_space ||
        request_line.substr(0, first_space) != "GET" ||
        request_line.substr(last_space + 1) != "HTTP/1.1" ||
        last_space == first_space + 1) {
        return {};
    }
    //"GET / HTTP/1.1" checked

    request.remove_prefix(first_end + 2);

    // 2. Parse HTTP headers.
    bool upgrade = false;
    bool connection = false;
    bool version = false;
    bool host = false;
    bool key_seen = false;

    std::string client_key;

    while (!request.empty()) {
        const auto end = request.find("\r\n");

        if (end == std::string_view::npos)
            return {};

        const auto line = request.substr(0, end);
        request.remove_prefix(end + 2);

        if (line.empty())
            break;

        const auto colon = line.find(':');

        if (colon == std::string_view::npos)
            return {};

        const auto name = lowercase(trim(line.substr(0, colon)));
        const auto value = trim(line.substr(colon + 1));

        if (name == "upgrade") {
            upgrade = upgrade ||
                contains_token(value, "websocket");
        }
        else if (name == "connection") {
            connection = connection ||
                contains_token(value, "upgrade");
        }
        else if (name == "sec-websocket-version") {
            version = (value == "13");
        }
        else if (name == "sec-websocket-key") {
            if (key_seen)
                return {}; // Duplicate key

            key_seen = true;
            client_key = value;
        }
        else if (name == "host") {
            host = !value.empty();
        }
    }

    // 3. Validate required WebSocket headers.
    if (!upgrade || !connection || !version ||
        !host || !key_seen ||
        !valid_websocket_key(client_key)) {
        return {};
    }

    // 4. Generate the WebSocket accept key.
    const std::string accept =
        util::websocket_accept_key(client_key);

    // 5. Return HTTP 101 response.
    return
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: " + accept + "\r\n"
        "\r\n";
}



/**
 * @brief A server-to-browser text frame has the form
 *        0x81, payload length, payload data.
 *
 * Example: a 3-byte payload "abc" encodes as 0x81 0x03 0x61 0x62 0x63.
 */
bool WebSocketCodec::append_frame(
    std::vector<std::uint8_t>& out,
    std::span<const std::uint8_t> text
) const {
    // Maximum payload allowed by P2Place
    if (text.size() > MAX_MESSAGE_SIZE ||
        text.size() > 65535) {
        return false;
    }

    // FIN = 1, opcode = 0x1 (text)
    out.push_back(0x81);

    const auto length = text.size();

    if (length < 126) {
        // 7-bit payload length
        out.push_back(
            static_cast<std::uint8_t>(length)
        );
    } else {
        // 126 means next 2 bytes contain length, 
        //127	The next 8 bytes contain the actual length
        out.push_back(126);

        // Network byte order (big-endian)
        out.push_back(
            static_cast<std::uint8_t>(length >> 8)
        );
        out.push_back(
            static_cast<std::uint8_t>(length & 0xFF)
        );
    }

    // Server-to-client frames are NOT masked
    out.insert(out.end(), text.begin(), text.end());

    return true;
}
bool WebSocketCodec::extract_frame(
    std::vector<std::uint8_t>& in,
    std::vector<std::uint8_t>& text
) const {
    // Need at least 2 bytes for the header
    if (in.size() < 2) {
        return false;
    }

    const std::uint8_t b0 = in[0];
    const std::uint8_t b1 = in[1];
                                                         //   0x80     opcode     
    const bool fin = (b0 & 0x80) != 0; //complete frame check 10000 0001
    const std::uint8_t opcode = b0 & 0x0F;

    //WebSocket rules
    // browser-to-server frames MUST be masked, and the server must unmask them.
    const bool masked = (b1 & 0x80) != 0;

    // Reject fragmentation and reserved bits
    // 0x70: 0x70 = 0111 0000, RSV1, RSV2, RSV3 must be 0
    if (!fin || (b0 & 0x70) != 0) {
        throw std::runtime_error(
            "Unsupported WebSocket fragmentation or RSV"
        );
    }

    // Only text frames are handled here
    if (opcode != 0x1) {
        throw std::runtime_error(
            "Unsupported WebSocket opcode"
        );
    }

    // Client-to-server frames MUST be masked
    if (!masked) {
        throw std::runtime_error(
            "WebSocket client frame is not masked"
        );
    }

    std::size_t payload_length = b1 & 0x7F; // 0x7F is 0111 1111,
    std::size_t offset = 2;

    // Extended 16-bit length
    if (payload_length == 126) {
        if (in.size() < 4) {
            return false;
        }

        payload_length =
            (static_cast<std::size_t>(in[2]) << 8) |
             static_cast<std::size_t>(in[3]);

        if (payload_length < 126) {
            throw std::runtime_error(
                "Non-minimal WebSocket length"
            );
        }

        offset = 4;
    } else if (payload_length == 127) {
        // Lab 3 supports only 7/16-bit lengths
        throw std::runtime_error(
            "64-bit WebSocket length unsupported"
        );
    }

    if (payload_length > MAX_MESSAGE_SIZE) {
        throw std::runtime_error(
            "WebSocket payload too large"
        );
    }

    // Need 4 bytes for the masking key
    if (in.size() < offset + 4) {
        return false;
    }

    const std::size_t mask_offset = offset;
    offset += 4;

    // Wait for full payload
    if (in.size() < offset + payload_length) {
        return false;
    }

    text.resize(payload_length);

    // Unmask payload using 4-byte XOR key
    for (std::size_t i = 0; i < payload_length; ++i) {
        text[i] = in[offset + i] ^
                  in[mask_offset + (i % 4)];
    }

    // Remove consumed frame from receive buffer
    in.erase(
        in.begin(),
        in.begin() + offset + payload_length
    );

    return true;
}
} // namespace p2place::net
