/**
 * @file messages/message.hpp
 * @brief The 6-byte binary message envelope shared by all peers.
 *
 * Every peer-channel message is framed as
 * [uint8 type][uint8 flags][uint32 BE request_id][payload].
 */
#pragma once
#include <cstdint>
#include <span>
#include <vector>

/**
 * @brief Peer-to-peer wire protocol: message types and the envelope.
 */
namespace p2place::messages {

/**
 * @brief All message types defined by the wire protocol.
 *
 * Labs 1-3 handle HELLO, PING and PONG; the remaining types are
 * reserved for the labs that name them (GET/PUT routing in Lab 4,
 * replication in Lab 5, recovery in Lab 6).
 */
enum class Type : std::uint8_t {
    HELLO=1, PING=2, PONG=3, GET=4, GET_OK=5, PUT=6, PUT_OK=7,
    REGION_PUT=8, SYNC_REQ=9, SYNC_RESP=10, NODELIST=11, HINT=12, ERROR=13, BYE=14
};

/** Flags bit: the message is a response (echoes request_id). */
inline constexpr std::uint8_t IS_RESPONSE = 0x01;

/**
 * @brief One decoded peer-channel message.
 */
struct Message {
    /** Message type (see Type). */
    Type type = Type::ERROR;

    /** Bit flags; IS_RESPONSE marks a response. */
    std::uint8_t flags = 0;

    /** Request id echoed by every response; 0 = broadcast. */
    std::uint32_t request_id = 0;

    /** Raw payload bytes; interpretation depends on type. */
    std::vector<std::uint8_t> payload;
};

/**
 * @brief Serialize a message into its 6-byte envelope + payload.
 * @param msg The message to serialize.
 * @return The big-endian wire bytes.
 */
std::vector<std::uint8_t> encode(const Message& msg);

/**
 * @brief Parse a message from its wire bytes.
 * @param bytes The received envelope + payload.
 * @return The decoded message.
 * @throws std::invalid_argument if the envelope is shorter than 6 bytes
 *         or the type byte is outside 1..14.
 */
Message decode(std::span<const std::uint8_t> bytes);

} // namespace p2place::messages
