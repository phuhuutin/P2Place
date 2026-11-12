/**
 * @file net/tcp_client.hpp
 * @brief Non-blocking outbound TCP connection setup.
 */
#pragma once

#include "net/socket.hpp"

#include <cstdint>
#include <string_view>

/**
 * @brief Networking: epoll loop, sockets, connections, framing codecs.
 */
namespace p2place::net {

/**
 * @brief Outcome of a non-blocking connect() attempt.
 */
enum class ConnectStatus {
    /** connect() returned 0: the connection is already established. */
    Connected,
    /** EINPROGRESS: the connection is still being established. */
    InProgress
};

/**
 * @brief The socket plus the state of its connect() attempt.
 */
struct ConnectResult {
    /** The connecting socket (owned). */
    Socket socket;
    /** Whether the connection is established or still pending. */
    ConnectStatus status;
};

/**
 * @brief Static helper for initiating non-blocking TCP connections.
 */
class TcpClient final {
public:
    /**
     * @brief Initiate a nonblocking TCP connection to host:port.
     *
     * EINPROGRESS is not a connection failure — it means the
     * connection is still being established. The caller hands the
     * returned socket to EventLoop::add_pending_connection(), which
     * completes it via EPOLLOUT + SO_ERROR.
     * @param host IPv4 address as text (e.g. "127.0.0.1").
     * @param port Destination TCP port.
     * @return The socket and whether it connected immediately.
     * @throws std::system_error if socket() fails or connect() fails
     *         immediately with an error other than EINPROGRESS.
     * @throws std::invalid_argument if host is not a valid IPv4
     *         address.
     */
    static ConnectResult connect_to(
        std::string_view host,
        std::uint16_t port
    );
};

} // namespace p2place::net
