/**
 * @file net/tcp_server.hpp
 * @brief Non-blocking TCP listener with accept4().
 */

#pragma once

#include "net/socket.hpp"

#include <cstdint>
#include <functional>

/**
 * @brief Networking: epoll loop, sockets, connections, framing codecs.
 */
namespace p2place::net {

/**
 * @brief A non-blocking, bound-and-listening TCP server socket.
 */
class TcpServer final {
public:
    /**
     * @brief Create a listening socket on all interfaces.
     *
     * socket() -> SO_REUSEADDR -> bind(INADDR_ANY) -> listen().
     * @param port TCP port to listen on.
     * @throws std::system_error if any setup step fails.
     */
    explicit TcpServer(std::uint16_t port);

    /**
     * @brief Listening socket descriptor.
     * @return fd_.get().
     */
    int fd() const noexcept {
        return fd_.get();
    }

    /**
     * @brief Accept all pending connections, transferring each to
     *        @p callback.
     *
     * Loops until EAGAIN/EWOULDBLOCK; retries on EINTR.
     * @param callback Invoked with the owned accepted socket.
     * @throws std::system_error on an unexpected accept error.
     */
    void accept_ready(
        const std::function<void(Socket)>& callback
    );

private:
    /** The owned listening socket. */
    Socket fd_;
};

} // namespace p2place::net
