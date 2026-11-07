#include "net/tcp_server.hpp"

#include <cerrno>
#include <stdexcept>
#include <system_error>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace p2place::net {

TcpServer::TcpServer(std::uint16_t port) {
    // TODO:
    // 1. Create listening socket
    int fd = ::socket(
        AF_INET, //IPv4
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0
    );
    //Check whether fd == -1. If it is, socket creation failed.
    if (fd == -1) {
        throw std::system_error(
            errno, std::generic_category(), "socket() failed"
        );
    }

    fd_.reset(fd);

    // 2. Enable SO_REUSEADDR
    int enabled = 1;
    if (::setsockopt(
            fd_.get(),
            SOL_SOCKET,
            SO_REUSEADDR,
            &enabled,
            sizeof(enabled)
        ) == -1) {
        throw std::system_error(
            errno, std::generic_category(), "setsockopt() failed"
        );
    }

    // 3. Configure sockaddr_in
    sockaddr_in address{};

    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    // 4. bind()
    if (::bind(
            fd_.get(),
            reinterpret_cast<sockaddr*>(&address),
            sizeof(address)
        ) == -1) {
        throw std::system_error(
            errno, std::generic_category(), "bind() failed"
        );
    }
    // 5. listen()
    if (::listen(fd_.get(), SOMAXCONN) == -1) {
        throw std::system_error(
            errno, std::generic_category(), "listen() failed"
        );
    }
}

void TcpServer::accept_ready(
    const std::function<void(Socket)>& callback
) {
    // TODO:
    // 1. Loop calling accept4()
    // 2. Wrap accepted fd in Socket
    // 3. Transfer ownership to callback
    // 4. Retry EINTR
    // 5. Stop on EAGAIN/EWOULDBLOCK

    while (true) {
        // Accept one connected socket.
        const int client_fd = ::accept4(
            fd_.get(),
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC
        );

        if (client_fd >= 0) {
            // RAII: this object now owns the accepted socket.
            Socket client(client_fd);

            // Transfer ownership to the application.
            callback(std::move(client));

            // Continue accepting pending connections.
            continue;
        }

        if (errno == EINTR) {
            // Interrupted by a signal; retry.
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // No more connections waiting to be accepted.
            return;
        }

        // Unexpected accept error.
        throw std::system_error(
            errno, std::generic_category(), "accept4() failed"
        );
    }
}

} // namespace p2place::net