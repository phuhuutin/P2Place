
#include "net/tcp_client.hpp"

#include <cerrno>
#include <stdexcept>
#include <string>
#include <system_error>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace p2place::net {

    

ConnectResult  TcpClient::connect_to(
    std::string_view host,
    std::uint16_t port
) {
    // 1. Create a nonblocking IPv4 TCP socket.
    const int fd = ::socket(
        AF_INET,
        SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
        0
    );

    if (fd == -1) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "socket() failed"
        );
    }

    // 2. Give ownership immediately to RAII Socket.
    Socket sock(fd);

    // 3. Configure remote address.
    //Specify the destination
    sockaddr_in address{};

    address.sin_family = AF_INET;
    address.sin_port = htons(port);

    // inet_pton requires a null-terminated string.
    const std::string host_string(host);

    //converts a human-readable numeric IP address 
    //into the binary representation used by socket APIs.
    const int result = ::inet_pton(
        AF_INET,
        host_string.c_str(),
        &address.sin_addr
    );

    if (result == 0) {
        throw std::invalid_argument(
            "Invalid IPv4 address: " + host_string
        );
    }

    if (result == -1) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "inet_pton() failed"
        );
    }

    // 4. Initiate TCP connection.
    const int rc = ::connect(
        sock.get(),
        reinterpret_cast<const sockaddr*>(&address),
        sizeof(address)
    );

    // rc == 0: Connected immediately.
    if (rc == 0) {
        return {
            std::move(sock),
            ConnectStatus::Connected
        };
    }

    // EINPROGRESS: Connection is being established.
    // EventLoop completes it via EPOLLOUT + SO_ERROR.
    if (rc == -1 && errno == EINPROGRESS) {
        return {
            std::move(sock),
            ConnectStatus::InProgress
        };
    }

    // In either case, return ownership of the socket.
    throw std::system_error(
        errno,
        std::generic_category(),
        "connect failed"
    );
     
}

} // namespace p2place::net
