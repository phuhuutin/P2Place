
#pragma once

#include "net/socket.hpp"

#include <cstdint>
#include <functional>

namespace p2place::net {

class TcpServer final {
public:
    explicit TcpServer(std::uint16_t port);

    // Listening socket descriptor.
    int fd() const noexcept {
        return fd_.get();
    }

    // Accept all pending connections.
    void accept_ready(
        const std::function<void(Socket)>& callback
    );

private:
    Socket fd_;
};

} // namespace p2place::net
