#pragma once

#include "net/socket.hpp"

#include <cstdint>
#include <string_view>

namespace p2place::net {

enum class ConnectStatus {
    Connected,
    InProgress
};

struct ConnectResult {
    Socket socket;
    ConnectStatus status;
};

class TcpClient final {
public:
    // Initiates a nonblocking TCP connection.
    //
    // Returns an RAII Socket.
    //
    // The connection may be established or still pending.
    // For EINPROGRESE (is not a connection failure. It means the connection is still being established.), 
    //use EPOLLOUT and SO_ERROR
    // to determine when connection setup finishes.
    // 
    static ConnectResult connect_to(
        std::string_view host,
        std::uint16_t port
    );
};

} // namespace p2place::net
