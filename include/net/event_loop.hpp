
#pragma once

#include "net/socket.hpp"
#include "net/tcp_connection.hpp"
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <vector>
#include <cstdint>
#include <span>

namespace p2place::net {

struct PendingConnection {
    Socket socket;
    std::function<void(Socket)> on_connected;
    std::function<void(int)> on_error;
};    

class EventLoop final {
public:
    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // Register the server's listening socket.
    // EventLoop does not own this socket.
    void register_listener(
        int fd,
        std::function<void()> on_accept
    );

    // Transfer ownership of a connection to EventLoop.
    void add(std::unique_ptr<TcpConnection> conn);

    // Remove a connection safely.
    void remove(int fd);

    // Interrupt epoll_wait().
    void wake();

    // Process network events and periodic callbacks.
    void run(const std::function<void()>& on_tick);

    // Request the event loop to stop.
    void stop() noexcept;
    void send(int fd,std::span<const std::uint8_t> body);

    void add_pending_connection(
        Socket socket,
        std::function<void(Socket)> on_connected,
        std::function<void(int)> on_error
    );

    // Optional callback invoked with the fd whenever an established
    // connection is destroyed by the loop. PeerNode uses it to keep
    // its fd-to-peer mappings and PeerTable statuses in sync.
    void set_on_disconnect(std::function<void(int)> callback);

private:
    // Linux epoll and eventfd descriptors.
    Socket epfd_;
    Socket wake_fd_;

    // Listener is owned externally by TcpServer.
    int listener_ = -1;

    std::atomic<bool> running_{false};

    // Set by stop() and consumed by run(). Guarantees that a stop()
    // issued before run() started is not lost when run() sets
    // running_ to true.
    std::atomic<bool> stop_requested_{false};

    std::function<void()> on_accept_;
    std::function<void(int)> on_disconnect_;

    // EventLoop owns every connected TcpConnection.
    std::map<int, std::unique_ptr<TcpConnection>> connections_;

    // Defer destruction while processing returned epoll events.
    bool dispatching_ = false;
    std::vector<int> pending_removals_;

    void remove_now(int fd);
    void process_removals();

    std::map<int, PendingConnection> pending_connections_;
    void complete_connection(int fd);


};

} // namespace p2place::net
