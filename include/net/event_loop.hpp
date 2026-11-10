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
#include <unordered_map>
#include <utility>
#include <cstddef>


namespace p2place::net {

struct PendingConnection {
    Socket socket;
    std::function<void(Socket)> on_connected;
    std::function<void(int)> on_error;
};

using DisconnectCallback = std::function<void(int)>;
using ListenerCallback = std::function<void()>;
using RawDataCallback = std::function<void(
    int,
    std::span<const std::uint8_t>
)>;

struct RawConnection {
    Socket socket;

    RawDataCallback on_data;
    DisconnectCallback on_disconnect;

    std::vector<std::uint8_t> output;
    std::size_t output_offset = 0;
    bool close_after_flush = false;

    RawConnection(
        Socket s,
        RawDataCallback data_cb,
        DisconnectCallback disconnect_cb
    )
        : socket(std::move(s)),
          on_data(std::move(data_cb)),
          on_disconnect(std::move(disconnect_cb)) {}
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

    void unregister_listener(int fd);

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

    //websocket server support
    //Take socket ownership, register EPOLLIN, and retain callbacks
    void register_raw_connection(
        Socket socket,
        RawDataCallback on_data,
        DisconnectCallback on_disconnect
    );
    //Append bytes to an output queue and enable EPOLLOUT
    void send_raw(
        int fd,
        std::span<const std::uint8_t> bytes
    );
    //Safely remove connection after queued output, or close immediately for an error
    void close_connection(int fd);

    void close_after_flush(int fd);

private:
    // Linux epoll and eventfd descriptors.
    Socket epfd_;
    Socket wake_fd_;



    std::atomic<bool> running_{false};

    // Set by stop() and consumed by run(). Guarantees that a stop()
    // issued before run() started is not lost when run() sets
    // running_ to true.
    std::atomic<bool> stop_requested_{false};

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

    //listener_fd_ and its callback funcs
    //support multiple listeners, for example, a TCP listener and websocket listener

    
    std::unordered_map<int, ListenerCallback> listener_callbacks_;

    std::unordered_map<int, std::unique_ptr<RawConnection>> raw_connections_;

    void handle_raw_event(int fd, std::uint32_t events);

};

} // namespace p2place::net
