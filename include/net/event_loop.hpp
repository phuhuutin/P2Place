/**
 * @file net/event_loop.hpp
 * @brief Single-threaded epoll event loop (the gomoku pattern).
 *
 * The loop owns every TcpConnection and raw browser connection, handles
 * pending outbound connects, listener callbacks, a wake eventfd and a
 * periodic tick.
 */

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


/**
 * @brief Networking: epoll loop, sockets, connections, framing codecs.
 */
namespace p2place::net {

/**
 * @brief An outbound connection whose connect() is still in flight.
 */
struct PendingConnection {
    /** The connecting socket (owned). */
    Socket socket;
    /** Invoked with the socket once the connection succeeds. */
    std::function<void(Socket)> on_connected;
    /** Invoked with errno if the connection fails. */
    std::function<void(int)> on_error;
};

/** Invoked when the loop destroys a connection (fd given). */
using DisconnectCallback = std::function<void(int)>;
/** Invoked when a listener fd becomes readable (accept pending). */
using ListenerCallback = std::function<void()>;
/**
 * @brief Invoked with raw bytes read from a raw connection.
 * @param fd The connection's descriptor.
 * @param data The bytes just read.
 */
using RawDataCallback = std::function<void(
    int,
    std::span<const std::uint8_t>
)>;

/**
 * @brief A connection managed directly by the loop without a codec.
 *
 * Used by the browser channel (WebSocketServer), which owns its own
 * protocol state machine.
 */
struct RawConnection {
    /** The owned socket. */
    Socket socket;

    /** Called with every chunk of bytes received. */
    RawDataCallback on_data;
    /** Called once when the connection is destroyed. */
    DisconnectCallback on_disconnect;

    /** Framed output bytes waiting to be sent. */
    std::vector<std::uint8_t> output;
    /** Number of leading output bytes already transmitted. */
    std::size_t output_offset = 0;
    /** Close the connection as soon as output is flushed. */
    bool close_after_flush = false;

    /**
     * @brief Construct a raw connection.
     * @param s The socket (ownership transferred).
     * @param data_cb Receive callback.
     * @param disconnect_cb Disconnect callback.
     */
    RawConnection(
        Socket s,
        RawDataCallback data_cb,
        DisconnectCallback disconnect_cb
    )
        : socket(std::move(s)),
          on_data(std::move(data_cb)),
          on_disconnect(std::move(disconnect_cb)) {}
};

/**
 * @brief Single-threaded edge-triggered epoll loop with an eventfd wake.
 *
 * Conventions (the "gomoku pattern"):
 *  - listener fds: data.fd = fd, callback in listener_callbacks_;
 *  - wake eventfd: data.fd = wake_fd_;
 *  - connections: data.fd = fd, object in connections_ /
 *    raw_connections_.
 *
 * All callbacks run on the single thread that executes run().
 */
class EventLoop final {
public:
    /**
     * @brief Create the epoll instance and the wake eventfd.
     * @throws std::runtime_error if epoll_create1 or eventfd fails.
     */
    EventLoop();

    /**
     * @brief Shut down and close every managed connection.
     */
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    /**
     * @brief Register a server's listening socket.
     *
     * EventLoop does not own this socket; the owner must keep it alive.
     * @param fd The listening descriptor.
     * @param on_accept Invoked when the listener is readable.
     * @throws std::invalid_argument for a bad fd or empty callback;
     *         std::logic_error if the fd is already registered.
     */
    void register_listener(
        int fd,
        std::function<void()> on_accept
    );

    /**
     * @brief Remove a previously registered listener.
     * @param fd The listening descriptor.
     */
    void unregister_listener(int fd);

    /**
     * @brief Transfer ownership of an established connection to the
     *        loop.
     * @param conn The connection (ownership transferred).
     * @throws std::invalid_argument if the connection is invalid or
     *         its fd is already registered.
     */
    void add(std::unique_ptr<TcpConnection> conn);

    /**
     * @brief Remove a connection safely.
     *
     * During event dispatch the removal is deferred until the batch of
     * epoll events has been processed.
     * @param fd The connection descriptor.
     */
    void remove(int fd);

    /**
     * @brief Interrupt epoll_wait().
     *
     * Writes to the eventfd; EAGAIN means the counter is already
     * saturated and the loop will wake anyway.
     */
    void wake();

    /**
     * @brief Process network events and periodic callbacks until
     *        stop() is called.
     * @param on_tick Invoked about every 500 ms (steady clock).
     * @throws std::logic_error if the loop is already running.
     */
    void run(const std::function<void()>& on_tick);

    /**
     * @brief Request the event loop to stop.
     *
     * Safe to call from any thread; wakes the loop so run() returns.
     */
    void stop() noexcept;

    /**
     * @brief Frame and send a message on an established connection.
     * @param fd The connection descriptor.
     * @param body The payload to frame and queue.
     * @throws std::runtime_error if the connection is unknown.
     */
    void send(int fd,std::span<const std::uint8_t> body);

    /**
     * @brief Track an outbound connection while connect() is in flight.
     * @param socket The connecting socket (ownership transferred).
     * @param on_connected Called with the socket on success.
     * @param on_error Called with errno on failure.
     * @throws std::invalid_argument for an invalid socket;
     *         std::runtime_error for a duplicate fd.
     */
    void add_pending_connection(
        Socket socket,
        std::function<void(Socket)> on_connected,
        std::function<void(int)> on_error
    );

    /**
     * @brief Install a callback invoked with the fd whenever an
     *        established connection is destroyed by the loop.
     *
     * PeerNode uses it to keep its fd-to-peer mappings and PeerTable
     * statuses in sync.
     * @param callback The disconnect callback.
     */
    void set_on_disconnect(std::function<void(int)> callback);

    // WebSocket server support (Lab 3)

    /**
     * @brief Take ownership of a browser socket, register EPOLLIN and
     *        retain its callbacks.
     * @param socket The accepted socket (ownership transferred).
     * @param on_data Receive callback.
     * @param on_disconnect Disconnect callback.
     * @throws std::invalid_argument for an invalid socket/callback;
     *         std::logic_error if the fd is already registered.
     */
    void register_raw_connection(
        Socket socket,
        RawDataCallback on_data,
        DisconnectCallback on_disconnect
    );

    /**
     * @brief Append bytes to a raw connection's output queue and arm
     *        EPOLLOUT.
     * @param fd The raw connection's descriptor.
     * @param bytes The bytes to queue.
     * @throws std::out_of_range if the connection is unknown;
     *         std::runtime_error if the 8 MiB output cap is exceeded.
     */
    void send_raw(
        int fd,
        std::span<const std::uint8_t> bytes
    );

    /**
     * @brief Safely remove a connection after queued output, or close
     *        it immediately for an error.
     * @param fd The connection descriptor.
     */
    void close_connection(int fd);

    /**
     * @brief Mark a raw connection to be closed once its output queue
     *        has been flushed.
     * @param fd The raw connection's descriptor.
     */
    void close_after_flush(int fd);

private:
    /** The epoll descriptor. */
    Socket epfd_;
    /** The eventfd whose readability interrupts epoll_wait. */
    Socket wake_fd_;

    /** True while run() is executing. */
    std::atomic<bool> running_{false};

    /**
     * Set by stop() and consumed by run(). Guarantees that a stop()
     * issued before run() started is not lost when run() sets
     * running_ to true.
     */
    std::atomic<bool> stop_requested_{false};

    /** Optional callback for destroyed connections. */
    std::function<void(int)> on_disconnect_;

    /** EventLoop owns every connected TcpConnection. */
    std::map<int, std::unique_ptr<TcpConnection>> connections_;

    /** Defer destruction while processing returned epoll events. */
    bool dispatching_ = false;
    /** fds queued for removal at the end of a dispatch batch. */
    std::vector<int> pending_removals_;

    /**
     * @brief Remove and destroy a connection immediately.
     * @param fd The connection descriptor.
     */
    void remove_now(int fd);

    /**
     * @brief Apply every deferred removal queued by remove().
     */
    void process_removals();

    /** Outbound connects waiting for EPOLLOUT + SO_ERROR. */
    std::map<int, PendingConnection> pending_connections_;

    /**
     * @brief Finish an outbound connect: check SO_ERROR, then run the
     *        success or failure callback.
     * @param fd The connecting socket.
     */
    void complete_connection(int fd);

    /**
     * Listener fds and their callbacks; supports multiple listeners,
     * for example a TCP listener and a WebSocket listener.
     */
    std::unordered_map<int, ListenerCallback> listener_callbacks_;

    /** Raw browser connections owned by the loop. */
    std::unordered_map<int, std::unique_ptr<RawConnection>> raw_connections_;

    /**
     * @brief Handle one epoll event batch item for a raw connection.
     * @param fd The raw connection's descriptor.
     * @param events The epoll event flags for this fd.
     */
    void handle_raw_event(int fd, std::uint32_t events);

};

} // namespace p2place::net
