
#include "net/event_loop.hpp"
#include "net/tcp_connection.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace p2place::net {

namespace {

constexpr int MAX_EVENTS = 64;
constexpr auto TICK_INTERVAL = std::chrono::milliseconds(500);

void check_epoll(int result, const char* operation) {
    if (result == -1) {
        throw std::system_error(
            errno,
            std::generic_category(),
            operation
        );
    }
}

} // namespace



void EventLoop::complete_connection(int fd) {
    auto it = pending_connections_.find(fd);

    if (it == pending_connections_.end())
        return;

    int error = 0;
    socklen_t len = sizeof(error);
    //this is get cket options, which is used to retrieve information 
    //about a socket, such as its error status, buffer sizes, and other settings.
    //we use it to determine whether a non-blocking TCP connection succeeded or failed.
    if (::getsockopt(
            fd,
            SOL_SOCKET,
            SO_ERROR,
            &error,
            &len
        ) == -1) {
        error = errno;
    }

    // Stop monitoring as a pending connection.
    ::epoll_ctl(
        epfd_.get(),
        EPOLL_CTL_DEL,
        fd,
        nullptr
    );

    // Move the pending entry out of the map.
    PendingConnection pending = std::move(it->second);

    pending_connections_.erase(it);

    if (error != 0) {
        if (pending.on_error)
            pending.on_error(error);

        // PendingConnection's Socket destructor
        // closes the failed socket.
        return;
    }

    // TCP is now connected.
    if (pending.on_connected) {
        pending.on_connected(
            std::move(pending.socket)
        );
    }
}


void EventLoop::set_on_disconnect(std::function<void(int)> callback) {
    on_disconnect_ = std::move(callback);
}

void EventLoop::add_pending_connection(
    Socket socket,
    std::function<void(Socket)> on_connected, //injected from PeerNode::on_connected
    std::function<void(int)> on_error //injecft  from PeerNode::on_connected
) {
    const int fd = socket.get();

    if (fd < 0) {
        throw std::invalid_argument("invalid socket");
    }

    if (pending_connections_.contains(fd) ||
        connections_.contains(fd)) {
        throw std::runtime_error("duplicate socket");
    }

    epoll_event ev{};
    //EPOLLOUT is used to indicate that the socket is ready for writing.
    //which is a good indication that the connection has been established.
    ev.events = EPOLLOUT | EPOLLET;
    ev.data.fd = fd;

    // Store ownership before registering with epoll.
    auto [it, inserted] = pending_connections_.emplace(
        fd,
        PendingConnection{
            std::move(socket),
            std::move(on_connected),
            std::move(on_error)
        }
    );

    if (::epoll_ctl(
            epfd_.get(),
            EPOLL_CTL_ADD,
            fd,
            &ev
        ) == -1) {
        int error = errno;
        pending_connections_.erase(it);

        throw std::system_error(
            error,
            std::generic_category(),
            "epoll_ctl ADD pending"
        );
    }
}

// ============================================================
// Constructor - TODO(L2.8)
// EPOLL_CLOEXEC automatically close the epoll file descriptor when 
// the process successfully executes another program
// EFD_NONBLOCK	Do not block when reading or writing
// EFD_CLOEXEC Automatically close the descriptor on successful exec()
// ============================================================

EventLoop::EventLoop()
    : epfd_(::epoll_create1(EPOLL_CLOEXEC)),
      wake_fd_(::eventfd(
          0,
          EFD_NONBLOCK | EFD_CLOEXEC
      )) {

    if (!epfd_ || !wake_fd_) {
        throw std::runtime_error(
            "Failed to initialize epoll or eventfd"
        );
    }

    epoll_event event{};

    event.events = EPOLLIN;
    event.data.fd = wake_fd_.get();

    check_epoll(
        ::epoll_ctl(
            epfd_.get(),
            EPOLL_CTL_ADD,
            wake_fd_.get(),
            &event
        ),
        "epoll_ctl wake_fd"
    );
}

// ============================================================
// Destructor - TODO(L2.9)
// ============================================================

EventLoop::~EventLoop() {
    running_.store(false);

    for (auto& [fd, conn] : connections_) {
        ::epoll_ctl(
            epfd_.get(),
            EPOLL_CTL_DEL,
            fd,
            nullptr
        );

        conn->shutdown();
    }

    connections_.clear();

    if (listener_ >= 0) {
        ::epoll_ctl(
            epfd_.get(),
            EPOLL_CTL_DEL,
            listener_,
            nullptr
        );
    }

    // epfd_ and wake_fd_ close automatically through RAII.
}

// ============================================================
// Register listener - TODO(L2.10)
// ============================================================

void EventLoop::register_listener(
    int fd,
    std::function<void()> on_accept
) {
    if (fd < 0 || !on_accept || listener_ >= 0) {
        throw std::invalid_argument(
            "Invalid listener registration"
        );
    }

    epoll_event event{};

    event.events = EPOLLIN | EPOLLET;

    // data.fd identifies the listening socket on dispatch.
    event.data.fd = fd;

    check_epoll(
        ::epoll_ctl(
            epfd_.get(),
            EPOLL_CTL_ADD,
            fd,
            &event
        ),
        "epoll_ctl add listener"
    );

    listener_ = fd;
    on_accept_ = std::move(on_accept);
}

// ============================================================
// Add connection - TODO(L2.15)
// ============================================================

void EventLoop::add(
    std::unique_ptr<TcpConnection> conn
) {
    if (!conn || conn->fd() < 0) {
        throw std::invalid_argument(
            "Invalid TcpConnection"
        );
    }

    const int fd = conn->fd();

    if (connections_.contains(fd)) {
        throw std::invalid_argument(
            "Connection already registered"
        );
    }

    epoll_event event{};

    event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;

    event.data.fd = fd;

    check_epoll(
        ::epoll_ctl(
            epfd_.get(),
            EPOLL_CTL_ADD,
            fd,
            &event
        ),
        "epoll_ctl add connection"
    );

    connections_.emplace(fd, std::move(conn));
}

// ============================================================
// Remove connection - TODO(L2.16)
// ============================================================

void EventLoop::remove_now(int fd) {
    auto it = connections_.find(fd);

    if (it == connections_.end()) {
        return;
    }

    ::epoll_ctl(
        epfd_.get(),
        EPOLL_CTL_DEL,
        fd,
        nullptr
    );

    it->second->shutdown();

    // Destroy TcpConnection and close its Socket.
    connections_.erase(it);

    // Notify the owner (PeerNode) so runtime peer tables
    // never keep referencing a destroyed connection.
    if (on_disconnect_) {
        try {
            on_disconnect_(fd);
        } catch (...) {
            // Bookkeeping callbacks must not break the loop.
        }
    }
}

void EventLoop::remove(int fd) {
    if (!connections_.contains(fd)) {
        return;
    }

    if (dispatching_) {
        // Avoid destroying connection objects while
        // returned epoll events may reference them.
        if (std::find(
                pending_removals_.begin(),
                pending_removals_.end(),
                fd
            ) == pending_removals_.end()) {
            pending_removals_.push_back(fd);
        }

        return;
    }

    remove_now(fd);
}

void EventLoop::process_removals() {
    for (int fd : pending_removals_) {
        remove_now(fd);
    }

    pending_removals_.clear();
}

// ============================================================
// Wake event loop - TODO(L2.17)
// ============================================================

void EventLoop::wake() {
    const std::uint64_t value = 1;

    while (true) {
        const ssize_t n = ::write(
            wake_fd_.get(),
            &value,
            sizeof(value)
        );

        if (n == static_cast<ssize_t>(sizeof(value))) {
            return;
        }

        if (n == -1 && errno == EINTR) {
            continue;
        }

        if (n == -1 &&
            (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // Wake counter already saturated.
            return;
        }

        throw std::system_error(
            errno,
            std::generic_category(),
            "eventfd write failed"
        );
    }
}

// ============================================================
// Stop event loop
// ============================================================

void EventLoop::stop() noexcept {
    stop_requested_.store(true);
    running_.store(false);

    // Wake epoll_wait() without allowing exceptions to escape.
    try {
        wake();
    } catch (...) {
    }
}

// ============================================================
// Event dispatch - TODO(L2.18)
// ============================================================

void EventLoop::run(
    const std::function<void()>& on_tick
) {
    using Clock = std::chrono::steady_clock;

    if (running_.exchange(true)) {
        throw std::logic_error(
            "EventLoop is already running"
        );
    }

    // A stop() that arrived before run() got scheduled must take
    // effect: consume the request and return without running.
    // (Without this, the exchange above would overwrite the
    // running_ = false written by stop().)
    if (stop_requested_.exchange(false)) {
        running_.store(false);
        return;
    }

    epoll_event events[MAX_EVENTS]{};

    auto next_tick = Clock::now() + TICK_INTERVAL;

    try {
        while (running_.load()) {
            const auto now = Clock::now();

            const auto remaining =
                std::chrono::duration_cast<
                    std::chrono::milliseconds
                >(next_tick - now);

            // Round upward so we don't continually
            // wake slightly before the deadline.
            int timeout = 0;

            if (remaining.count() > 0) { //dealine is still in the fututer
                timeout = static_cast<int>(
                    remaining.count()
                );
            }

            const int count = ::epoll_wait(
                epfd_.get(),
                events,
                MAX_EVENTS,
                timeout
            );

            if (count == -1) {
                if (errno == EINTR) {
                    continue;
                }

                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "epoll_wait failed"
                );
            }

            dispatching_ = true;

            for (int i = 0; i < count; ++i) {
                if (!running_.load()) {
                    break;
                }

                const auto flags = events[i].events;

                const int fd = events[i].data.fd;

                // Handle pending connections FIRST.
                if (pending_connections_.contains(fd)) {
                    if (flags & (EPOLLOUT | EPOLLERR | EPOLLHUP)) {
                        complete_connection(fd);
                    }

                    continue;
                }

                // 1. Wake descriptor event.
                if (fd == wake_fd_.get()) {
                    std::uint64_t value;

                    while (true) {
                        const ssize_t n = ::read(
                            wake_fd_.get(),
                            &value,
                            sizeof(value)
                        );

                        if (n > 0) {
                            continue;
                        }

                        if (n == -1 && errno == EINTR) {
                            continue;
                        }

                        break;
                    }

                    continue;
                }

                // 2. Incoming connections on listener.
                if (fd == listener_) {
                    if ((flags & EPOLLIN) && on_accept_) {
                        on_accept_();
                    }

                    continue;
                }

                // 3. Connected TCP socket.
                auto it = connections_.find(fd);

                if (it == connections_.end()) {
                    continue;
                }

                TcpConnection& conn = *it->second;

                if (std::find(
                        pending_removals_.begin(),
                        pending_removals_.end(),
                        fd
                    ) != pending_removals_.end()) {
                    continue;
                }

                bool alive = true;

                // Drain available incoming bytes.
                if (flags & EPOLLIN) {
                    alive = conn.drain_and_process();
                }

                // Flush queued outgoing bytes.
                if (alive && (flags & EPOLLOUT)) {
                    alive = conn.try_flush(
                        epfd_.get()
                    );
                }

                // Disconnect on fatal errors or hangup.
                if (flags & (EPOLLERR | EPOLLHUP)) { // Error | Hang UP
                    alive = false;
                }

                if (flags & EPOLLRDHUP) {
                    // Peer closed its sending side. Give any final
                    // buffered frames one more drain before closing.
                    conn.drain_and_process();
                    alive = false;
                }

                if (!alive) {
                    remove(fd);
                }
            }

            dispatching_ = false;
            process_removals();

            // Periodic heartbeat / maintenance callback.
            const auto current = Clock::now();

            if (current >= next_tick) {
                if (on_tick) {
                    on_tick();
                }

                next_tick = Clock::now() + TICK_INTERVAL;
            }
        }
    } catch (...) {
        dispatching_ = false;
        process_removals();
        running_.store(false);
        stop_requested_.store(false);
        throw;
    }

    process_removals();
    running_.store(false);
    stop_requested_.store(false);
}

void EventLoop::send(
    int fd,
    std::span<const std::uint8_t> body
) {
    auto it = connections_.find(fd);

    if (it == connections_.end()) {
        throw std::runtime_error(
            "EventLoop::send: connection not found"
        );
    }

    TcpConnection& conn = *it->second;

    // Append a length-prefixed message to the send buffer.
    conn.send_message_to_buffer(body);

    // Attempt to transmit the buffered bytes.
    // try_flush() manages EPOLLOUT when necessary.
    if (!conn.try_flush(epfd_.get())) {
        // Defer removal if currently dispatching a callback.
        // The connection must not be destroyed while in use.
        remove(fd);
    }
}

} // namespace p2place::net
