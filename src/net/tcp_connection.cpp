
/**
 * @file net/tcp_connection.cpp
 * @brief Implementation of the per-connection buffers and flushing.
 */
#include "net/tcp_connection.hpp"

#include <cerrno>
#include <stdexcept>
#include <utility>

#include <sys/epoll.h>
#include <sys/socket.h>

namespace p2place::net {

namespace {

/** Receive buffer cap: exceeding it closes the connection. */
constexpr std::size_t MAX_READ_BUFFER = 64 * 1024;
/** Send buffer cap for queued outgoing frames. */
constexpr std::size_t MAX_SEND_BUFFER = 64 * 1024;
/** Read in 4 KiB chunks. */
constexpr std::size_t READ_CHUNK_SIZE = 4096;

} // namespace

TcpConnection::TcpConnection(
    Socket fd,
    std::unique_ptr<FrameCodec> codec,
    MessageHandler handler
)
    : fd_(std::move(fd)),
      codec_(std::move(codec)),
      handler_(std::move(handler)) {

    if (!fd_ || !codec_ || !handler_) {
        throw std::invalid_argument(
            "TcpConnection requires a socket, codec and handler"
        );
    }
}

// ============================================================
// TODO(L2.3, L2.4) - Receiving
// ============================================================

bool TcpConnection::drain_and_process() {
    std::uint8_t buffer[READ_CHUNK_SIZE];

    // Phase 1: Read all currently available bytes.
    while (true) {
        const ssize_t n = ::recv(
            fd_.get(),
            buffer,
            sizeof(buffer),
            0
        );

        if (n > 0) {
            const auto count = static_cast<std::size_t>(n);

            // Prevent unbounded receive buffer growth.
            if (count > MAX_READ_BUFFER - read_buf_.size()) {
                return false;
            }

            read_buf_.insert(
                read_buf_.end(),
                buffer,
                buffer + count
            );

            continue;
        }

        if (n == 0) {
            // Remote peer closed its sending side.
            return false;
        }

        if (errno == EINTR) {
            // Interrupted by a signal. Retry.
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // No more bytes currently available.
            break;
        }

        // Unexpected socket error.
        return false;
    }

    // Phase 2: Extract and dispatch complete frames.
    try {
        std::vector<std::uint8_t> body;

        while (codec_->extract_frame(read_buf_, body)) {
            handler_(*this, body);
        }
    } catch (const std::exception&) {
        // Malformed frame or failed message handler.
        return false;
    }

    return true;
}

// ============================================================
// TODO(L2.7) - Queue outgoing messages
// ============================================================

void TcpConnection::send_message_to_buffer(
    std::span<const std::uint8_t> bytes
) {
    std::vector<std::uint8_t> frame;

    if (!codec_->append_frame(frame, bytes)) {
        throw std::length_error("Invalid outgoing frame");
    }

    // Compact the already transmitted prefix.
    if (send_offset_ > 0) {
        send_buf_.erase(
            send_buf_.begin(),
            send_buf_.begin() + send_offset_
        );

        send_offset_ = 0;
    }

    if (frame.size() > MAX_SEND_BUFFER - send_buf_.size()) {
        throw std::length_error("Outgoing buffer limit exceeded");
    }

    // Queue the complete encoded frame.
    send_buf_.insert(
        send_buf_.end(),
        frame.begin(),
        frame.end()
    );
}

// ============================================================
// TODO(L2.5, L2.6) - Sending
// ============================================================

bool TcpConnection::try_flush(int epfd) {

    // Attempt to transmit all queued bytes.
    while (send_offset_ < send_buf_.size()) {
        const std::size_t remaining =
            send_buf_.size() - send_offset_;

        const ssize_t n = ::send(
            fd_.get(),
            send_buf_.data() + send_offset_,
            remaining,
            MSG_NOSIGNAL 
            //prevents send() from generating a SIGPIPE signal when writing to a broken connection.
            //By default, SIGPIPE terminates the process.
        );

        if (n > 0) {
            send_offset_ += static_cast<std::size_t>(n);
            continue;
        }

        if (n < 0 && errno == EINTR) {
            continue;
        }

        if (n < 0 &&
            (errno == EAGAIN || errno == EWOULDBLOCK)) {
            // Socket cannot accept more bytes right now.
            break;
        }

        // Includes send() returning zero or fatal errors.
        return false;
    }

    const bool pending = send_offset_ < send_buf_.size();

    // Update epoll interest only when its state changes.
    if (pending != wants_write_) {
        epoll_event event{};

        event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;

        if (pending) {
            //notify us when the socket becomes writable
            event.events |= EPOLLOUT;
        }

        event.data.fd = fd_.get();

        if (::epoll_ctl(
                epfd,
                EPOLL_CTL_MOD,
                fd_.get(),
                &event
            ) == -1) {
            return false;
        }

        wants_write_ = pending;
    }

    if (!pending) {
        // Everything has been transmitted.
        send_buf_.clear();
        send_offset_ = 0;
    }

    return true;
}

// ============================================================
// Shutdown
// ============================================================

void TcpConnection::shutdown() noexcept {
    if (fd_) {
        ::shutdown(fd_.get(), SHUT_RDWR);
    }
}

int TcpConnection::fd()
{
    return fd_.get();
}

} // namespace p2place::net
