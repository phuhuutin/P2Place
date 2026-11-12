/**
 * @file net/socket.hpp
 * @brief RAII, move-only owner of a socket file descriptor.
 */
#pragma once
#include <utility>

/**
 * @brief Networking: epoll loop, sockets, connections, framing codecs.
 */
namespace p2place::net {

/**
 * @brief Move-only RAII wrapper around a socket file descriptor.
 *
 * The object owns exactly one fd and closes it in the destructor.
 * Copying is deleted so two Socket objects can never own the same fd.
 */
class Socket final {
    public:
        /**
         * @brief Construct from a raw descriptor (or -1 = no socket).
         * @param fd Descriptor to take ownership of.
         */
        explicit Socket(int fd = -1) noexcept : fd_(fd) {}

        /**
         * @brief Close the owned descriptor.
         */
        ~Socket() noexcept;

        /**
         * @brief Copying is disabled: two Socket objects must not own
         *        the same fd.
         */
        Socket(const Socket &) = delete;

        /**
         * @brief Copy assignment is disabled.
         */
        Socket &operator=(const Socket &) = delete;

        /**
         * @brief Move constructor: transfer ownership from @p other.
         *
         * Example usage:
         * Socket s1(5);
         * Socket s2(std::move(s1)); // s1 is now invalid (fd_ == -1)
         * @param other The socket being moved from.
         */
        Socket(Socket &&other) noexcept;

        /**
         * @brief Move assignment: close the current fd (if any), then
         *        take ownership of @p other's fd.
         *
         * Example usage:
         * Socket s1(5);
         * Socket s2;
         * s2 = std::move(s1); // s1 is now invalid
         * @param other The socket being moved from.
         * @return *this.
         */
        Socket &operator=(Socket &&other) noexcept;

        /**
         * @brief The raw descriptor.
         * @return The owned fd (possibly -1).
         */
        int get() const noexcept { return fd_; }

        /**
         * @brief True while a descriptor is owned.
         * @return fd_ >= 0.
         */
        explicit operator bool() const noexcept { return fd_ >= 0; }

        /**
         * @brief Implicit conversion to int for APIs expecting a raw
         *        descriptor.
         *
         * Example usage:
         * Socket s(5);
         * int fd = s; // Implicitly converts to int
         */
        operator int() const noexcept {
            return fd_;
        }

        /**
         * @brief Return the current fd and stop owning it.
         * @return The previously owned fd; fd_ becomes -1.
         */
        int release() noexcept;

        /**
         * @brief Close the current fd (if any) and own @p fd instead.
         * @param fd New descriptor to own (default -1 = none).
         */
        void reset(int fd = -1) noexcept;

    private:
        /** The owned descriptor; -1 when none. */
        int fd_ = -1;
};

} // namespace p2place::net
