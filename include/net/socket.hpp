#pragma once
#include <utility>
namespace p2place::net {
class Socket final {
    public:
        explicit Socket(int fd = -1) noexcept : fd_(fd) {}
        ~Socket() noexcept;

        // Disable copying: Two Socket objects must not own the same fd.
        Socket(const Socket &) = delete;
        Socket &operator=(const Socket &) = delete;

        // Move constructor: Transfer ownership from 'other' to this object.
        // After moving, 'other' should have fd_ == -1.
        // example usage: 
        //Socket s1(5); 
        //Socket s2(std::move(s1)); // s1 is now invalid
        Socket(Socket &&other) noexcept;
        // Move assignment: Close the current fd (if any), then take
        Socket &operator=(Socket &&other) noexcept;
        int get() const noexcept { return fd_; }
        explicit operator bool() const noexcept { return fd_ >= 0; }

        // Implicit conversion to int for APIs expecting a raw descriptor.
        // example usage:
        //Socket s(5);
        //int fd = s; // Implicitly converts to int
        operator int() const noexcept {
            return fd_;
        } 

        int release() noexcept;

        void reset(int fd = -1) noexcept;

    private:
        int fd_ = -1;
};
} // namespace p2place::net
