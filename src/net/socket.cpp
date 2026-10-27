#include "net/socket.hpp"
#include <unistd.h>
namespace p2place::net {
Socket::~Socket() noexcept { reset(); }

// Move constructor: Transfer ownership from 'other' to this object.
Socket::Socket(Socket &&o) noexcept : fd_(o.release()) {}

// ownership of 'other's fd.
//example usage:
//Socket s1(5);
//Socket s2;
//s2 = std::move(s1); // s1 is now invalid
Socket &Socket::operator=(Socket &&o) noexcept {
  if (this != &o)
    reset(o.release());
  return *this;
}
//return the current fd and set fd_ to -1, 
//effectively releasing ownership of the fd.
int Socket::release() noexcept { return std::exchange(fd_, -1); }
void Socket::reset(int fd) noexcept {
  if (fd_ >= 0)
    ::close(fd_);
  fd_ = fd;
}

} // namespace p2place::net
