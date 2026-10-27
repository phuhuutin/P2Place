#include "net/protocol.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
namespace p2place::net {
std::vector<std::uint8_t> prefix_message(std::string_view body) {
  if (body.empty() || body.size() > MAX_MESSAGE_SIZE)
    throw std::length_error("invalid body length");
  auto len = htons(static_cast<std::uint16_t>(body.size()));
  std::vector<std::uint8_t> out(2 + body.size());
  std::memcpy(out.data(), &len, 2);
  std::memcpy(out.data() + 2, body.data(), body.size());
  return out;
}
bool sendFramed(int fd, std::string_view body) {
  const auto packet = prefix_message(body);
  std::size_t sent = 0;
  while (sent < packet.size()) {
    const auto n =
        ::send(fd, packet.data() + sent, packet.size() - sent, MSG_NOSIGNAL);
    if (n > 0) {
      sent += static_cast<std::size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR)
      continue;
    // TODO(L1.2): Observe EAGAIN in tests. NEVER retry this function on a
    // nonblocking socket after a partial send; TcpConnection must preserve
    // unsent bytes instead.
    return false;
  }
  return true;
}
} // namespace p2place::net
