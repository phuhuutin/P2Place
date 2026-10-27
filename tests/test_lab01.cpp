#include "net/protocol.hpp"
#include "net/socket.hpp"
#include <gtest/gtest.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
using namespace p2place::net;
TEST(Lab01Socket, MoveAndRelease) {
  int fds[2];
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  Socket original(fds[0]);
  Socket moved(std::move(original));
  EXPECT_EQ(original.get(), -1);
  EXPECT_EQ(moved.get(), fds[0]);
  const int released = moved.release();
  EXPECT_EQ(released, fds[0]);
  EXPECT_EQ(moved.get(), -1);
  EXPECT_EQ(::close(released), 0);
  ::close(fds[1]);
}
TEST(Lab01Framing, HeaderLengthsAndLimits) {
  for (std::size_t n : {std::size_t(126), std::size_t(127), std::size_t(300),
                        std::size_t(4096), std::size_t(16384)}) {
    const auto packet = prefix_message(std::string(n, 'x'));
    ASSERT_EQ(packet.size(), n + 2);
    EXPECT_EQ((unsigned(packet[0]) << 8) | packet[1], n);
  }
  EXPECT_THROW(prefix_message(""), std::length_error);
  EXPECT_THROW(prefix_message(std::string(16385, 'x')), std::length_error);
}
TEST(Lab01Framing, SocketPairSendsFullFrame) {
  int fds[2];
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  ASSERT_TRUE(sendFramed(fds[0], "hello"));
  char got[7];
  ASSERT_EQ(::recv(fds[1], got, sizeof(got), MSG_WAITALL), 7);
  EXPECT_EQ((unsigned(static_cast<unsigned char>(got[0])) << 8) |
                static_cast<unsigned char>(got[1]),
            5u);
  EXPECT_EQ(std::string(got + 2, 5), "hello");
  ::close(fds[0]);
  ::close(fds[1]);
}
