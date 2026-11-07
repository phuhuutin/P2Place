#include <gtest/gtest.h>
#include "net/protocol_codec.hpp"
#include "core/peer_table.hpp"
#include "net/event_loop.hpp"
#include "net/tcp_client.hpp"
#include "net/tcp_connection.hpp"
#include "net/tcp_server.hpp"
#include "net/socket.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <vector>
using namespace p2place::core;

TEST(Lab02Codec, AppendFrame) {
    p2place::net::ProtocolCodec codec;

    std::vector<std::uint8_t> out;
    std::vector<std::uint8_t> body = {'A', 'B', 'C'};

    ASSERT_TRUE(codec.append_frame(out, body));

    std::vector<std::uint8_t> expected = {
        0x00, 0x03, 0x41, 0x42, 0x43
    };

    EXPECT_EQ(out, expected);
}
TEST(Lab02Codec, ExtractFrame) {
    p2place::net::ProtocolCodec codec;

    std::vector<std::uint8_t> in = {
        0x00, 0x03, 0x41, 0x42, 0x43
    };

    std::vector<std::uint8_t> body;
    ASSERT_TRUE(codec.extract_frame(in, body));
    std::vector<std::uint8_t> expected_body = {'A', 'B', 'C'};
    EXPECT_EQ(body, expected_body);
    EXPECT_TRUE(in.empty());
}
TEST(Lab02Codec, IncompleteFrame) {
    p2place::net::ProtocolCodec codec;

    std::vector<std::uint8_t> in = {
        0x00, 0x03, 0x41
    };

    std::vector<std::uint8_t> body;

    // TODO 1: Verify extract_frame() returns false.
    ASSERT_FALSE(codec.extract_frame(in, body));

    // TODO 2: Verify all 3 input bytes remain unchanged.
    std::vector<std::uint8_t> expected_in = {
        0x00, 0x03, 0x41
    };
    EXPECT_EQ(in, expected_in);

    // TODO 3: Append the missing bytes 0x42 and 0x43.
    in.push_back(0x42);
    in.push_back(0x43);

    // TODO 4: Verify extract_frame() now succeeds.
    ASSERT_TRUE(codec.extract_frame(in, body));
}
TEST(Lab02Peers, InsertAndFind) {
    PeerTable table;

    table.upsert({
        1, "127.0.0.1", 9000, 9100,
        NodeStatus::Alive
    });

    auto peer = table.find(1);

    ASSERT_TRUE(peer.has_value());
    EXPECT_EQ(peer->id, 1u);
    EXPECT_EQ(peer->peer_port, 9000);
    EXPECT_EQ(peer->status, NodeStatus::Alive);
}

TEST(Lab02Peers, UnknownPeer) {
    PeerTable table;

    EXPECT_FALSE(table.find(99).has_value());
}

TEST(Lab02Peers, MarkDead) {
    PeerTable table;

    table.upsert({
        2, "127.0.0.1", 9002, 9102,
        NodeStatus::Alive
    });

    table.mark(2, NodeStatus::Dead);

    auto peer = table.find(2);

    ASSERT_TRUE(peer.has_value());
    EXPECT_EQ(peer->status, NodeStatus::Dead);

    EXPECT_TRUE(table.alive_set().empty());
}

TEST(Lab02Peers, SortedAliveSet) {
    PeerTable table;

    table.upsert({3, "localhost", 9003, 9103,
                  NodeStatus::Alive});

    table.upsert({1, "localhost", 9001, 9101,
                  NodeStatus::Alive});

    table.upsert({2, "localhost", 9002, 9102,
                  NodeStatus::Dead});

    auto alive = table.alive_set();

    ASSERT_EQ(alive.size(), 2u);
    EXPECT_EQ(alive[0].id, 1u);
    EXPECT_EQ(alive[1].id, 3u);
}

// ============================================================
// Lab 02 epoll: Solution A (epoll_event.data.fd) verification
// ============================================================

using namespace std::chrono_literals;

namespace {

void write_all(int fd, std::span<const std::uint8_t> data) {
    std::size_t off = 0;
    while (off < data.size()) {
        const ssize_t n = ::write(fd, data.data() + off, data.size() - off);
        if (n > 0) {
            off += static_cast<std::size_t>(n);
            continue;
        }
        if (n == -1 && errno == EINTR) {
            continue;
        }
        if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            struct pollfd p{fd, POLLOUT, 0};
            if (::poll(&p, 1, 10000) <= 0) {
                throw std::runtime_error("write_all: poll timeout");
            }
            continue;
        }
        throw std::runtime_error("write_all: write failed");
    }
}

std::vector<std::uint8_t> read_exact(int fd, std::size_t count) {
    std::vector<std::uint8_t> out;
    out.reserve(count);
    while (out.size() < count) {
        struct pollfd p{fd, POLLIN, 0};
        if (::poll(&p, 1, 10000) <= 0) {
            throw std::runtime_error("read_exact: poll timeout");
        }
        std::uint8_t buffer[4096];
        const std::size_t want = count - out.size();
        const ssize_t n = ::read(
            fd,
            buffer,
            want < sizeof(buffer) ? want : sizeof(buffer)
        );
        if (n > 0) {
            out.insert(out.end(), buffer, buffer + n);
            continue;
        }
        if (n == 0) {
            throw std::runtime_error("read_exact: EOF");
        }
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            continue;
        }
        throw std::runtime_error("read_exact: read failed");
    }
    return out;
}

std::vector<std::uint8_t> read_frame(int fd) {
    const auto header = read_exact(fd, 2);
    const std::size_t len =
        (static_cast<std::size_t>(header[0]) << 8) | header[1];
    return read_exact(fd, len);
}

bool wait_until(
    const std::function<bool()>& cond,
    std::chrono::milliseconds timeout = 5s
) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (cond()) {
            return true;
        }
        std::this_thread::sleep_for(20ms);
    }
    return cond();
}

// Runs an EventLoop in a background thread; stops and joins it when
// the guard goes out of scope, so failed ASSERTs never hang the test.
struct LoopRunner {
    p2place::net::EventLoop& loop;
    std::thread thread;

    explicit LoopRunner(p2place::net::EventLoop& l)
        : loop(l), thread([&l] { l.run({}); }) {}

    ~LoopRunner() {
        loop.stop();
        thread.join();
    }
};

} // namespace

// 1. fd round trip: EPOLL_CTL_ADD stores data.fd and the event
// dispatched by epoll_wait() reaches the connection registered
// under that fd.
TEST(Lab02Epoll, FdRoundTripReturnsDataFd) {
    using namespace p2place::net;
    int fds[2];
    ASSERT_EQ(::socketpair(
        AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds
    ), 0);
    Socket peer(fds[1]);
    const int conn_fd = fds[0];

    std::atomic<int> handled{0};
    std::vector<std::vector<std::uint8_t>> received;
    EventLoop loop;
    loop.add(std::make_unique<TcpConnection>(
        Socket(fds[0]),
        std::make_unique<ProtocolCodec>(),
        [&](TcpConnection&, std::span<const std::uint8_t> body) {
            received.push_back({body.begin(), body.end()});
            handled.store(1);
            loop.stop();
        }
    ));

    {
        LoopRunner runner(loop);

        ProtocolCodec codec;
        std::vector<std::uint8_t> frame;
        ASSERT_TRUE(codec.append_frame(
            frame, std::vector<std::uint8_t>{'h', 'i'}
        ));
        write_all(peer.get(), frame);

        // If the EPOLLIN event were keyed by anything other than
        // data.fd, the handler would never run.
        ASSERT_TRUE(wait_until([&] { return handled.load() != 0; }))
            << "event for fd " << conn_fd << " was not dispatched";
    }

    ASSERT_EQ(handled.load(), 1);
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0], (std::vector<std::uint8_t>{'h', 'i'}));
}

// 2. MOD preservation: when try_flush() re-arms EPOLLOUT through
// EPOLL_CTL_MOD, the events it gets back must still carry data.fd.
// A frame larger than both socket buffers forces a partial send
// (EAGAIN), the MOD, and a resumed flush on EPOLLOUT.
TEST(Lab02Epoll, ModPreservesFdOnPartialSend) {
    using namespace p2place::net;
    int fds[2];
    ASSERT_EQ(::socketpair(
        AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds
    ), 0);
    for (int fd : {fds[0], fds[1]}) {
        const int small = 2048;  // kernel doubles to 4096
        ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &small, sizeof(small));
        ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
    }
    Socket peer(fds[1]);
    const int conn_fd = fds[0];

    EventLoop loop;
    loop.add(std::make_unique<TcpConnection>(
        Socket(fds[0]),
        std::make_unique<ProtocolCodec>(),
        [](TcpConnection&, std::span<const std::uint8_t>) {}
    ));

    const std::vector<std::uint8_t> small_body{'a'};
    // 12 KiB is within MAX_MESSAGE_SIZE (16 KiB) but far larger than
    // the 4 KiB effective socket buffers.
    const std::vector<std::uint8_t> big_body(12 * 1024, 'b');

    {
        LoopRunner runner(loop);

        // Fits entirely: flushed at once, no EPOLLOUT needed.
        loop.send(conn_fd, small_body);

        // Cannot fit in the shrunken buffers: EAGAIN -> try_flush()
        // must EPOLL_CTL_MOD in EPOLLOUT with data.fd = conn_fd.
        // The EPOLLOUT event then resumes the flush as the test
        // drains the peer side. If the MOD stored anything else
        // (zero, a pointer), the event would be dropped and the
        // frame would never arrive.
        loop.send(conn_fd, big_body);

        const auto f1 = read_frame(peer.get());
        const auto f2 = read_frame(peer.get());
        EXPECT_EQ(f1, small_body);
        EXPECT_EQ(f2, big_body);
    }
}

// 3. Listener dispatch + outgoing connect. On some kernels loopback
// connect() completes immediately (Connected), on others it reports
// EINPROGRESS even against a live listener; both routes must end in
// an established connection that can exchange framed messages.
TEST(Lab02Epoll, ListenerDispatchAndImmediateConnect) {
    using namespace p2place::net;
    TcpServer server(0);
    sockaddr_in bound{};
    socklen_t blen = sizeof(bound);
    ASSERT_EQ(::getsockname(
        server.fd(), reinterpret_cast<sockaddr*>(&bound), &blen
    ), 0);
    const std::uint16_t port = ntohs(bound.sin_port);

    std::atomic<int> handled{0};
    std::atomic<bool> pending_error{false};
    std::vector<std::vector<std::uint8_t>> received;
    EventLoop loop;

    // Install an established socket as a TcpConnection and send the
    // "HELLO" frame through the loop (shared by the accepted and the
    // completed-outgoing routes, like PeerNode::on_accepted).
    const auto install = [&](Socket established) {
        const int fd = established.get();
        loop.add(std::make_unique<TcpConnection>(
            std::move(established),
            std::make_unique<ProtocolCodec>(),
            [&](TcpConnection&, std::span<const std::uint8_t> body) {
                received.push_back({body.begin(), body.end()});
                handled.fetch_add(1);
            }
        ));
        const std::vector<std::uint8_t> hello{'h', 'e', 'l', 'l', 'o'};
        loop.send(fd, hello);
    };

    loop.register_listener(server.fd(), [&] {
        server.accept_ready([&](Socket client) {
            install(std::move(client));
        });
    });

    // Connect before the loop runs so the test thread never mutates
    // the loop's connection table concurrently with dispatch.
    auto result = TcpClient::connect_to("127.0.0.1", port);

    if (result.status == ConnectStatus::Connected) {
        // Already established: install directly.
        install(std::move(result.socket));
    } else {
        ASSERT_EQ(result.status, ConnectStatus::InProgress);
        // Let the event loop finish the connect; install the
        // connection and send HELLO only on successful completion.
        loop.add_pending_connection(
            std::move(result.socket),
            [&](Socket established) { install(std::move(established)); },
            [&](int) { pending_error.store(true); }
        );
    }

    {
        LoopRunner runner(loop);

        // Both sides install and exchange a HELLO, like two
        // PeerNodes would: the server records the client's HELLO
        // (and the client records the server's).
        ASSERT_TRUE(wait_until([&] { return handled.load() >= 1; }))
            << "listener event or accept callback never fired";
    }

    EXPECT_FALSE(pending_error.load());
    ASSERT_GE(received.size(), 1u);
    for (const auto& body : received) {
        EXPECT_EQ(body, (std::vector<std::uint8_t>{'h', 'e', 'l', 'l', 'o'}));
    }
}

// 4a. Pending machinery: EPOLLOUT -> SO_ERROR == 0 -> on_connected ->
// established TcpConnection -> framed message. Uses an already
// connected socketpair fd so the completion path runs deterministically.
TEST(Lab02Epoll, PendingCompletionPath) {
    using namespace p2place::net;
    int fds[2];
    ASSERT_EQ(::socketpair(
        AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds
    ), 0);
    Socket peer(fds[1]);

    std::atomic<bool> connected{false};
    std::atomic<bool> errored{false};
    EventLoop loop;
    loop.add_pending_connection(
        Socket(fds[0]),
        [&](Socket established) {
            const int fd = established.get();
            connected.store(true);
            loop.add(std::make_unique<TcpConnection>(
                std::move(established),
                std::make_unique<ProtocolCodec>(),
                [](TcpConnection&, std::span<const std::uint8_t>) {}
            ));
            // "HELLO" is only sent once SO_ERROR reported success.
            const std::vector<std::uint8_t> hello{'h', 'e', 'l', 'l', 'o'};
            loop.send(fd, hello);
        },
        [&](int) { errored.store(true); }
    );

    {
        LoopRunner runner(loop);

        const auto frame = read_frame(peer.get());
        EXPECT_EQ(
            frame,
            (std::vector<std::uint8_t>{'h', 'e', 'l', 'l', 'o'})
        );
        ASSERT_TRUE(wait_until([&] { return connected.load(); }));
    }

    EXPECT_FALSE(errored.load());
}

// 4b. Real EINPROGRESS: fill a one-slot accept queue so the next
// connect() must return EINPROGRESS, then drain the queue and verify
// the pending connection completes via EPOLLOUT + SO_ERROR.
TEST(Lab02Epoll, PendingConnectCompletesWithSoError) {
    using namespace p2place::net;
    const int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(lfd, 0);
    sockaddr_in la{};
    la.sin_family = AF_INET;
    la.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(lfd, reinterpret_cast<sockaddr*>(&la), sizeof(la)), 0);
    ASSERT_EQ(::listen(lfd, 1), 0);
    socklen_t blen = sizeof(la);
    ASSERT_EQ(::getsockname(
        lfd, reinterpret_cast<sockaddr*>(&la), &blen
    ), 0);
    const std::uint16_t port = ntohs(la.sin_port);

    // Fill the only accept-queue slot.
    const int c1 = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(c1, 0);
    ASSERT_EQ(::connect(c1, reinterpret_cast<sockaddr*>(&la), sizeof(la)), 0);

    // Queue full: the next SYN is dropped, so connect() reports
    // EINPROGRESS and completion must come from the event loop.
    auto result = TcpClient::connect_to("127.0.0.1", port);
    if (result.status != ConnectStatus::InProgress) {
        ::close(c1);
        ::close(lfd);
        GTEST_SKIP() << "kernel completed the connect despite full backlog";
    }

    std::atomic<bool> connected{false};
    std::atomic<bool> errored{false};
    EventLoop loop;
    loop.add_pending_connection(
        std::move(result.socket),
        [&](Socket established) {
            const int fd = established.get();
            connected.store(true);
            loop.add(std::make_unique<TcpConnection>(
                std::move(established),
                std::make_unique<ProtocolCodec>(),
                [](TcpConnection&, std::span<const std::uint8_t>) {}
            ));
            const std::vector<std::uint8_t> hello{'h', 'e', 'l', 'l', 'o'};
            loop.send(fd, hello);
        },
        [&](int) { errored.store(true); }
    );

    int a2 = -1;
    {
        LoopRunner runner(loop);

        // Drain the queue so the pending SYN can complete.
        const int a1 = ::accept(lfd, nullptr, nullptr);
        ASSERT_GE(a1, 0);
        ::close(a1);

        struct pollfd p{lfd, POLLIN, 0};
        ASSERT_GT(::poll(&p, 1, 10000), 0)
            << "pending connect never completed";
        a2 = ::accept(lfd, nullptr, nullptr);
        ASSERT_GE(a2, 0);

        const auto frame = read_frame(a2);
        EXPECT_EQ(
            frame,
            (std::vector<std::uint8_t>{'h', 'e', 'l', 'l', 'o'})
        );
        ASSERT_TRUE(wait_until([&] { return connected.load(); }));
    }

    EXPECT_FALSE(errored.load());
    ::close(a2);
    ::close(c1);
    ::close(lfd);
}

// 5. Refused connect: TcpClient must report the failure and no
// connection object may be created / nothing sent. Some kernels fail
// the loopback connect synchronously with ECONNREFUSED; others only
// report EINPROGRESS first, in which case completion must come from
// EPOLLOUT/EPOLLERR + SO_ERROR and the error callback must fire
// (never the success callback).
TEST(Lab02Epoll, RefusedConnectThrowsAndSendsNothing) {
    using namespace p2place::net;
    const int tmp = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(tmp, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(::bind(tmp, reinterpret_cast<sockaddr*>(&a), sizeof(a)), 0);
    socklen_t alen = sizeof(a);
    ASSERT_EQ(::getsockname(
        tmp, reinterpret_cast<sockaddr*>(&a), &alen
    ), 0);
    const std::uint16_t port = ntohs(a.sin_port);
    ::close(tmp);  // nothing listens here anymore

    ConnectResult result;  // only valid when !threw
    bool threw = false;
    try {
        result = TcpClient::connect_to("127.0.0.1", port);
    } catch (const std::system_error& e) {
        threw = true;
        EXPECT_EQ(e.code().value(), ECONNREFUSED);
    }

    if (threw) {
        // Synchronous refusal: nothing to clean up.
        return;
    }

    ASSERT_NE(result.status, ConnectStatus::Connected)
        << "connect to a closed port cannot be established";

    // Asynchronous refusal: EPOLLOUT/EPOLLERR -> SO_ERROR != 0 ->
    // error callback, no connection, no HELLO.
    std::atomic<bool> connected{false};
    std::atomic<bool> errored{false};
    std::atomic<int> error{0};
    EventLoop loop;
    loop.add_pending_connection(
        std::move(result.socket),
        [&](Socket) { connected.store(true); },
        [&](int e) { errored.store(true); error.store(e); }
    );

    {
        LoopRunner runner(loop);
        ASSERT_TRUE(wait_until([&] {
            return connected.load() || errored.load();
        })) << "pending refused connect never resolved";
    }

    EXPECT_FALSE(connected.load());
    ASSERT_TRUE(errored.load());
    EXPECT_EQ(error.load(), ECONNREFUSED);
}

// 6. Partial reads: split frames and batched frames must be
// extracted exactly once each, in order.
TEST(Lab02Epoll, SplitAndBatchedFrames) {
    using namespace p2place::net;
    int fds[2];
    ASSERT_EQ(::socketpair(
        AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds
    ), 0);
    Socket peer(fds[1]);

    std::atomic<int> handled{0};
    std::vector<std::vector<std::uint8_t>> received;
    EventLoop loop;
    loop.add(std::make_unique<TcpConnection>(
        Socket(fds[0]),
        std::make_unique<ProtocolCodec>(),
        [&](TcpConnection&, std::span<const std::uint8_t> body) {
            received.push_back({body.begin(), body.end()});
            handled.fetch_add(1);
        }
    ));

    ProtocolCodec codec;
    std::vector<std::uint8_t> f1, f2;
    ASSERT_TRUE(codec.append_frame(f1, std::vector<std::uint8_t>{1, 2, 3, 4, 5}));
    ASSERT_TRUE(codec.append_frame(f2, std::vector<std::uint8_t>{6, 7, 8}));

    {
        LoopRunner runner(loop);

        // 2-byte header + half of frame 1's body only.
        write_all(peer.get(),
                  std::vector<std::uint8_t>(f1.begin(), f1.begin() + 4));
        std::this_thread::sleep_for(100ms);
        ASSERT_EQ(handled.load(), 0)
            << "incomplete frame must not be dispatched";

        // Rest of frame 1 plus all of frame 2 in one write.
        std::vector<std::uint8_t> rest(f1.begin() + 4, f1.end());
        rest.insert(rest.end(), f2.begin(), f2.end());
        write_all(peer.get(), rest);

        ASSERT_TRUE(wait_until([&] { return handled.load() == 2; }))
            << "complete frames were not dispatched";
    }

    ASSERT_EQ(received.size(), 2u);
    EXPECT_EQ(received[0], (std::vector<std::uint8_t>{1, 2, 3, 4, 5}));
    EXPECT_EQ(received[1], (std::vector<std::uint8_t>{6, 7, 8}));
}

// 7. Safe removal: a handler that throws removes its connection
// without crashing the loop, and a fresh connection (whose fd may
// reuse the dead one) dispatches correctly afterwards.
TEST(Lab02Epoll, HandlerErrorRemovesConnectionSafely) {
    using namespace p2place::net;
    int fds1[2];
    ASSERT_EQ(::socketpair(
        AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds1
    ), 0);
    Socket peer1(fds1[1]);
    const int fd1 = fds1[0];

    std::atomic<int> handled{0};
    std::vector<std::vector<std::uint8_t>> received;
    EventLoop loop;
    loop.add(std::make_unique<TcpConnection>(
        Socket(fds1[0]),
        std::make_unique<ProtocolCodec>(),
        [](TcpConnection&, std::span<const std::uint8_t>) {
            throw std::runtime_error("handler failed");
        }
    ));

    ProtocolCodec codec;
    std::vector<std::uint8_t> frame1;
    ASSERT_TRUE(codec.append_frame(frame1, std::vector<std::uint8_t>{1}));

    {
        LoopRunner runner(loop);

        write_all(peer1.get(), frame1);

        // Removal is deferred to the end of the dispatch batch;
        // afterwards EventLoop::send must report the fd as gone.
        ASSERT_TRUE(wait_until([&] {
            try {
                const std::vector<std::uint8_t> body{2};
                loop.send(fd1, body);
                return false;
            } catch (const std::runtime_error&) {
                return true;  // "connection not found"
            }
        })) << "throwing handler did not remove the connection";

        // A brand-new connection must still dispatch correctly even
        // if the kernel reuses fd1's number for it.
        int fds2[2];
        ASSERT_EQ(::socketpair(
            AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, fds2
        ), 0);
        Socket peer2(fds2[1]);
        loop.add(std::make_unique<TcpConnection>(
            Socket(fds2[0]),
            std::make_unique<ProtocolCodec>(),
            [&](TcpConnection&, std::span<const std::uint8_t> body) {
                received.push_back({body.begin(), body.end()});
                handled.store(1);
                loop.stop();
            }
        ));

        std::vector<std::uint8_t> frame2;
        ASSERT_TRUE(codec.append_frame(frame2, std::vector<std::uint8_t>{9}));
        write_all(peer2.get(), frame2);

        ASSERT_TRUE(wait_until([&] { return handled.load() != 0; }))
            << "connection after handler-throw removal did not dispatch";
    }

    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0], (std::vector<std::uint8_t>{9}));
}