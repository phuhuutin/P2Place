#include <gtest/gtest.h>
#include "core/region.hpp"
#include "core/region_store.hpp"
#include "peer/peer_node.hpp"
#include "net/event_loop.hpp"
#include <chrono>
#include <stdexcept>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
using namespace p2place;
TEST(Lab03Region, MergeIndependentPixels) {
    core::Region a, b;

    a.set_pixel(5, {0xFF0000, 10, 1});
    b.set_pixel(10, {0x0000FF, 11, 2});

    EXPECT_EQ(a.merge(b), 1u);
    EXPECT_EQ(a.merge(b), 0u);

    EXPECT_EQ(a.pixel(5).color, 0xFF0000u);
    EXPECT_EQ(a.pixel(10).color, 0x0000FFu);
    EXPECT_EQ(a.version(), 11u);
}
TEST(Lab03Store, ApplyAndRetrieve) {
    core::RegionStore store;

    auto key = core::RegionStore::region_key(20, 5);
    auto index = core::RegionStore::pixel_index(20, 5);

    EXPECT_EQ(key, 1u);
    EXPECT_EQ(index, 84u);

    EXPECT_TRUE(store.apply_pixel(
        key, index, {0xFF0000, 10, 1}
    ));

    auto region = store.get_region(key);

    ASSERT_TRUE(region.has_value());
    EXPECT_EQ(region->pixel(index).color, 0xFF0000u);

    EXPECT_EQ(store.all_keys().size(), 1u);
}

TEST(EventLoop, MultipleListenerRegistration) {
    net::EventLoop loop;

    int peer_calls = 0;
    int browser_calls = 0;

    // Port 0 lets the OS assign free ports.
    net::TcpServer peer_server(0);
    net::TcpServer browser_server(0);

    loop.register_listener(
        peer_server.fd(),
        [&] {
            ++peer_calls;
            peer_server.accept_ready([](net::Socket socket) {
                // Accepted socket closes via RAII.
            });
        }
    );

    loop.register_listener(
        browser_server.fd(),
        [&] {
            ++browser_calls;
            browser_server.accept_ready([](net::Socket socket) {
            });
        }
    );

    // Connect one client to each listener.
    auto connect_client = [](int listener_fd) -> net::Socket {
        sockaddr_in address{};
        socklen_t length = sizeof(address);

        if (::getsockname(
                listener_fd,
                reinterpret_cast<sockaddr*>(&address),
                &length
            ) != 0) {
            throw std::runtime_error("getsockname failed");
        }

        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            throw std::runtime_error("socket failed");
        }

        net::Socket client(fd);

        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

        if (::connect(
                fd,
                reinterpret_cast<sockaddr*>(&address),
                sizeof(address)
            ) != 0) {
            throw std::runtime_error("connect failed");
        }

        return client;
    };

    auto peer_client = connect_client(peer_server.fd());
    auto browser_client = connect_client(browser_server.fd());

    // Run until both callbacks execute, with a timeout.
    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds(2);

    loop.run([&] {
        if ((peer_calls >= 1 && browser_calls >= 1) ||
            std::chrono::steady_clock::now() >= deadline) {
            loop.stop();
        }
    });

    EXPECT_EQ(peer_calls, 1);
    EXPECT_EQ(browser_calls, 1);
}