#include <gtest/gtest.h>
#include "core/region.hpp"
#include "core/region_store.hpp"
#include "peer/peer_node.hpp"
#include "net/event_loop.hpp"
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

    // Use two valid, nonblocking listening sockets.
    // For example, port 9000 and port 9001.
    net::TcpServer peer_server(9000);
    net::TcpServer browser_server(9001);

    loop.register_listener(
        peer_server.fd(),
        [&] {
            ++peer_calls;

            peer_server.accept_ready(
                [](net::Socket socket) {
                    // Socket closes via RAII.
                }
            );
        }
    );

    loop.register_listener(
        browser_server.fd(),
        [&] {
            ++browser_calls;

            browser_server.accept_ready(
                [](net::Socket socket) {
                    // Socket closes via RAII.
                }
            );
        }
    );

    // TODO: Launch loop, connect one client to
    // each listener, and stop the loop.

    EXPECT_EQ(peer_calls, 1);
    EXPECT_EQ(browser_calls, 1);
}