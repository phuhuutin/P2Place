#include <gtest/gtest.h>

#include "core/consistent_hash_ring.hpp"
#include "core/membership_gossip.hpp"
#include "messages/payloads.hpp"

#include <algorithm>

using namespace p2place;

namespace {
core::NodeInfo node(core::NodeId id, std::uint16_t port,
                    core::NodeStatus status = core::NodeStatus::Alive,
                    const char* host = "127.0.0.1") {
    core::NodeInfo n;
    n.id = id;
    n.host = host;
    n.peer_port = port;
    n.ws_port = static_cast<std::uint16_t>(port + 1);
    n.status = status;
    return n;
}
}

TEST(Lab04Membership, IncludesLocalAndSorts) {
    core::PeerTable table;
    table.upsert(node(9, 9009));
    table.upsert(node(2, 9002));
    const auto all = core::live_membership(table, node(5, 9005));
    ASSERT_EQ(all.size(), 3u);
    EXPECT_EQ(all[0].id, 2u);
    EXPECT_EQ(all[1].id, 5u);
    EXPECT_EQ(all[2].id, 9u);
}

TEST(Lab04Membership, GossipDiscoversNewPeerAndPreservesHost) {
    core::PeerTable a;
    a.upsert(node(2, 9002, core::NodeStatus::Alive, "peer2.example"));
    const auto bview = messages::encode_nodelist({{
        node(2, 9002), node(3, 9003)
    }});
    const auto decoded = messages::decode_nodelist(bview);
    EXPECT_TRUE(core::merge_gossip(a, 1, decoded.peers));
    ASSERT_TRUE(a.find(3).has_value());
    EXPECT_TRUE(a.find(3)->host.empty()); // No host on the wire.
    EXPECT_EQ(a.find(2)->host, "peer2.example");
    EXPECT_FALSE(core::merge_gossip(a, 1, decoded.peers));
}

TEST(Lab04Membership, StaleGossipCannotResurrectDeadOrSuspect) {
    core::PeerTable a;
    a.upsert(node(2, 9002, core::NodeStatus::Dead));
    a.upsert(node(3, 9003, core::NodeStatus::Suspect));
    const auto incoming = std::vector<core::NodeInfo>{node(2, 9002), node(3, 9003)};
    EXPECT_FALSE(core::merge_gossip(a, 1, incoming));
    EXPECT_EQ(a.find(2)->status, core::NodeStatus::Dead);
    EXPECT_EQ(a.find(3)->status, core::NodeStatus::Suspect);
}

TEST(Lab04Membership, SameViewBuildsSameRingAfterGossip) {
    core::PeerTable a;
    core::PeerTable b;
    a.upsert(node(2, 9002));
    b.upsert(node(1, 9001));
    a.upsert(node(3, 9003));
    b.upsert(node(3, 9003));

    const auto message_to_a = messages::encode_nodelist({
        core::live_membership(b, node(2, 9002))
    });
    const auto message_to_b = messages::encode_nodelist({
        core::live_membership(a, node(1, 9001))
    });
    auto from_b = messages::decode_nodelist(message_to_a);
    auto from_a = messages::decode_nodelist(message_to_b);
    core::merge_gossip(a, 1, from_b.peers);
    core::merge_gossip(b, 2, from_a.peers);
    core::ConsistentHashRing ring_a, ring_b;
    ring_a.rebuild(core::live_membership(a, node(1, 9001)));
    ring_b.rebuild(core::live_membership(b, node(2, 9002)));
    for (std::uint32_t key : {0u, 15u, 127u, 255u}) {
        EXPECT_EQ(ring_a.coordinator(key), ring_b.coordinator(key));
        EXPECT_EQ(ring_a.successors(key, 3), ring_b.successors(key, 3));
    }
}

TEST(Lab04Membership, IgnoreSelfAndInvalidAndNonAliveGossip) {
    core::PeerTable a;
    EXPECT_FALSE(core::merge_gossip(a, 1,
        {node(1, 9001), node(0, 9000), node(4, 9004, core::NodeStatus::Dead), node(5, 0)}));
    EXPECT_TRUE(a.alive_set().empty());
}
