#include <gtest/gtest.h>
#include "core/consistent_hash_ring.hpp"
#include "core/region_store.hpp"
#include "messages/payloads.hpp"
#include "messages/message.hpp"
#include <algorithm>
#include <set>

using namespace p2place;

TEST(Lab04Route, AllPeersChooseSameOwner) {
    std::vector<core::NodeInfo> peers;
    for (core::NodeId id : {1u,2u,3u}) {
        core::NodeInfo p; p.id=id; p.status=core::NodeStatus::Alive;
        peers.push_back(p);
    }
    core::ConsistentHashRing a,b;
    a.rebuild(peers);
    std::reverse(peers.begin(), peers.end());
    b.rebuild(peers);
    for (std::uint32_t key=0; key<core::RegionStore::REGION_COUNT; ++key) {
        ASSERT_TRUE(a.coordinator(key).has_value());
        EXPECT_EQ(a.coordinator(key),b.coordinator(key));
    }
}

TEST(Lab04Route, RegionCoordinatesRoundTrip) {
    for (std::uint16_t y : {0,15,16,128,255})
        for (std::uint16_t x : {0,15,16,128,255}) {
            const auto key=core::RegionStore::region_key(x,y);
            const auto index=core::RegionStore::pixel_index(x,y);
            EXPECT_EQ((key%16)*16+index%16,x);
            EXPECT_EQ((key/16)*16+index/16,y);
        }
}

TEST(Lab04Route, PutRequestAndPutOkCorrelation) {
    messages::Message request;
    request.type=messages::Type::PUT;
    request.request_id=321;
    request.payload=messages::encode_put({17,5,{0xabcdef,42,3}});
    const auto recovered=messages::decode(messages::encode(request));
    EXPECT_EQ(recovered.type,messages::Type::PUT);
    EXPECT_EQ(recovered.request_id,321u);
    const auto put=messages::decode_put(recovered.payload);
    EXPECT_EQ(put.key,17u);
    EXPECT_EQ(put.pixel.color,0xabcdefu);
    messages::Message response;
    response.type=messages::Type::PUT_OK;
    response.flags=messages::IS_RESPONSE;
    response.request_id=recovered.request_id;
    response.payload=messages::encode_put_ok({put.key,put.pixel.lamport});
    auto answer=messages::decode(messages::encode(response));
    EXPECT_EQ(answer.request_id,request.request_id);
    EXPECT_TRUE(answer.flags & messages::IS_RESPONSE);
    EXPECT_EQ(messages::decode_put_ok(answer.payload).key,17u);
}

TEST(Lab04Route, GetOkReturnsCompleteRegionSnapshot) {
    core::Region region;
    ASSERT_TRUE(region.set_pixel(7,{0x123456,10,1}));
    const auto payload=messages::encode_region({15,region});
    EXPECT_EQ(payload.size(),4100u);
    auto decoded=messages::decode_region(payload);
    EXPECT_EQ(decoded.key,15u);
    EXPECT_EQ(decoded.region.pixel(7).color,0x123456u);
}
