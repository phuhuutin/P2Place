#include <gtest/gtest.h>
#include "messages/payloads.hpp"
#include "messages/message.hpp"

#include <algorithm>
#include <array>
#include <stdexcept>
#include <vector>

using namespace p2place;

TEST(Lab04Wire, BigEndianPutRoundTrip) {
    const messages::PutPayload p{7, 9, {0xAABBCCDD, 123456789u, 4}};
    const auto data = messages::encode_put(p);
    ASSERT_EQ(data.size(), 22u);
    EXPECT_EQ((std::vector<std::uint8_t>(data.begin(), data.begin() + 10)),
              (std::vector<std::uint8_t>{0,0,0,7,0,9,0xAA,0xBB,0xCC,0xDD}));
    const auto q = messages::decode_put(data);
    EXPECT_EQ(q.key, p.key);
    EXPECT_EQ(q.pixel_index, p.pixel_index);
    EXPECT_EQ(q.pixel.color, p.pixel.color);
    EXPECT_EQ(q.pixel.lamport, p.pixel.lamport);
    EXPECT_EQ(q.pixel.node, p.pixel.node);
}

TEST(Lab04Wire, PutRejectsBadLengthsAndIndex) {
    const auto data = messages::encode_put({7,9,{0xFF,42,4}});
    auto short_data = data;
    short_data.pop_back();
    EXPECT_THROW(messages::decode_put(short_data), std::invalid_argument);
    auto long_data = data;
    long_data.push_back(0);
    EXPECT_THROW(messages::decode_put(long_data), std::invalid_argument);
    auto bad_index = data;
    bad_index[4] = 1;
    bad_index[5] = 0; // 256
    EXPECT_THROW(messages::decode_put(bad_index), std::invalid_argument);
    EXPECT_THROW((messages::encode_put({7,256,{}})), std::invalid_argument);
}

TEST(Lab04Wire, GetAndPutOkRoundTrip) {
    const auto get = messages::encode_get({0x10203040});
    EXPECT_EQ(get, (std::vector<std::uint8_t>{0x10,0x20,0x30,0x40}));
    EXPECT_EQ(messages::decode_get(get).key, 0x10203040u);
    EXPECT_THROW(messages::decode_get({}), std::invalid_argument);
    const auto ok = messages::encode_put_ok({12,987654321ULL});
    EXPECT_EQ(ok.size(), 12u);
    const auto decoded = messages::decode_put_ok(ok);
    EXPECT_EQ(decoded.key, 12u);
    EXPECT_EQ(decoded.applied_version, 987654321ULL);
}

TEST(Lab04Wire, RegionRoundTripTwoPixels) {
    messages::RegionPayload original;
    original.key = 127;
    ASSERT_TRUE(original.region.set_pixel(0, {0x11223344, 8, 1}));
    ASSERT_TRUE(original.region.set_pixel(255, {0xAABBCCDD, 9, 2}));
    const auto bytes = messages::encode_region(original);
    ASSERT_EQ(bytes.size(), 4100u);
    const auto decoded = messages::decode_region(bytes);
    EXPECT_EQ(decoded.key, 127u);
    EXPECT_EQ(decoded.region.pixel(0).color, 0x11223344u);
    EXPECT_EQ(decoded.region.pixel(0).lamport, 8u);
    EXPECT_EQ(decoded.region.pixel(0).node, 1u);
    EXPECT_EQ(decoded.region.pixel(255).color, 0xAABBCCDDu);
    EXPECT_EQ(decoded.region.pixel(255).lamport, 9u);
    EXPECT_EQ(decoded.region.pixel(255).node, 2u);
    EXPECT_EQ(messages::encode_region(decoded), bytes);
    auto truncated = bytes;
    truncated.pop_back();
    EXPECT_THROW(messages::decode_region(truncated), std::invalid_argument);
}

TEST(Lab04Wire, NodeListRoundTripAndValidation) {
    messages::NodeListPayload p;
    p.peers = {{1,"peer1",9000,9001,core::NodeStatus::Alive},
               {2,"peer2",9010,9011,core::NodeStatus::Suspect}};
    const auto bytes = messages::encode_nodelist(p);
    ASSERT_EQ(bytes.size(), 20u);
    const auto decoded = messages::decode_nodelist(bytes);
    ASSERT_EQ(decoded.peers.size(), 2u);
    EXPECT_EQ(decoded.peers[0].id, 1u);
    EXPECT_EQ(decoded.peers[0].peer_port, 9000u);
    EXPECT_EQ(decoded.peers[1].ws_port, 9011u);
    EXPECT_EQ(decoded.peers[1].status, core::NodeStatus::Suspect);
    auto truncated = bytes;
    truncated.pop_back();
    EXPECT_THROW(messages::decode_nodelist(truncated), std::invalid_argument);
    auto invalid_status = bytes;
    invalid_status.back() = 99;
    EXPECT_THROW(messages::decode_nodelist(invalid_status), std::invalid_argument);
}

TEST(Lab04Wire, SyncReqRoundTrip) {
    const auto bytes = messages::encode_sync_req({5, 1000});
    ASSERT_EQ(bytes.size(), 12u);
    const auto decoded = messages::decode_sync_req(bytes);
    EXPECT_EQ(decoded.key, 5u);
    EXPECT_EQ(decoded.my_version, 1000u);
}

TEST(Lab04Wire, ExistingMessageEnvelopeCompatible) {
    messages::Message msg;
    msg.type = messages::Type::PUT;
    msg.request_id = 42;
    msg.payload = messages::encode_put({7, 9, {0xAABBCCDD, 123456789, 4}});
    const auto bytes = messages::encode(msg);
    EXPECT_EQ(bytes.size(), 28u); // 6-byte message envelope + 22-byte payload
    const auto decoded = messages::decode(bytes);
    EXPECT_EQ(decoded.request_id, 42u);
    EXPECT_EQ(messages::decode_put(decoded.payload).pixel.color, 0xAABBCCDDu);
}
