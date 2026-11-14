#pragma once
#include "core/consistent_hash_ring.hpp"
#include "core/region_store.hpp"
#include "messages/message.hpp"
#include "net/timer_queue.hpp"
#include "replication/replication_strategy.hpp"
#include <functional>
#include <map>
#include <set>
#include <string>

namespace p2place::replication {
// Strategy A fixed quorum contract; do not silently downgrade N, R or W.
// Async coordinator; one instance per peer, owned by PeerNode.
// All methods execute on the EventLoop thread. send() enqueues only;
// it NEVER counts as an ACK. Local replica follows the same response path.
class ReplicationManager final {
public:
    using NodeId = core::NodeId;
    using Send = std::function<bool(NodeId,const messages::Message&)>;
    using Schedule = std::function<net::TimerQueue::Token(std::function<void()>)>;
    using Cancel = std::function<void(net::TimerQueue::Token)>;
    using PutResult = std::function<void(bool,const std::string&)>;
    using GetResult = std::function<void(bool,const core::Region&,const std::string&)>;

    ReplicationManager(NodeId local, core::ConsistentHashRing& ring,
        core::RegionStore& store, const ReplicationStrategy& strategy,
        Send send, Schedule schedule, Cancel cancel);
    std::uint32_t put(std::uint32_t key,std::uint16_t index,
        core::VersionedPixel pixel,PutResult callback);
    std::uint32_t get(std::uint32_t key,GetResult callback);
    void on_message(NodeId sender,const messages::Message& msg);
    void on_disconnect(NodeId sender);
    void cancel_all(const std::string& reason);
    std::size_t pending_count() const noexcept { return pending_.size(); }
    QuorumConfig config(std::uint32_t key) const {return strategy_.plan(key);}
private:
    struct Pending {
        bool read=false;
        std::uint32_t key=0;
        std::size_t required=0;
        std::set<NodeId> expected, responded, failed;
        std::map<NodeId,core::Region> versions;
        net::TimerQueue::Token timer=0;
        PutResult put_callback;
        GetResult get_callback;
    };
    NodeId local_;
    core::ConsistentHashRing& ring_;
    core::RegionStore& store_;
    const ReplicationStrategy& strategy_;
    Send send_;
    Schedule schedule_;
    Cancel cancel_;
    std::map<std::uint32_t,Pending> pending_;
    std::uint32_t next_id_=0x40000000;
    std::uint32_t id();
    std::set<NodeId> targets(std::uint32_t key,std::size_t count) const;
    bool dispatch(NodeId to,const messages::Message& msg);
    void failed(std::uint32_t id,NodeId sender);
    void deadline(std::uint32_t id);
    void finish_put(std::uint32_t id,bool ok,const std::string& error);
    void finish_get(std::uint32_t id,bool ok,const std::string& error);
};
}
