#pragma once
#include "core/consistent_hash_ring.hpp"
#include "core/region_store.hpp"
#include "messages/message.hpp"
#include "net/timer_queue.hpp"
#include "replication/replication_strategy.hpp"
#include <chrono>
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
    // complete_all: a read finishes only when every expected target has
    // responded or failed (or at the deadline, which then merges the best
    // available responses). exclude_self: the local node is not counted as a
    // source (used by backfill, whose local store is by definition empty).
    // fanout>0 overrides the target set to the first `fanout` ring
    // successors (the strategy's max-N superset): backfill uses it to
    // discover acknowledged copies stranded on replicas dropped by a later
    // shrink (e.g. N4->N2). With fanout, required is capped to the number
    // of available sources so an N=2 pull can still succeed.
    // background: a repair pull (backfill) that must not block membership
    // quiescence gates — a transition can proceed while a background pull
    // is in flight; its merge is idempotent and all activity is serialized
    // on the event-loop thread.
    std::uint32_t get(std::uint32_t key,GetResult callback,
                      bool complete_all=false,bool exclude_self=false,
                      std::size_t fanout=0,bool background=false);
    void on_message(NodeId sender,const messages::Message& msg);
    void on_disconnect(NodeId sender);
    // Pace periodic in-memory repair; returns true if one region was attempted.
    bool anti_entropy_step();
    // Policy-activation backfill: pull a quorum-merged copy of one queued
    // region per call into the local store (no browser GET required).
    void request_backfill(std::uint32_t key);
    // Re-queue a region even if an earlier backfill resolved it (used when a
    // NEW policy snapshot changed the target set: an empty/absent owned
    // region must re-pull from the current max-N superset, which may now
    // include former holders of acknowledged data).
    void reconsider_backfill(std::uint32_t key);
    bool backfill_step();
    // Called when a stale-generation reset clears the local store: forget
    // queued/resolved backfill state so the fresh joiner re-pulls everything,
    // and drop in-flight backfill callbacks from the previous state epoch.
    void reset_repair_state();
    // Optional metrics observer (ops CSV); set by the peer when configured.
    using OpMetric = std::function<void(const char* op,std::uint32_t region,
                                        bool ok,double latency_ms,const std::string& error)>;
    void set_metrics(OpMetric cb) {metrics_cb_=std::move(cb);}
    using BackfillMetric = std::function<void(std::uint32_t region,bool ok,
                                              const std::string& error)>;
    void set_backfill_metric(BackfillMetric cb) {backfill_metric_cb_=std::move(cb);}
    std::uint64_t repair_bytes_sent() const noexcept {return repair_bytes_sent_;}
    void cancel_all(const std::string& reason);
    std::size_t pending_count() const noexcept { return pending_.size(); }
    // Pending requests that are NOT background repair pulls (browser PUT/GET
    // quorums); membership quiescence gates use this so a draining backfill
    // queue cannot stall join/handoff transitions.
    std::size_t foreground_pending() const noexcept {
        std::size_t n=0;
        for(const auto& [id,p]:pending_)if(!p.background)++n;
        return n;
    }
    QuorumConfig config(std::uint32_t key) const {return strategy_.plan(key);}
private:
    struct Pending {
        bool read=false;
        bool complete_all=false;   // finish only when all expected sources settle
        bool background=false;     // repair pull; excluded from quiescence gates
        std::uint32_t key=0;
        std::size_t required=0;
        std::set<NodeId> expected, responded, failed;
        std::map<NodeId,core::Region> versions;
        net::TimerQueue::Token timer=0;
        std::chrono::steady_clock::time_point started{};
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
    std::size_t repair_cursor_=0;
    std::uint64_t repair_bytes_sent_=0;
    std::set<std::uint32_t> backfill_queue_;
    std::set<std::uint32_t> backfill_resolved_;
    std::map<std::uint32_t,unsigned> backfill_attempts_;
    std::uint64_t state_epoch_=0;
    // Bounded retries before an empty merge is accepted as empty_valid
    // (anti-entropy push remains the backstop after acceptance).
    static constexpr unsigned kMaxBackfillAttempts=4;
    OpMetric metrics_cb_;
    BackfillMetric backfill_metric_cb_;
    std::uint32_t id();
    std::set<NodeId> targets(std::uint32_t key,std::size_t count) const;
    bool dispatch(NodeId to,const messages::Message& msg);
    void failed(std::uint32_t id,NodeId sender);
    void deadline(std::uint32_t id);
    void finish_put(std::uint32_t id,bool ok,const std::string& error);
    void finish_get(std::uint32_t id,bool ok,const std::string& error);
};
}
