#include "peer/peer_node.hpp"
#include "messages/placement_protocol.hpp"
#include "messages/payloads.hpp"
#include "messages/join_request.hpp"
#include "net/tcp_client.hpp"
#include <arpa/inet.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <cstring>
#include <filesystem>

namespace p2place::peer {
namespace {
using Clock=std::chrono::steady_clock;
constexpr auto TRANSITION_TIMEOUT=std::chrono::seconds(90);
void push32(std::vector<std::uint8_t>& v,std::uint32_t n) {
    v.push_back(n>>24);v.push_back(n>>16);v.push_back(n>>8);v.push_back(n);
}
std::uint32_t read32(std::span<const std::uint8_t> b,std::size_t& at) {
    if(b.size()-at<4)throw std::invalid_argument("short endpoint payload");
    auto x=(std::uint32_t(b[at])<<24)|(std::uint32_t(b[at+1])<<16)|
        (std::uint32_t(b[at+2])<<8)|std::uint32_t(b[at+3]);at+=4;return x;
}
std::vector<std::uint8_t> version_tag(const core::BalancedPlacement& p) {
    std::vector<std::uint8_t> b;
    const auto fp=p.fingerprint();
    for(int i=7;i>=0;--i)b.push_back(static_cast<std::uint8_t>(fp>>(i*8)));
    return b;
}
void check_tag(std::span<const std::uint8_t> payload,const core::BalancedPlacement& p) {
    const auto expected=version_tag(p);
    if(payload.size()!=expected.size() ||
       !std::equal(payload.begin(),payload.end(),expected.begin()))
       throw std::runtime_error("placement version fingerprint mismatch");
}
void push16(std::vector<std::uint8_t>& v,std::uint16_t n) {
    v.push_back(n>>8);v.push_back(n);
}
std::uint16_t read16(std::span<const std::uint8_t> b,std::size_t& at) {
    if(b.size()-at<2)throw std::invalid_argument("short endpoint payload");
    auto x=(std::uint16_t(b[at])<<8)|b[at+1];at+=2;return x;
}
}

void PeerNode::set_committed(const core::BalancedPlacement& candidate) {
    candidate.validate();
    if(!placement_ready_ || placement_.fingerprint()!=candidate.fingerprint()) {
        seen_pixel_events_=PixelEventDedup{};
        last_browser_pixel_versions_.clear();
    }
    placement_=candidate;
    placement_ready_=candidate.contains(config_.id);
    // Even removed peers can retain the ring for protocol completion.
    ring_.rebuild(candidate);
    std::cout << "[placement] committed generation=" << candidate.epoch()
              << " fingerprint=" << candidate.fingerprint() << " owners="
              << candidate.members().size() << "\n";
}

void PeerNode::send_control(core::NodeId id,messages::Type type,
                            const std::vector<std::uint8_t>& payload) {
    const auto fd=owner_fd(id);
    if(!fd)throw std::runtime_error("membership peer not connected: "+std::to_string(id));
    messages::Message msg;
    msg.type=type;
    msg.payload=payload;
    send_message(*fd,msg);
}
void PeerNode::send_transition_message(messages::Type type,
                                       const std::vector<std::uint8_t>& payload) {
    for(auto id:transition_peers_) {
        if(id==config_.id)continue;
        try {
            send_control(id,type,payload);
        } catch(const std::exception& e) {
            // A missing connection must never wedge or crash the transition
            // state machine: skip the peer, log it, and let the deadline
            // abort path or commit-uncertain recovery handle the rest.
            std::cerr << "[membership] cannot send type=" << static_cast<int>(type)
                      << " to peer " << id << ": " << e.what() << '\n';
        }
    }
}

bool PeerNode::connected_to_all(const std::vector<core::NodeId>& ids) const {
    for(auto id:ids)if(id!=config_.id && !fd_by_node_.contains(id))return false;
    return true;
}

void PeerNode::schedule_join(core::NodeId id) {
    if(config_.id!=authority_id_)return;
    if(placement_ready_ && placement_.contains(id))return;
    // A transition already admitting this candidate is in flight: absorb the
    // referral instead of re-enqueueing (one enqueue per candidate per
    // generation). If that transition aborts, later referrals re-enqueue.
    if(phase_!=Phase::Idle && staged_ && staged_->contains(id))return;
    const bool inserted =
        std::find(membership_queue_.begin(),membership_queue_.end(),
                  std::make_pair(id,true)) == membership_queue_.end();
    if(inserted) {
        membership_queue_.emplace_back(id,true);
        std::cout << "[membership] queued JOIN " << id
                  << " newly_enqueued=true queue_size=" << membership_queue_.size() << '\n';
    }
    // Duplicate referrals/HELLOs for an already queued candidate are
    // silently absorbed; the referral remains rate-limited upstream.
}
void PeerNode::schedule_leave(core::NodeId id, const std::string& reason) {
    if(config_.id!=authority_id_)return;
    if(id==authority_id_) {
        std::cerr << "[membership] authority cannot leave without failover protocol\n";
        return;
    }
    if(!placement_ready_ || !placement_.contains(id))return;
    const bool inserted =
        std::find(membership_queue_.begin(),membership_queue_.end(),
                  std::make_pair(id,false)) == membership_queue_.end();
    if(inserted) {
        membership_queue_.emplace_back(id,false);
        if(reason=="unavailable")
            std::cout << "[membership] queued AUTO_REMOVE peer=" << id
                      << " reason=unavailable queue_size=" << membership_queue_.size() << '\n';
        else
            std::cout << "[membership] queued LEAVE " << id
                      << " newly_enqueued=true queue_size=" << membership_queue_.size() << '\n';
    }
}

void PeerNode::schedule_auto_removal_tick(std::chrono::steady_clock::time_point now) {
    // Authority-only, serialized: one membership change at a time, and only
    // after the unavailable state has persisted for the stabilization window.
    if(config_.id!=authority_id_ || !placement_ready_ || phase_!=Phase::Idle)return;
    for(auto id:placement_.members()) {
        if(id==config_.id)continue;
        auto info=peers_.find(id);
        if(!info || info->status!=core::NodeStatus::Dead)continue;
        if(!unavailable_since_.contains(id)) {
            unavailable_since_[id]=now;
            continue;
        }
        if(now-unavailable_since_[id] <
           std::chrono::seconds(config_.recovery_stabilization_seconds))continue;
        if(!has_majority()) {
            // No surviving strict majority: do NOT claim a safe new placement.
            const std::string msg="reachable="+std::to_string(reachable_voters())
                +"/"+std::to_string(placement_.members().size());
            if(msg!=last_blocked_log_) {
                last_blocked_log_=msg;
                std::cout << "[membership] reconfiguration blocked: no surviving majority ("
                          << msg << ")\n";
            }
            continue;
        }
        last_blocked_log_.clear();
        schedule_leave(id,"unavailable");
    }
}

void PeerNode::begin_transition(core::NodeId id,bool joining) {
    if(phase_!=Phase::Idle)throw std::logic_error("overlapping membership transition");
    if(joining)placement_manager_.propose_join(id);
    else placement_manager_.propose_leave(id);
    const auto& next=*placement_manager_.pending();
    transition_peers_.clear();
    for(auto n:placement_.members())transition_peers_.insert(n);
    for(auto n:next.members())transition_peers_.insert(n);
    waiting_=transition_peers_;
    waiting_.erase(config_.id);
    removal_transition_=!joining;
    removed_peers_.clear();
    transition_started_=Clock::now();
    if(!joining) {
        // Note: members() returns by value — bind it once; begin()/end()
        // from two separate temporaries would be undefined behavior.
        const auto next_members=next.members();
        for(auto n:placement_.members())
            if(!std::binary_search(next_members.begin(),next_members.end(),n))
                removed_peers_.insert(n);
        // Survivor-only participants: never wait for a READY/ACK from a
        // removed member (disconnected or confirmed dead).
        for(auto it=waiting_.begin();it!=waiting_.end();) {
            auto info=peers_.find(*it);
            const bool dead=info && info->status==core::NodeStatus::Dead;
            if(!fd_by_node_.contains(*it) || dead) it=waiting_.erase(it);
            else ++it;
        }
    }
    transition_keys_.clear();
    transition_keys_[config_.id] = {};
    for(auto key:region_store_.all_keys())transition_keys_[config_.id].insert(key);
    staged_=next;
    frozen_=true;
    ready_sent_=false;
    // Quiesce ongoing browser snapshots before preparing the handoff.
    for(auto it=snapshots_.begin();it!=snapshots_.end();) {
        const int fd=it->first;
        ++it;
        finish_snapshot(fd,"cluster reconfiguring");
    }
    phase_=Phase::Preparing;
    transition_deadline_=Clock::now()+TRANSITION_TIMEOUT;
    commit_uncertain_logged_=false;
    last_transition_wait_.clear();
    last_ready_wait_.clear();
    {
        std::ostringstream os;
        for(auto n:placement_.members())os<<n<<',';
        const auto old_members=os.str();
        os.str({});os.clear();
        for(auto n:next.members())os<<n<<',';
        std::cout << "[transition-start] authority=" << config_.id
                  << " gen=" << next.epoch()
                  << " members_old=[" << old_members << "]"
                  << " members_new=[" << os.str() << "]\n";
    }
    std::cout << "[membership] PREPARE generation " << next.epoch() << '\n';
    send_transition_message(messages::Type::PLACEMENT_PREPARE,
        messages::encode_placement(next));
}

void PeerNode::abort_transition(const std::string& why) {
    if(phase_==Phase::Idle)return;
    {
        std::ostringstream os;
        for(auto n:waiting_)os<<n<<',';
        std::cerr << "[transition-abort] phase=" << static_cast<int>(phase_)
                  << " gen=" << (staged_ ? staged_->epoch() : placement_.epoch())
                  << " reason=" << why
                  << " waiting=[" << os.str() << "]\n";
    }
    // Never roll back a partially committed version automatically.
    if(phase_==Phase::Committing) {
        if(!commit_uncertain_logged_) {
            std::cerr << "[membership] COMMIT UNCERTAIN: remain frozen until recovery\n";
            commit_uncertain_logged_=true;
        }
        return;
    }
    // Tolerant broadcast: a missing participant connection must not leave
    // this state machine wedged. Unreachable peers keep their fail-closed
    // prepare markers until they reconnect and receive a fresh PREPARE/ABORT.
    send_transition_message(messages::Type::PLACEMENT_ABORT);
    placement_manager_.abort();
    staged_.reset();
    moves_.clear();
    transition_keys_.clear();
    waiting_.clear();
    transition_peers_.clear();
    ready_sent_=false;
    next_move_=0;
    commit_uncertain_logged_=false;
    removal_transition_=false;
    removed_peers_.clear();
    last_transition_wait_.clear();
    last_ready_wait_.clear();
    phase_=Phase::Idle;
    frozen_=false;
}

void PeerNode::progress_transition() {
    if(config_.id!=authority_id_)return;
    if(phase_==Phase::Idle) {
        std::ostringstream wait;
        const auto [id,joining] = membership_queue_.empty()
            ? std::pair<core::NodeId,bool>{0,true}
            : membership_queue_.front();
        if(membership_queue_.empty()) wait << "empty_queue";
        else if(replicator_ && replicator_->pending_count()!=0)
            wait << "pending_quorums=" << replicator_->pending_count();
        else if(joining && !fd_by_node_.contains(id))
            wait << "candidate=" << id << " not_connected";
        else if(joining && !connected_to_all(placement_.members())) {
            wait << "missing_member_connections=[";
            for(auto n:placement_.members())
                if(n!=config_.id && !fd_by_node_.contains(n))wait<<n<<',';
            wait << "]";
        } else if(!joining) {
            // Removal: the member being removed may be unreachable. Require
            // every OTHER committed member to be connected; never wait for
            // an ACK, snapshot, or release from the confirmed-unavailable
            // member.
            bool missing=false;
            for(auto n:placement_.members())
                if(n!=config_.id && n!=id && !fd_by_node_.contains(n))missing=true;
            if(missing)
                wait << "missing_survivor_connections peer=" << id << " being_removed";
        }
        if(!wait.str().empty()) {
            if(wait.str()!=last_transition_wait_) {
                last_transition_wait_=wait.str();
                std::cout << "[join-wait] authority=" << config_.id
                          << " reason=" << wait.str() << '\n';
            }
            return;
        }
        last_transition_wait_.clear();
        membership_queue_.pop_front();
        try {begin_transition(id,joining);}
        catch(const std::exception& e) {std::cerr << "[membership] " << e.what() << '\n';}
        return;
    }
    if(Clock::now()>transition_deadline_) {
        abort_transition("timeout waiting for participants or region handoff");
        return;
    }
    if(phase_==Phase::Preparing) {
        std::ostringstream wait;
        if(!waiting_.empty()) {
            wait << "awaiting_ready=[";
            for(auto n:waiting_)wait<<n<<',';
            wait << "]";
        } else if(replicator_ && replicator_->pending_count()!=0) {
            wait << "pending_quorums=" << replicator_->pending_count();
        } else if(!snapshots_.empty()) {
            wait << "browser_snapshots=" << snapshots_.size();
        }
        if(!wait.str().empty()) {
            if(wait.str()!=last_transition_wait_) {
                last_transition_wait_=wait.str();
                std::cout << "[transition-wait] phase=preparing gen="
                          << staged_->epoch() << " reason=" << wait.str() << '\n';
            }
            return;
        }
        last_transition_wait_.clear();
        moves_.clear();
        // Snapshot local keys after all prior work is quiescent.
        transition_keys_[config_.id].clear();
        for(auto key:region_store_.all_keys())transition_keys_[config_.id].insert(key);
        // Backfill replicas newly entering the canonical N-set, not merely
        // partitions whose primary owner changed. Frozen writes provide a
        // stable snapshot during handoff.
        for(std::uint32_t key=0;key<core::BalancedPlacement::PARTITIONS;++key) {
            const auto before=placement_.preference(key,
                std::min(config_.replication_n,placement_.members().size()));
            const auto after=staged_->preference(key,
                std::min(config_.replication_n,staged_->members().size()));
            for(auto dest:after) {
                if(std::find(before.begin(),before.end(),dest)!=before.end())continue;
                // Merge EVERY old replica into the new replica. A write may
                // reach W=2 without reaching the old primary; relying on
                // the primary alone would silently lose that update.
                for(auto source:before)
                    if(transition_keys_[source].contains(key))
                        moves_.push_back({key,source,dest});
            }
        }
        next_move_=0;
        std::cout << "[handoff-plan] gen=" << staged_->epoch()
                  << " moves=" << moves_.size() << '\n';
        // Honest recovery accounting for failed-member removal: a region is
        // lost-possible when a removed member was among its old replicas and
        // NO surviving old replica reported it. RAM-only pixels make
        // "never written" vs "lost" indistinguishable here, so the prototype
        // reports the unknown category instead of inventing certainty.
        if(removal_transition_) {
            std::size_t lost=0, held=0;
            std::vector<std::uint32_t> examples;
            for(std::uint32_t key=0;key<core::BalancedPlacement::PARTITIONS;++key) {
                bool survivor_has=false;
                for(const auto& [n,keys]:transition_keys_)
                    if(keys.contains(key)) {survivor_has=true;break;}
                if(survivor_has) {++held;continue;}
                const auto old_reps=placement_.preference(key,
                    std::min(config_.replication_n,placement_.members().size()));
                bool removed_was_replica=false;
                for(auto n:old_reps)
                    if(removed_peers_.contains(n)) {removed_was_replica=true;break;}
                if(removed_was_replica) {
                    ++lost;
                    if(examples.size()<8)examples.push_back(key);
                }
            }
            std::cout << "[data-loss] generation=" << staged_->epoch()
                      << " lost_possible=" << lost
                      << " reason=no-surviving-valid-replica-or-never-written";
            if(!examples.empty()) {
                std::cout << " regions=[";
                for(auto k:examples)std::cout<<k<<',';
                std::cout << ']';
            }
            std::cout << '\n';
            std::cout << "[recovery] generation=" << staged_->epoch()
                      << " moves=" << moves_.size()
                      << " survivor_held=" << held
                      << " lost_possible=" << lost
                      << " untouched=" << (core::BalancedPlacement::PARTITIONS-held-lost)
                      << '\n';
        }
        next_handoff();
    } else if(phase_==Phase::Fetching || phase_==Phase::Installing) {
        std::ostringstream wait;
        if(next_move_<moves_.size()) {
            const auto& m=moves_[next_move_];
            wait << "move=" << (next_move_+1) << '/' << moves_.size()
                 << " key=" << m.key << " from=" << m.old_owner << " to=" << m.new_owner;
        }
        if(wait.str()!=last_transition_wait_) {
            last_transition_wait_=wait.str();
            std::cout << "[transition-wait] phase="
                      << (phase_==Phase::Fetching?"fetching":"installing")
                      << " gen=" << staged_->epoch() << " reason=" << wait.str() << '\n';
        }
    } else if(phase_==Phase::Committing) {
        std::ostringstream wait;
        wait << "awaiting_commit_ack=[";
        for(auto n:waiting_)wait<<n<<',';
        wait << "]";
        if(wait.str()!=last_transition_wait_) {
            last_transition_wait_=wait.str();
            std::cout << "[transition-wait] phase=committing gen="
                      << placement_.epoch() << " reason=" << wait.str() << '\n';
        }
    }
}

void PeerNode::next_handoff() {
    if(!staged_)throw std::logic_error("no staged placement");
    transition_deadline_=Clock::now()+TRANSITION_TIMEOUT;
    if(next_move_==moves_.size()) {
        // Important: actual region copies are complete BEFORE commitment.
        try {
            placement_manager_.activate();
        } catch(const std::exception& e) {
            abort_transition(std::string("activate failed: ")+e.what());
            return;
        }
        set_committed(placement_manager_.active());
        phase_=Phase::Committing;
        waiting_=transition_peers_;
        waiting_.erase(config_.id);
        // Survivor-only commit destinations: never await an unavailable
        // member being removed (a no-op for joins, where every participant
        // is connected by the admission guard).
        for(auto it=waiting_.begin();it!=waiting_.end();) {
            auto info=peers_.find(*it);
            const bool dead=info && info->status==core::NodeStatus::Dead;
            if(!fd_by_node_.contains(*it) || dead) it=waiting_.erase(it);
            else ++it;
        }
        send_transition_message(messages::Type::PLACEMENT_COMMIT,
            messages::encode_placement(placement_));
        std::cout << "[commit-sent] gen=" << placement_.epoch() << '\n';
        return;
    }
    const auto& m=moves_[next_move_];
    if(m.old_owner==config_.id) {
        const auto r=region_store_.get_region(m.key).value_or(core::Region{});
        if(m.new_owner==config_.id) {++next_move_;next_handoff();return;}
        phase_=Phase::Installing;
        std::cout << "[handoff-put] src=" << config_.id << " dst=" << m.new_owner
                  << " region=" << m.key << " move=" << (next_move_+1)
                  << '/' << moves_.size() << '\n';
        try {
            send_control(m.new_owner,messages::Type::HANDOFF_PUT,
                messages::encode_region({m.key,r}));
        } catch(const std::exception& e) {
            abort_transition(std::string("HANDOFF_PUT to ")+std::to_string(m.new_owner)
                +" failed: "+e.what());
        }
    } else {
        phase_=Phase::Fetching;
        std::cout << "[handoff-get] src=" << m.old_owner << " dst=" << m.new_owner
                  << " region=" << m.key << " move=" << (next_move_+1)
                  << '/' << moves_.size() << '\n';
        try {
            send_control(m.old_owner,messages::Type::HANDOFF_GET,
                messages::encode_partition_id(m.key));
        } catch(const std::exception& e) {
            abort_transition(std::string("HANDOFF_GET to ")+std::to_string(m.old_owner)
                +" failed: "+e.what());
        }
    }
}

void PeerNode::send_ready_if_quiescent() {
    if(!staged_ || ready_sent_ || !frozen_)return;
    std::ostringstream wait;
    if(replicator_ && replicator_->pending_count()!=0)
        wait << "pending_quorums=" << replicator_->pending_count() << ' ';
    if(!snapshots_.empty())wait << "browser_snapshots=" << snapshots_.size() << ' ';
    // All-to-all connectivity with the staged membership is a deliberate
    // READY gate: the authority can only trust handoff sources/destinations
    // that each participant has already authenticated directly.
    std::vector<core::NodeId> missing;
    for(auto id:staged_->members())
        if(id!=config_.id && !fd_by_node_.contains(id))missing.push_back(id);
    if(!missing.empty()) {
        wait << "missing_connections=[";
        for(auto id:missing)wait<<id<<',';
        wait << ']';
    }
    if(!wait.str().empty()) {
        if(wait.str()!=last_ready_wait_) {
            last_ready_wait_=wait.str();
            std::cout << "[ready-wait] node=" << config_.id
                      << " gen=" << staged_->epoch()
                      << " reason=" << wait.str() << '\n';
        }
        return;
    }
    last_ready_wait_.clear();
    auto payload=version_tag(*staged_);
    payload.resize(8+32,0); // bitmap of non-empty local region keys
    for(auto key:region_store_.all_keys())payload[8+key/8] |= static_cast<std::uint8_t>(1u<<(key%8));
    send_control(authority_id_,messages::Type::PLACEMENT_READY,payload);
    ready_sent_=true;
    std::cout << "[ready-sent] node=" << config_.id
              << " gen=" << staged_->epoch()
              << " keys_present=" << region_store_.all_keys().size() << '\n';
}

void PeerNode::on_handoff_message(net::TcpConnection& conn,
        const messages::Message& msg,core::NodeId sender) {
    if(!frozen_)throw std::runtime_error("handoff outside transition");
    switch(msg.type) {
    case messages::Type::HANDOFF_GET: {
        // Handoff runs before commit, so placement_ is still the OLD
        // committed placement here and staged_ must be present.
        if(sender!=authority_id_ || !placement_ready_ || !staged_)
            throw std::runtime_error("unauthorized HANDOFF_GET");
        const auto key=messages::decode_partition_id(msg.payload);
        // Any legitimate OLD replica may hold the newest acknowledged copy:
        // a W=2 write can reach a secondary without ever reaching the
        // primary. Verify membership in the old canonical replica set
        // instead of demanding primary ownership.
        const auto old_replicas=placement_.preference(key,
            std::min(config_.replication_n,placement_.members().size()));
        if(std::find(old_replicas.begin(),old_replicas.end(),config_.id)==
           old_replicas.end())
            throw std::runtime_error("HANDOFF_GET for region not held by this replica");
        const auto r=region_store_.get_region(key).value_or(core::Region{});
        const auto payload=messages::encode_region({key,r});
        std::cout << "[handoff-data] src=" << config_.id << " region=" << key
                  << " bytes=" << payload.size() << '\n';
        send_control(sender,messages::Type::HANDOFF_DATA,payload);
        break;
    }
    case messages::Type::HANDOFF_PUT: {
        if(sender!=authority_id_ || !staged_)
            throw std::runtime_error("unauthorized HANDOFF_PUT");
        const auto r=messages::decode_region(msg.payload);
        if(r.key>=256)throw std::runtime_error("invalid handoff region");
        const auto replicas=staged_->preference(r.key,
            std::min(config_.replication_n,staged_->members().size()));
        if(std::find(replicas.begin(),replicas.end(),config_.id)==replicas.end())
            throw std::runtime_error("handoff destination is not a replica");
        region_store_.merge_region(r.key,r.region);
        std::cout << "[handoff-put] dst=" << config_.id << " region=" << r.key
                  << " bytes=" << msg.payload.size() << '\n';
        send_control(sender,messages::Type::HANDOFF_OK,
            messages::encode_partition_id(r.key));
        break;
    }
    case messages::Type::HANDOFF_DATA: {
        if(config_.id!=authority_id_ || phase_!=Phase::Fetching ||
           next_move_>=moves_.size() || sender!=moves_[next_move_].old_owner)
            throw std::runtime_error("unexpected HANDOFF_DATA");
        const auto r=messages::decode_region(msg.payload);
        const auto& m=moves_[next_move_];
        if(r.key!=m.key)throw std::runtime_error("HANDOFF_DATA wrong region");
        std::cout << "[handoff-data] src=" << m.old_owner << " region=" << r.key
                  << " bytes=" << msg.payload.size() << '\n';
        if(m.new_owner==config_.id) {
            region_store_.merge_region(m.key,r.region);
            ++next_move_;
            next_handoff();
        } else {
            phase_=Phase::Installing;
            try {
                send_control(m.new_owner,messages::Type::HANDOFF_PUT,msg.payload);
            } catch(const std::exception& e) {
                abort_transition(std::string("HANDOFF_PUT to ")+std::to_string(m.new_owner)
                    +" failed: "+e.what());
            }
        }
        break;
    }
    case messages::Type::HANDOFF_OK: {
        if(config_.id!=authority_id_ || phase_!=Phase::Installing ||
           next_move_>=moves_.size() || sender!=moves_[next_move_].new_owner ||
           messages::decode_partition_id(msg.payload)!=moves_[next_move_].key)
            throw std::runtime_error("unexpected HANDOFF_OK");
        std::cout << "[handoff-ok] dst=" << moves_[next_move_].new_owner
                  << " region=" << moves_[next_move_].key
                  << " move=" << (next_move_+1) << '/' << moves_.size() << '\n';
        ++next_move_;
        next_handoff();
        break;
    }
    default: (void)conn;throw std::runtime_error("invalid handoff message");
    }
}

bool PeerNode::accept_authority_message(core::NodeId sender) {
    if(sender==authority_id_)return true;
    if(!placement_ready_) {
        // A fresh or stale-reset joiner may not know the elected authority:
        // when the authority never failed, no election token exists to sync,
        // and rejecting the authority's first control message would tear
        // down the connection. Only the authority emits these messages.
        authority_id_=sender;
        std::cout << "[authority] leader=" << authority_id_
                  << " adopted-from=control node=" << config_.id << '\n';
        return true;
    }
    return false;
}

void PeerNode::on_placement_message(net::TcpConnection& conn,
        const messages::Message& msg,core::NodeId sender) {
    using T=messages::Type;
    switch(msg.type) {
    case T::LEAVE_REQUEST:
        if(config_.id!=authority_id_ || sender==0 ||
           !msg.payload.empty())throw std::runtime_error("unauthorized leave request");
        schedule_leave(sender);
        return;
    case T::JOIN_REQUEST: {
        // JOIN_REQUEST is a referral, NOT admission. A committed seed may
        // introduce a directly connected joiner, but the authority must dial
        // the advertised endpoint and verify its HELLO fingerprint itself.
        if(config_.id!=authority_id_ || sender==0 || !placement_ready_ ||
           !placement_.contains(sender))
            throw std::runtime_error("unauthorized join referral");
        const auto referral=messages::decode_join_referral(msg.payload);
        if(referral.node_id==config_.id || placement_.contains(referral.node_id))
            return; // Already a member or a nonsensical referral.
        if(auto direct=fd_by_node_.find(referral.node_id);direct!=fd_by_node_.end()) {
            // A direct HELLO already completed; normal admission applies.
            schedule_join(referral.node_id);
            return;
        }
        in_addr addr{htonl(referral.ipv4)};
        char ip[INET_ADDRSTRLEN]{};
        if(!inet_ntop(AF_INET,&addr,ip,sizeof(ip)))
            throw std::runtime_error("invalid referral IPv4");
        std::cout << "[join-referral] authority=" << config_.id
                  << " from-seed=" << sender << " candidate=" << referral.node_id
                  << " endpoint=" << ip << ':' << referral.tcp_port << '\n';
        connect_peer(referral.node_id, ip, referral.tcp_port);
        return;
    }
    case T::PLACEMENT_PREPARE: {
        if(!accept_authority_message(sender))
            throw std::runtime_error("unexpected placement PREPARE");
        auto next=messages::decode_placement(msg.payload);
        if(placement_ready_ && next.epoch()!=placement_.epoch()+1)
            throw std::runtime_error("nonconsecutive placement generation");
        if(!next.contains(config_.id) &&
           (!placement_ready_ || !placement_.contains(config_.id)))
            throw std::runtime_error("PREPARE for unrelated peer");
        // A fresh consecutive-generation PREPARE supersedes any earlier
        // UNCOMMITTED proposal: this participant may have missed the ABORT
        // that ended the previous attempt, or restarted committed-but-
        // unreleased. placement_ is still the committed base in all of
        // those states, so replacing staged_ cannot roll back committed data.
        // (A live transition cannot send a second PREPARE: the authority
        // serializes them, and TCP keeps this connection FIFO.)
        // Durable, fail-closed prepare marker. A restart during transition
        // does not silently enable old routing.
        if(auto dir=std::filesystem::path(config_.state_file).parent_path();!dir.empty())
            std::filesystem::create_directories(dir);
        next.save(config_.state_file+".pending");
        frozen_=true;
        staged_=std::move(next);
        ready_sent_=false;
        std::cout << "[prepare-received] node=" << config_.id
                  << " gen=" << staged_->epoch()
                  << " pending_file=" << config_.state_file << ".pending\n";
        // Abort any partially streamed UI snapshots. No new requests may start.
        for(auto it=snapshots_.begin();it!=snapshots_.end();) {
            const int fd=it->first;
            ++it;
            finish_snapshot(fd,"cluster reconfiguring");
        }
        send_ready_if_quiescent();
        return;
    }
    case T::PLACEMENT_READY: {
        if(config_.id!=authority_id_)return; // not a READY consumer; ignore
        // A stale READY (e.g., raced with an abort) must not tear down a
        // healthy member connection.
        if(phase_!=Phase::Preparing || !waiting_.contains(sender)) {
            std::cerr << "[ready-stale] authority=" << config_.id
                      << " from=" << sender
                      << " phase=" << static_cast<int>(phase_) << '\n';
            return;
        }
        if(msg.payload.size()!=40)throw std::runtime_error("invalid READY bitmap size");
        check_tag(std::span<const std::uint8_t>(msg.payload.data(),8),*staged_);
        std::set<std::uint32_t> present;
        for(std::uint32_t k=0;k<256;++k)
            if(msg.payload[8+k/8] & (1u<<(k%8)))present.insert(k);
        transition_keys_[sender]=std::move(present);
        {
            std::ostringstream os;
            for(auto n:waiting_)os<<n<<',';
            std::cout << "[ready-ack] authority=" << config_.id
                      << " from=" << sender
                      << " keys_present=" << transition_keys_[sender].size()
                      << " waiting=[" << os.str() << "]\n";
        }
        waiting_.erase(sender);
        progress_transition();
        return;
    }
    case T::PLACEMENT_COMMIT: {
        if(!accept_authority_message(sender) || !frozen_)
            throw std::runtime_error("unexpected placement COMMIT");
        auto incoming=messages::decode_placement(msg.payload);
        if(!staged_) {
            // Committed-but-unreleased participant receiving the same commit
            // again: idempotently re-acknowledge.
            if(incoming.fingerprint()==placement_.fingerprint()) {
                std::cout << "[commit-dup] node=" << config_.id
                          << " gen=" << placement_.epoch() << " re-acknowledging\n";
                send_control(sender,T::PLACEMENT_COMMITTED,version_tag(placement_));
                return;
            }
            throw std::runtime_error("commit does not match prepared placement");
        }
        if(incoming.fingerprint()!=staged_->fingerprint())
            throw std::runtime_error("commit does not match prepared placement");
        placement_manager_.install_committed(incoming);
        set_committed(placement_manager_.active());
        staged_.reset();
        std::cout << "[commit-ack] node=" << config_.id
                  << " gen=" << placement_.epoch() << '\n';
        send_control(sender,T::PLACEMENT_COMMITTED,version_tag(placement_));
        return;
    }
    case T::PLACEMENT_COMMITTED:
        if(config_.id!=authority_id_)return;
        // Stale commit ACK must not tear down a healthy member connection.
        if(phase_!=Phase::Committing || !waiting_.contains(sender)) {
            std::cerr << "[commit-ack-stale] authority=" << config_.id
                      << " from=" << sender
                      << " phase=" << static_cast<int>(phase_) << '\n';
            return;
        }
        check_tag(msg.payload,placement_);
        waiting_.erase(sender);
        std::cout << "[commit-ack] authority=" << config_.id
                  << " gen=" << placement_.epoch()
                  << " from=" << sender
                  << " remaining=" << waiting_.size() << '\n';
        if(waiting_.empty()) {
            send_transition_message(T::PLACEMENT_RELEASE,version_tag(placement_));
            frozen_=false;
            staged_.reset();
            moves_.clear();
            transition_peers_.clear();
            if(removal_transition_) {
                // The removal is now committed by all survivors: distinct
                // from the earlier UNAVAILABLE detection state.
                for(auto id:removed_peers_) {
                    std::cout << "[membership] REMOVED peer=" << id
                              << " generation=" << placement_.epoch() << '\n';
                    unavailable_since_.erase(id);
                    last_peer_health_status_.erase(id);
                    reported_unavailable_.erase(id);
                }
                const auto ms=std::chrono::duration_cast<std::chrono::milliseconds>(
                    Clock::now()-transition_started_).count();
                std::cout << "[reconfig] cause=failed-peer peer=";
                for(auto id:removed_peers_)std::cout<<id<<',';
                std::cout << " generation=" << placement_.epoch()
                          << " duration_ms=" << ms << '\n';
                // Under-replication is honest: target N stays, live members
                // may be fewer than N.
                std::size_t under=0;
                for(std::uint32_t key=0;key<core::BalancedPlacement::PARTITIONS;++key) {
                    const auto reps=placement_.preference(key,
                        std::min(config_.replication_n,placement_.members().size()));
                    if(reps.size()<config_.replication_n)++under;
                }
                std::cout << "[replication] strategy=A target_n=" << config_.replication_n
                          << " live_members=" << placement_.members().size()
                          << " under_replicated_regions=" << under << '\n';
                removal_transition_=false;
                removed_peers_.clear();
            }
            phase_=Phase::Idle;
            std::cout<<"[membership] RELEASE generation "<<placement_.epoch()<<'\n';
            broadcast_endpoints();
        }
        return;
    case T::PLACEMENT_RELEASE:
        if(!accept_authority_message(sender) || !frozen_ || staged_)
            throw std::runtime_error("unexpected placement RELEASE");
        check_tag(msg.payload,placement_);
        std::filesystem::remove(config_.state_file+".pending");
        frozen_=false;
        ready_sent_=false;
        std::cout<<"[release-received] node="<<config_.id
                 <<" gen="<<placement_.epoch()<<" pending_removed=true\n";
        if(!placement_ready_) {
            std::cout<<"[membership] graceful departure completed; stopping peer\n";
            loop_.stop();
        }
        return;
    case T::PLACEMENT_ABORT:
        if(!accept_authority_message(sender) || !frozen_)
            throw std::runtime_error("unexpected placement ABORT");
        if(staged_) {
            std::filesystem::remove(config_.state_file+".pending");
            staged_.reset();frozen_=false;ready_sent_=false;
            std::cout << "[transition-abort] node=" << config_.id
                      << " phase=participant gen=" << placement_.epoch()
                      << " reason=authority abort pending_removed=true\n";
        }
        else throw std::runtime_error("cannot abort after commit; fail closed");
        return;
    case T::PEER_ENDPOINTS:
        if(!accept_authority_message(sender))
            throw std::runtime_error("unauthorized endpoint advertisement");
        if(msg.payload.size()<2)throw std::invalid_argument("short endpoints");
        {
            std::size_t at=0;
            auto count=read16(msg.payload,at);
            if(count>256 || msg.payload.size()!=2+static_cast<std::size_t>(count)*10)
                throw std::invalid_argument("invalid endpoints length");
            for(std::size_t i=0;i<count;++i) {
                const auto id=read32(msg.payload,at);
                const auto ipnum=read32(msg.payload,at);
                const auto port=read16(msg.payload,at);
                if(id==0 || port==0 || id==config_.id)continue;
                in_addr addr{htonl(ipnum)};
                char ip[INET_ADDRSTRLEN]{};
                if(!inet_ntop(AF_INET,&addr,ip,sizeof(ip)))continue;
                core::NodeInfo info;
                info.id=id;info.host=ip;info.peer_port=port;
                info.status=fd_by_node_.contains(id)?core::NodeStatus::Alive:core::NodeStatus::Suspect;
                peers_.upsert(info);
            }
        }
        return;
    default: (void)conn;throw std::runtime_error("unknown membership control");
    }
}

void PeerNode::broadcast_endpoints() {
    if(config_.id!=authority_id_)return;
    std::vector<std::uint8_t> payload;
    const auto ids=fd_by_node_;
    if(ids.size()>255)return;
    push16(payload,static_cast<std::uint16_t>(ids.size()+1));
    // The authority's endpoint is discoverable from the seed connection;
    // advertise 127.0.0.1 for local experiments only; remote clients keep
    // their seed address for the authority instead.
    push32(payload,config_.id);
    push32(payload,0x7f000001u);
    push16(payload,config_.port);
    for(const auto& [id,fd]:ids) {
        auto peer=peers_.find(id);
        if(!peer || peer->peer_port==0)continue;
        in_addr addr{};
        if(inet_pton(AF_INET,peer->host.c_str(),&addr)!=1)continue;
        push32(payload,id);push32(payload,ntohl(addr.s_addr));push16(payload,peer->peer_port);
    }
    // Adjust count in case a peer had an invalid address.
    const std::uint16_t actual=static_cast<std::uint16_t>((payload.size()-2)/10);
    payload[0]=static_cast<std::uint8_t>(actual>>8);
    payload[1]=static_cast<std::uint8_t>(actual);
    for(const auto& [id,fd]:ids) {
        (void)id;
        messages::Message m;m.type=messages::Type::PEER_ENDPOINTS;
        m.payload=payload;send_message(fd,m);
    }
}


void PeerNode::forward_join_referrals() {
    if(join_referrals_.empty() || config_.id==authority_id_ ||
       !placement_ready_ || !placement_.contains(config_.id))return;
    const auto now=Clock::now();
    if(now<next_join_referral_)return;
    next_join_referral_=now+std::chrono::seconds(5);
    const auto fd=owner_fd(authority_id_);
    if(!fd) {
        std::cout << "[join-referral] seed=" << config_.id
                  << " waiting for elected authority " << authority_id_ << '\n';
        return;
    }
    for(auto it=join_referrals_.begin();it!=join_referrals_.end();) {
        const auto& info=it->second;
        if((placement_ready_ && placement_.contains(it->first)) ||
           !fd_by_node_.contains(it->first)) {
            it=join_referrals_.erase(it);
            continue;
        }
        in_addr address{};
        if(info.peer_port==0 || inet_pton(AF_INET,info.host.c_str(),&address)!=1) {
            std::cerr << "[join-referral] invalid address for candidate=" << it->first << '\n';
            it=join_referrals_.erase(it);
            continue;
        }
        // ntohl() gives the numeric form used by the wire codec.
        const messages::JoinReferral referral{info.id,ntohl(address.s_addr),info.peer_port};
        send_control(authority_id_,messages::Type::JOIN_REQUEST,
                     messages::encode_join_referral(referral));
        std::cout << "[join-forward] seed=" << config_.id
                  << " candidate=" << info.id
                  << " authority=" << authority_id_ << '\n';
        ++it;
    }
}

void PeerNode::connect_peer(core::NodeId id,const std::string& host,std::uint16_t port) {
    if(connecting_.contains(id) || fd_by_node_.contains(id))return;
    connecting_.insert(id);
    next_dial_[id]=Clock::now()+std::chrono::seconds(5);
    try {
        auto result=net::TcpClient::connect_to(host,port);
        if(result.status==net::ConnectStatus::Connected) {
            connecting_.erase(id);
            last_dial_error_log_.erase(id);
            dialed_fds_.insert(result.socket.get());
            on_accepted(std::move(result.socket));
        } else {
            loop_.add_pending_connection(std::move(result.socket),
                [this,id](net::Socket sock) {
                    connecting_.erase(id);
                    last_dial_error_log_.erase(id);
                    dialed_fds_.insert(sock.get());
                    on_accepted(std::move(sock));
                },
                [this,id](int error) {
                    connecting_.erase(id);
                    const auto now=Clock::now();
                    auto& last=last_dial_error_log_[id];
                    if(last==Clock::time_point{} || now-last>=std::chrono::seconds(30)) {
                        last=now;
                        std::cerr<<"[discovery] connect to "<<id<<" failed: "
                                 <<std::system_category().message(error)<<'\n';
                    }
                });
        }
    } catch(const std::exception& e) {
        connecting_.erase(id);
        const auto now=Clock::now();
        auto& last=last_dial_error_log_[id];
        if(last==Clock::time_point{} || now-last>=std::chrono::seconds(30)) {
            last=now;
            std::cerr<<"[discovery] "<<e.what()<<'\n';
        }
    }
}
void PeerNode::connect_discovered_peers() {
    // Lower IDs dial higher IDs to avoid duplicate connections. All joiners
    // explicitly seed the authority even when their ID is greater.
    const auto now=Clock::now();
    for(const auto& peer:peers_.all()) {
        if(peer.peer_port==0 || peer.host.empty() ||
           fd_by_node_.contains(peer.id) || connecting_.contains(peer.id) ||
           next_dial_[peer.id]>now)continue;
        // Configured seeds are EXEMPT from the lower-ID rule: a node must
        // always be able to re-dial its own seeds. A stale-reset joiner has
        // no other path back into the cluster (the authority only dials
        // committed members or referral candidates), and a higher-ID joiner
        // would otherwise deadlock after the seed closes its stale HELLO.
        // Concurrent mutual dials are resolved deterministically at HELLO.
        const bool is_seed = std::any_of(config_.peers.begin(),config_.peers.end(),
            [&](const core::NodeInfo& s){return s.id==peer.id;});
        if(config_.id==authority_id_) {
            // The authority maintains direct links to every committed member:
            // a restarted member would otherwise never reconnect, because it
            // does not know the authority's address and seeds only forward
            // referrals for candidates. Re-dial with the last DIRECTLY
            // observed endpoint (peers_ hosts come only from HELLO/seed
            // config) — including Dead members, and faster than the
            // unavailable+stabilization removal window, so a brief outage
            // recovers before removal fires. Dial-failure logs are
            // throttled; duplicate mutual dials resolve at HELLO.
            if(placement_ready_ && placement_.contains(peer.id)) {
                connect_peer(peer.id,peer.host,peer.peer_port);
                next_dial_[peer.id]=now+std::chrono::seconds(1);
            }
            continue;
        }
        if(placement_ready_ && !placement_.contains(peer.id) &&
           (!staged_ || !staged_->contains(peer.id)))continue;
        if(peer.id<=config_.id && peer.id!=authority_id_ && !is_seed)continue;
        if(is_seed) {
            std::cout << "[discovery] node=" << config_.id
                      << " seed=" << peer.id
                      << " addr=" << peer.host << ':' << peer.peer_port
                      << " connect_attempt=" << ++dial_attempts_[peer.id]
                      << " placement_ready=" << placement_ready_ << '\n';
        }
        connect_peer(peer.id,peer.host,peer.peer_port);
    }

}
} // namespace p2place::peer
