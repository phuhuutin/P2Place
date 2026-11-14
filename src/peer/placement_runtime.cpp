#include "peer/peer_node.hpp"
#include "messages/placement_protocol.hpp"
#include "messages/payloads.hpp"
#include "net/tcp_client.hpp"
#include <arpa/inet.h>
#include <algorithm>
#include <iostream>
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
    for(auto id:transition_peers_) if(id!=config_.id) send_control(id,type,payload);
}

bool PeerNode::connected_to_all(const std::vector<core::NodeId>& ids) const {
    for(auto id:ids)if(id!=config_.id && !fd_by_node_.contains(id))return false;
    return true;
}

void PeerNode::schedule_join(core::NodeId id) {
    if(config_.id!=authority_id_)return;
    if(placement_ready_ && placement_.contains(id))return;
    if(std::find(membership_queue_.begin(),membership_queue_.end(),
          std::make_pair(id,true)) == membership_queue_.end())
        membership_queue_.emplace_back(id,true);
    std::cout << "[membership] queued JOIN " << id << '\n';
}
void PeerNode::schedule_leave(core::NodeId id) {
    if(config_.id!=authority_id_)return;
    if(id==authority_id_) {
        std::cerr << "[membership] authority cannot leave without failover protocol\n";
        return;
    }
    if(!placement_ready_ || !placement_.contains(id))return;
    if(std::find(membership_queue_.begin(),membership_queue_.end(),
           std::make_pair(id,false)) == membership_queue_.end())
        membership_queue_.emplace_back(id,false);
    std::cout << "[membership] queued LEAVE " << id << '\n';
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
    std::cout << "[membership] PREPARE generation " << next.epoch() << '\n';
    send_transition_message(messages::Type::PLACEMENT_PREPARE,
        messages::encode_placement(next));
}

void PeerNode::abort_transition(const std::string& why) {
    if(phase_==Phase::Idle)return;
    std::cerr << "[membership] ABORT: " << why << '\n';
    // Never roll back a partially committed version automatically.
    if(phase_==Phase::Committing) {
        std::cerr << "[membership] COMMIT UNCERTAIN: remain frozen until recovery\n";
        return;
    }
    send_transition_message(messages::Type::PLACEMENT_ABORT);
    placement_manager_.abort();
    staged_.reset();
    moves_.clear();
    transition_keys_.clear();
    waiting_.clear();
    transition_peers_.clear();
    phase_=Phase::Idle;
    frozen_=false;
}

void PeerNode::progress_transition() {
    if(config_.id!=authority_id_)return;
    if(phase_==Phase::Idle) {
        if(membership_queue_.empty() || (replicator_ && replicator_->pending_count()!=0)) return;
        const auto [id,joining]=membership_queue_.front();
        if(!fd_by_node_.contains(id))return;
        if(!connected_to_all(placement_.members()))return;
        membership_queue_.pop_front();
        try {begin_transition(id,joining);}
        catch(const std::exception& e) {std::cerr << "[membership] " << e.what() << '\n';}
        return;
    }
    if(Clock::now()>transition_deadline_) {
        abort_transition("timeout waiting for participants or region handoff");
        return;
    }
    if(phase_==Phase::Preparing && waiting_.empty() &&
       (!replicator_ || replicator_->pending_count()==0) && snapshots_.empty()) {
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
        std::cout << "[membership] handoff " << moves_.size() << " replica snapshots\n";
        next_handoff();
    }
}

void PeerNode::next_handoff() {
    if(!staged_)throw std::logic_error("no staged placement");
    transition_deadline_=Clock::now()+TRANSITION_TIMEOUT;
    if(next_move_==moves_.size()) {
        // Important: actual region copies are complete BEFORE commitment.
        placement_manager_.activate();
        set_committed(placement_manager_.active());
        phase_=Phase::Committing;
        waiting_=transition_peers_;
        waiting_.erase(config_.id);
        send_transition_message(messages::Type::PLACEMENT_COMMIT,
            messages::encode_placement(placement_));
        std::cout << "[membership] COMMIT generation " << placement_.epoch() << '\n';
        return;
    }
    const auto& m=moves_[next_move_];
    if(m.old_owner==config_.id) {
        const auto r=region_store_.get_region(m.key).value_or(core::Region{});
        if(m.new_owner==config_.id) {++next_move_;next_handoff();return;}
        phase_=Phase::Installing;
        send_control(m.new_owner,messages::Type::HANDOFF_PUT,
          messages::encode_region({m.key,r}));
    } else {
        phase_=Phase::Fetching;
        send_control(m.old_owner,messages::Type::HANDOFF_GET,
          messages::encode_partition_id(m.key));
    }
}

void PeerNode::send_ready_if_quiescent() {
    if(!staged_ || ready_sent_ || !frozen_)return;
    if((replicator_ && replicator_->pending_count()!=0) || !snapshots_.empty())return;
    if(!connected_to_all(staged_->members()))return;
    auto payload=version_tag(*staged_);
    payload.resize(8+32,0); // bitmap of non-empty local region keys
    for(auto key:region_store_.all_keys())payload[8+key/8] |= static_cast<std::uint8_t>(1u<<(key%8));
    send_control(authority_id_,messages::Type::PLACEMENT_READY,payload);
    ready_sent_=true;
}

void PeerNode::on_handoff_message(net::TcpConnection& conn,
        const messages::Message& msg,core::NodeId sender) {
    if(!frozen_)throw std::runtime_error("handoff outside transition");
    switch(msg.type) {
    case messages::Type::HANDOFF_GET: {
        if(sender!=authority_id_ || !placement_ready_)
            throw std::runtime_error("unauthorized HANDOFF_GET");
        const auto key=messages::decode_partition_id(msg.payload);
        if(placement_.primary(key)!=config_.id)
            throw std::runtime_error("HANDOFF_GET from nonowner");
        const auto r=region_store_.get_region(key).value_or(core::Region{});
        send_control(sender,messages::Type::HANDOFF_DATA,messages::encode_region({key,r}));
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
        if(m.new_owner==config_.id) {
            region_store_.merge_region(m.key,r.region);
            ++next_move_;
            next_handoff();
        } else {
            phase_=Phase::Installing;
            send_control(m.new_owner,messages::Type::HANDOFF_PUT,msg.payload);
        }
        break;
    }
    case messages::Type::HANDOFF_OK: {
        if(config_.id!=authority_id_ || phase_!=Phase::Installing ||
           next_move_>=moves_.size() || sender!=moves_[next_move_].new_owner ||
           messages::decode_partition_id(msg.payload)!=moves_[next_move_].key)
            throw std::runtime_error("unexpected HANDOFF_OK");
        ++next_move_;
        next_handoff();
        break;
    }
    default: (void)conn;throw std::runtime_error("invalid handoff message");
    }
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
    case T::JOIN_REQUEST:
        if(config_.id!=authority_id_ || sender==0 ||
           !msg.payload.empty())throw std::runtime_error("unauthorized join request");
        schedule_join(sender);
        return;
    case T::PLACEMENT_PREPARE: {
        if(sender!=authority_id_ || frozen_ || staged_)
            throw std::runtime_error("unexpected placement PREPARE");
        auto next=messages::decode_placement(msg.payload);
        if(placement_ready_ && next.epoch()!=placement_.epoch()+1)
            throw std::runtime_error("nonconsecutive placement generation");
        if(!next.contains(config_.id) &&
           (!placement_ready_ || !placement_.contains(config_.id)))
            throw std::runtime_error("PREPARE for unrelated peer");
        // Durable, fail-closed prepare marker. A restart during transition
        // does not silently enable old routing.
        if(auto dir=std::filesystem::path(config_.state_file).parent_path();!dir.empty())
            std::filesystem::create_directories(dir);
        next.save(config_.state_file+".pending");
        frozen_=true;
        staged_=std::move(next);
        ready_sent_=false;
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
        if(config_.id!=authority_id_ || phase_!=Phase::Preparing ||
           !waiting_.contains(sender))
            throw std::runtime_error("unexpected PLACEMENT_READY");
        if(msg.payload.size()!=40)throw std::runtime_error("invalid READY bitmap size");
        check_tag(std::span<const std::uint8_t>(msg.payload.data(),8),*staged_);
        std::set<std::uint32_t> present;
        for(std::uint32_t k=0;k<256;++k)
            if(msg.payload[8+k/8] & (1u<<(k%8)))present.insert(k);
        transition_keys_[sender]=std::move(present);
        waiting_.erase(sender);
        progress_transition();
        return;
    }
    case T::PLACEMENT_COMMIT: {
        if(sender!=authority_id_ || !frozen_ || !staged_)
            throw std::runtime_error("unexpected placement COMMIT");
        auto incoming=messages::decode_placement(msg.payload);
        if(incoming.fingerprint()!=staged_->fingerprint())
            throw std::runtime_error("commit does not match prepared placement");
        placement_manager_.install_committed(incoming);
        set_committed(placement_manager_.active());
        staged_.reset();
        send_control(sender,T::PLACEMENT_COMMITTED,version_tag(placement_));
        return;
    }
    case T::PLACEMENT_COMMITTED:
        if(config_.id!=authority_id_ || phase_!=Phase::Committing ||
           !waiting_.contains(sender))throw std::runtime_error("unexpected commit ACK");
        check_tag(msg.payload,placement_);
        waiting_.erase(sender);
        if(waiting_.empty()) {
            send_transition_message(T::PLACEMENT_RELEASE,version_tag(placement_));
            frozen_=false;
            staged_.reset();
            moves_.clear();
            transition_peers_.clear();
            phase_=Phase::Idle;
            std::cout<<"[membership] RELEASE generation "<<placement_.epoch()<<'\n';
            broadcast_endpoints();
        }
        return;
    case T::PLACEMENT_RELEASE:
        if(sender!=authority_id_ || !frozen_ || staged_)
            throw std::runtime_error("unexpected placement RELEASE");
        check_tag(msg.payload,placement_);
        std::filesystem::remove(config_.state_file+".pending");
        frozen_=false;
        ready_sent_=false;
        std::cout<<"[membership] RELEASE, active="<<placement_.epoch()<<'\n';
        if(!placement_ready_) {
            std::cout<<"[membership] graceful departure completed; stopping peer\n";
            loop_.stop();
        }
        return;
    case T::PLACEMENT_ABORT:
        if(sender!=authority_id_ || !frozen_)
            throw std::runtime_error("unexpected placement ABORT");
        if(staged_) {
            std::filesystem::remove(config_.state_file+".pending");
            staged_.reset();frozen_=false;ready_sent_=false;
        }
        else throw std::runtime_error("cannot abort after commit; fail closed");
        return;
    case T::PEER_ENDPOINTS:
        if(sender!=authority_id_)
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

void PeerNode::connect_peer(core::NodeId id,const std::string& host,std::uint16_t port) {
    if(connecting_.contains(id) || fd_by_node_.contains(id))return;
    connecting_.insert(id);
    next_dial_[id]=Clock::now()+std::chrono::seconds(5);
    try {
        auto result=net::TcpClient::connect_to(host,port);
        if(result.status==net::ConnectStatus::Connected) {
            connecting_.erase(id);
            on_accepted(std::move(result.socket));
        } else {
            loop_.add_pending_connection(std::move(result.socket),
                [this,id](net::Socket sock) {
                    connecting_.erase(id);
                    on_accepted(std::move(sock));
                },
                [this,id](int error) {
                    connecting_.erase(id);
                    std::cerr<<"[discovery] connect to "<<id<<" failed: "
                             <<std::system_category().message(error)<<'\n';
                });
        }
    } catch(const std::exception& e) {
        connecting_.erase(id);
        std::cerr<<"[discovery] "<<e.what()<<'\n';
    }
}
void PeerNode::connect_discovered_peers() {
    if(config_.id==authority_id_)return;
    // Lower IDs dial higher IDs to avoid duplicate connections. All joiners
    // explicitly seed the authority even when their ID is greater.
    const auto now=Clock::now();
    for(const auto& peer:peers_.all()) {
        if(placement_ready_ && !placement_.contains(peer.id) &&
           (!staged_ || !staged_->contains(peer.id)))continue;
        if((peer.id<=config_.id && peer.id!=authority_id_) ||
           peer.peer_port==0 || peer.host.empty() ||
           fd_by_node_.contains(peer.id) || connecting_.contains(peer.id))continue;
        if(next_dial_[peer.id]>now)continue;
        connect_peer(peer.id,peer.host,peer.peer_port);
    }

}
} // namespace p2place::peer
