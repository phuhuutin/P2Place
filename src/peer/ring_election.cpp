#include "peer/peer_node.hpp"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>

namespace p2place::peer {
namespace {
using Clock = std::chrono::steady_clock;
constexpr auto RETRY = std::chrono::seconds(3);
constexpr auto DEADLINE = std::chrono::seconds(5);

void put32(std::vector<std::uint8_t>& b, std::uint32_t v) {
    for (int shift = 24; shift >= 0; shift -= 8)
        b.push_back(static_cast<std::uint8_t>(v >> shift));
}
void put64(std::vector<std::uint8_t>& b, std::uint64_t v) {
    for (int shift = 56; shift >= 0; shift -= 8)
        b.push_back(static_cast<std::uint8_t>(v >> shift));
}
std::uint32_t get32(const std::vector<std::uint8_t>& b, std::size_t off) {
    if (off + 4 > b.size()) throw std::runtime_error("short ring election packet");
    std::uint32_t v=0;
    for (std::size_t i=0;i<4;++i) v=(v<<8)|b[off+i];
    return v;
}
std::uint64_t get64(const std::vector<std::uint8_t>& b, std::size_t off) {
    if (off + 8 > b.size()) throw std::runtime_error("short ring election packet");
    std::uint64_t v=0;
    for (std::size_t i=0;i<8;++i) v=(v<<8)|b[off+i];
    return v;
}
// Ring token: generation(8), fingerprint(8), round(8), origin(4),
// oldest_member_candidate(4), visited(4). Every receiver verifies placement identity.
struct Token {std::uint64_t gen, fingerprint, round; core::NodeId origin, winner; std::uint32_t visited;};
std::vector<std::uint8_t> encode(Token t) {
    std::vector<std::uint8_t> b;
    put64(b,t.gen); put64(b,t.fingerprint); put64(b,t.round);
    put32(b,t.origin); put32(b,t.winner); put32(b,t.visited);
    return b;
}
Token decode(const std::vector<std::uint8_t>& b) {
    if(b.size()!=36)throw std::runtime_error("invalid ring election token length");
    return {get64(b,0),get64(b,8),get64(b,16),get32(b,24),get32(b,28),get32(b,32)};
}
}

std::size_t PeerNode::reachable_voters() const {
    if (!placement_ready_) return 0;
    std::size_t count=1;
    for (const auto id:placement_.members())
        if(id!=config_.id && fd_by_node_.contains(id)) ++count;
    return count;
}
bool PeerNode::has_majority() const {
    return placement_ready_ && reachable_voters()>placement_.members().size()/2;
}
core::NodeId PeerNode::preferred_initiator() const {
    core::NodeId oldest=config_.id;
    for (const auto id:placement_.members()) {
        if (!fd_by_node_.contains(id)) continue;
        if (placement_.admission_order(id) < placement_.admission_order(oldest))
            oldest=id;
    }
    return oldest;
}
std::optional<core::NodeId> PeerNode::next_ring_peer() const {
    if(!placement_ready_)return std::nullopt;
    const auto& members=placement_.members();
    auto it=std::upper_bound(members.begin(),members.end(),config_.id);
    for(std::size_t i=0;i<members.size()-1;++i) {
        if(it==members.end())it=members.begin();
        const auto id=*it++;
        if(id!=config_.id && fd_by_node_.contains(id))return id;
    }
    return std::nullopt;
}
void PeerNode::start_ring_election() {
    if(!has_majority() || frozen_ || phase_!=Phase::Idle ||
       preferred_initiator()!=config_.id) return;
    const auto next=next_ring_peer();
    if(!next)return;
    ++election_round_;
    election_in_progress_=true;
    election_deadline_=Clock::now()+DEADLINE;
    next_election_attempt_=Clock::now()+RETRY;
    const Token t{placement_.epoch(),placement_.fingerprint(),election_round_,
                  config_.id,config_.id,1};
    std::cout << "[election] RING start origin="<<config_.id
              <<" round="<<election_round_<<"\n";
    send_control(*next,messages::Type::RING_ELECTION,encode(t));
}
void PeerNode::election_tick() {
    if(!placement_ready_ || frozen_ || phase_!=Phase::Idle) return;
    const auto now=Clock::now();
    if(election_in_progress_ && now>=election_deadline_) {
        election_in_progress_=false;
        std::cerr<<"[election] round timed out\n";
    }
    if(fd_by_node_.contains(authority_id_))return;
    if(authority_id_==config_.id) {
        // An isolated authority may not authorize transitions without quorum.
        return;
    }
    if(now>=next_election_attempt_ && !election_in_progress_) {
        next_election_attempt_=now+RETRY;
        start_ring_election();
    }
}
void PeerNode::on_ring_election(const messages::Message& msg,core::NodeId sender) {
    auto t=decode(msg.payload);
    if(!placement_ready_ || frozen_ || phase_!=Phase::Idle ||
       t.gen!=placement_.epoch() || t.fingerprint!=placement_.fingerprint() ||
       !placement_.contains(sender) || !placement_.contains(t.origin) ||
       !placement_.contains(t.winner) ||
       t.visited>placement_.members().size())return;
    if(t.origin==config_.id) {
        if(!election_in_progress_ || t.round!=election_round_ ||
           t.visited<=placement_.members().size()/2)return;
        election_in_progress_=false;
        authority_id_=t.winner;
        last_elected_token_=encode(t);
        std::cout<<"[election] RING elected authority="<<authority_id_
                 <<" generation="<<t.gen<<"\n";
        // Announce clockwise; all participants learn the same elected leader.
        // The election is advisory until all peer connections/placement are healthy.
        auto next=next_ring_peer();
        if(next) send_control(*next,messages::Type::RING_ELECTED,encode(t));
        return;
    }
    if(placement_.admission_order(config_.id) <
       placement_.admission_order(t.winner) ||
       (placement_.admission_order(config_.id)==placement_.admission_order(t.winner)
        && config_.id<t.winner)) t.winner=config_.id;
    ++t.visited;
    auto next=next_ring_peer();
    if(next)send_control(*next,messages::Type::RING_ELECTION,encode(t));
}
void PeerNode::on_ring_elected(const messages::Message& msg,core::NodeId sender) {
    const auto t=decode(msg.payload);
    if(!placement_ready_ || t.gen!=placement_.epoch() ||
       t.fingerprint!=placement_.fingerprint() ||
       t.visited<=placement_.members().size()/2 ||
       !placement_.contains(sender) || !placement_.contains(t.winner))return;
    if(t.origin==config_.id)return; // announcement completed full circuit
    authority_id_=t.winner;
    last_elected_token_=msg.payload;
    election_in_progress_=false;
    std::cout<<"[election] learned authority="<<authority_id_<<"\n";
    const auto next=next_ring_peer();
    if(next)send_control(*next,messages::Type::RING_ELECTED,msg.payload);
}
} // namespace p2place::peer
