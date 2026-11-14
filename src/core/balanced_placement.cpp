#include "core/balanced_placement.hpp"
#include <algorithm>
#include <fstream>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

namespace p2place::core {
namespace {
void normalize(std::vector<NodeId>& ids) {
    if (std::any_of(ids.begin(), ids.end(), [](NodeId n){ return n == 0; }))
        throw std::invalid_argument("node ID 0 is reserved");
    std::sort(ids.begin(), ids.end());
    if (std::adjacent_find(ids.begin(), ids.end()) != ids.end())
        throw std::invalid_argument("duplicate node IDs");
    if (ids.empty() || ids.size() > BalancedPlacement::PARTITIONS)
        throw std::invalid_argument("need 1..256 distinct nodes");
}
}
BalancedPlacement BalancedPlacement::create(std::vector<NodeId> ids) {
    normalize(ids);
    BalancedPlacement p;
    p.epoch_ = 1;
    p.members_ = std::move(ids);
    for (std::size_t i = 0; i < p.members_.size(); ++i)
        p.admission_[p.members_[i]] = i + 1;
    for (std::size_t i = 0; i < PARTITIONS; ++i)
        p.owners_[i] = p.members_[i % p.members_.size()];
    p.validate();
    return p;
}
BalancedPlacement BalancedPlacement::from_snapshot(
    std::uint64_t generation, std::vector<NodeId> members, Owners owners,
    std::map<NodeId, std::uint64_t> admission) {
    if (generation == 0) throw std::invalid_argument("generation must be positive");
    BalancedPlacement p;
    p.epoch_ = generation;
    p.members_ = std::move(members);
    p.owners_ = std::move(owners);
    if (admission.empty()) { // Legacy V1 snapshot: deterministic ID-order migration.
        for (std::size_t i=0; i<p.members_.size(); ++i) admission[p.members_[i]] = i+1;
    }
    p.admission_ = std::move(admission);
    p.validate();
    return p;
}
bool BalancedPlacement::contains(NodeId id) const {
    return std::binary_search(members_.begin(), members_.end(),id);
}
NodeId BalancedPlacement::primary(std::uint32_t part) const {
    if (part >= PARTITIONS) throw std::out_of_range("invalid region/partition");
    if (members_.empty()) throw std::logic_error("placement uninitialized");
    return owners_[part];
}
std::vector<NodeId> BalancedPlacement::members() const { return members_; }
std::uint64_t BalancedPlacement::admission_order(NodeId id) const {
    auto it=admission_.find(id);
    if(it==admission_.end()) throw std::out_of_range("unknown member admission");
    return it->second;
}
std::map<NodeId, std::size_t> BalancedPlacement::primary_counts() const {
    std::map<NodeId, std::size_t> counts;
    for (NodeId n : members_) counts[n] = 0;
    for (NodeId n : owners_) if (n) ++counts[n];
    return counts;
}
std::vector<NodeId> BalancedPlacement::preference(std::uint32_t part,
                                                   std::size_t limit) const {
    const NodeId owner = primary(part);
    std::vector<NodeId> result;
    if (limit == 0) return result;
    const auto it = std::lower_bound(members_.begin(), members_.end(), owner);
    if (it == members_.end() || *it != owner)
        throw std::logic_error("owner absent from membership");
    const std::size_t start = static_cast<std::size_t>(it - members_.begin());
    const std::size_t count = std::min(limit, members_.size());
    for (std::size_t j = 0; j < count; ++j)
        result.push_back(members_[(start + j) % members_.size()]);
    return result;
}
void BalancedPlacement::validate() const {
    auto ids = members_; normalize(ids);
    if (ids != members_) throw std::invalid_argument("members must be sorted");
    if (admission_.size() != members_.size())
        throw std::invalid_argument("missing admission records");
    std::set<std::uint64_t> seen_orders;
    for (auto id : members_) {
        auto it=admission_.find(id);
        if (it==admission_.end() || it->second==0 || !seen_orders.insert(it->second).second)
            throw std::invalid_argument("invalid/duplicate admission order");
    }
    std::map<NodeId, std::size_t> counts;
    for (NodeId id : members_) counts[id] = 0;
    for (NodeId owner : owners_) {
        auto it = counts.find(owner);
        if (it == counts.end()) throw std::invalid_argument("unknown partition owner");
        ++it->second;
    }
    std::size_t minimum = PARTITIONS, maximum = 0;
    for (const auto& [id,count] : counts) {
        (void)id;
        minimum = std::min(minimum,count); maximum = std::max(maximum,count);
    }
    if (maximum - minimum > 1) throw std::invalid_argument("unbalanced primary ownership");
}
std::size_t BalancedPlacement::plan_join(NodeId id) {
    if (id == 0) throw std::invalid_argument("node ID 0 reserved");
    if (std::binary_search(members_.begin(),members_.end(),id))
        throw std::invalid_argument("node already joined");
    BalancedPlacement next = *this;
    next.members_.push_back(id);
    normalize(next.members_);
    std::uint64_t maximum=0;
    for (auto [n,order] : admission_) { (void)n; maximum=std::max(maximum,order); }
    if (maximum==UINT64_MAX) throw std::overflow_error("admission sequence exhausted");
    next.admission_[id]=maximum+1;
    const auto targets = [&]{
        std::map<NodeId,std::size_t> r;
        const std::size_t q = PARTITIONS / next.members_.size();
        const std::size_t extra = PARTITIONS % next.members_.size();
        // Keep incumbents at high quota where possible, to minimize movement.
        // A new node receives floor(PARTITIONS / new_members) initially.
        for (NodeId n : next.members_) r[n] = q;
        std::size_t remaining = extra;
        for (NodeId n : members_) if (remaining) { ++r[n]; --remaining; }
        if (remaining) ++r[id];
        return r;
    }();
    auto counts = next.primary_counts();
    std::size_t moved=0;
    // For join only, old owners donate surplus to the new node.
    for (std::size_t part = 0; part < PARTITIONS; ++part) {
        const auto donor = next.owners_[part];
        if (counts[donor] > targets.at(donor) && counts[id] < targets.at(id)) {
            next.owners_[part] = id;
            --counts[donor]; ++counts[id]; ++moved;
        }
    }
    if (counts[id] != targets.at(id)) throw std::logic_error("rebalance failed");
    if (epoch_ == UINT64_MAX) throw std::overflow_error("epoch exhausted");
    next.epoch_++;
    next.validate();
    *this = std::move(next);
    return moved;
}
std::size_t BalancedPlacement::plan_leave(NodeId id) {
    if (!std::binary_search(members_.begin(),members_.end(),id))
        throw std::invalid_argument("node not present");
    if (members_.size() == 1)
        throw std::invalid_argument("cannot remove final node");
    BalancedPlacement next = *this;
    next.members_.erase(std::lower_bound(next.members_.begin(),next.members_.end(),id));
    next.admission_.erase(id);
    std::map<NodeId,std::size_t> counts;
    for (NodeId n : next.members_) counts[n] = 0;
    std::size_t moved = 0;
    for (NodeId owner : next.owners_) if (owner != id) ++counts[owner];
    const auto target = [&](NodeId n){
        const auto idx = std::lower_bound(next.members_.begin(),next.members_.end(),n)-next.members_.begin();
        return PARTITIONS/next.members_.size() +
            (static_cast<std::size_t>(idx) < PARTITIONS%next.members_.size());
    };
    // Reassign removed owner's partitions to underfull surviving nodes first.
    for (NodeId& owner : next.owners_) if (owner == id) {
        auto it = std::find_if(next.members_.begin(),next.members_.end(),
            [&](NodeId n){ return counts[n] < target(n); });
        if (it == next.members_.end()) throw std::logic_error("no receiver");
        owner = *it; ++counts[*it]; ++moved;
    }
    // Sometimes post-leave quota rounding requires a few incumbent-to-incumbent moves.
    for (NodeId& owner : next.owners_) {
        if (counts[owner] <= target(owner)) continue;
        auto it = std::find_if(next.members_.begin(),next.members_.end(),
            [&](NodeId n){ return counts[n] < target(n); });
        if (it != next.members_.end()) {
            --counts[owner]; owner = *it; ++counts[*it]; ++moved;
        }
    }
    if (epoch_ == UINT64_MAX) throw std::overflow_error("epoch exhausted");
    next.epoch_++;
    next.validate();
    *this = std::move(next);
    return moved;
}
std::uint64_t BalancedPlacement::fingerprint() const noexcept {
    // Stable FNV-1a 64-bit checksum; not a cryptographic signature.
    std::uint64_t h=14695981039346656037ULL;
    auto feed = [&](std::uint64_t v, int bytes) {
        for (int i=0;i<bytes;++i) { h ^= (v >> (i*8)) & 255u; h *= 1099511628211ULL; }
    };
    feed(epoch_,8); feed(members_.size(),4);
    for (NodeId n:members_) {feed(n,4); feed(admission_.at(n),8);}
    for (NodeId n:owners_) feed(n,4);
    return h;
}
void BalancedPlacement::save(const std::filesystem::path& file) const {
    validate();
    // Write temp in same directory, then rename; caller must coordinate cluster-wide activation.
    const auto tmp = file.string()+".tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        if (!out) throw std::runtime_error("cannot create placement file");
        out << "P2PLACE_BALANCED_V2 " << epoch_ << ' ' << members_.size() << '\n';
        for (NodeId n:members_) out << n << ' ';
        out << '\n';
        for (NodeId n:members_) out << admission_.at(n) << ' ';
        out << '\n';
        for (std::size_t i=0;i<PARTITIONS;++i) out << owners_[i] << (i%16==15?'\n':' ');
        out << "CHECKSUM " << fingerprint() << '\n';
        out.flush();
        if (!out) throw std::runtime_error("placement write failed");
    }
    std::filesystem::rename(tmp,file);
}
BalancedPlacement BalancedPlacement::load(const std::filesystem::path& file) {
    std::ifstream in(file);
    if (!in) throw std::runtime_error("cannot open placement file: "+file.string());
    std::string magic;
    BalancedPlacement p;
    std::size_t size=0;
    if (!(in >> magic >> p.epoch_ >> size) || (magic!="P2PLACE_BALANCED_V1" && magic!="P2PLACE_BALANCED_V2") ||
        p.epoch_==0 || size==0 || size>PARTITIONS)
        throw std::runtime_error("invalid placement header");
    for(std::size_t i=0;i<size;++i) {
        std::uint64_t id=0;
        if (!(in>>id) || id==0 || id>UINT32_MAX) throw std::runtime_error("invalid member");
        p.members_.push_back(static_cast<NodeId>(id));
    }
    if (magic=="P2PLACE_BALANCED_V2") {
        for (NodeId n:p.members_) {
            std::uint64_t order=0;
            if (!(in>>order) || order==0) throw std::runtime_error("invalid admission order");
            p.admission_[n]=order;
        }
    } else {
        // Old file never stored admission order; sorted ID order is the only
        // deterministic fallback. Real historical join order cannot be recovered.
        for (std::size_t i=0;i<p.members_.size();++i)
            p.admission_[p.members_[i]]=i+1;
    }
    for(NodeId& owner:p.owners_) {
        std::uint64_t id=0;
        if(!(in>>id) || id==0 || id>UINT32_MAX) throw std::runtime_error("invalid owner");
        owner=static_cast<NodeId>(id);
    }
    std::string check;
    std::uint64_t digest=0;
    if(!(in>>check>>digest) || check!="CHECKSUM")
        throw std::runtime_error("missing placement checksum");
    std::string trailing;
    if(in>>trailing) throw std::runtime_error("trailing placement data");
    p.validate();
    if (magic=="P2PLACE_BALANCED_V1") {
        // V1 checksum did not include admission records. Verify legacy digest.
        std::uint64_t h=14695981039346656037ULL;
        auto feed=[&](std::uint64_t v,int bytes){for(int i=0;i<bytes;++i){h^=(v>>(i*8))&255u;h*=1099511628211ULL;}};
        feed(p.epoch_,8);feed(p.members_.size(),4);
        for(NodeId n:p.members_)feed(n,4);
        for(NodeId n:p.owners_)feed(n,4);
        if(h!=digest) throw std::runtime_error("legacy placement checksum mismatch");
    } else if(p.fingerprint()!=digest) throw std::runtime_error("placement checksum mismatch");
    return p;
}
} // namespace p2place::core
