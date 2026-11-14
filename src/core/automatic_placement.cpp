#include "core/automatic_placement.hpp"
#include <algorithm>
#include <stdexcept>
#include <utility>

namespace p2place::core {
AutomaticPlacement::AutomaticPlacement(std::filesystem::path path)
    : state_file_(std::move(path)) {
    if (state_file_.empty()) throw std::invalid_argument("empty placement state path");
}
void AutomaticPlacement::bootstrap(NodeId self) {
    if (active_) throw std::logic_error("placement already initialized");
    if (std::filesystem::exists(state_file_)) {
        active_ = BalancedPlacement::load(state_file_);
        // A previously departed peer can restart and rejoin using its saved
        // committed placement. The owner will be excluded until approved.
        // Do not invent a new generation locally.
    } else {
        auto p = BalancedPlacement::create({self});
        persist(p);
        active_ = std::move(p);
    }
}
void AutomaticPlacement::require_ready() const {
    if (!active_) throw std::logic_error("placement not initialized");
}
const BalancedPlacement& AutomaticPlacement::active() const {
    require_ready(); return *active_;
}
std::uint64_t AutomaticPlacement::generation() const {
    return active().epoch(); // Internal monotonic generation, never a manual epoch file.
}
std::size_t AutomaticPlacement::propose_join(NodeId id) {
    require_ready();
    if (pending_) throw std::logic_error("placement transition already pending");
    auto candidate = *active_;
    auto changed = candidate.plan_join(id);
    pending_ = std::move(candidate);
    return changed;
}
std::size_t AutomaticPlacement::propose_leave(NodeId id) {
    require_ready();
    if (pending_) throw std::logic_error("placement transition already pending");
    auto candidate = *active_;
    auto changed = candidate.plan_leave(id);
    pending_ = std::move(candidate);
    return changed;
}
void AutomaticPlacement::abort() noexcept { pending_.reset(); }
void AutomaticPlacement::activate() {
    require_ready();
    if (!pending_) throw std::logic_error("no prepared placement");
    // Persist before publishing the new ownership view. A failure to persist
    // leaves current in-memory placement unchanged.
    persist(*pending_);
    active_ = std::move(*pending_);
    pending_.reset();
}
void AutomaticPlacement::install_committed(const BalancedPlacement& incoming) {
    incoming.validate();
    if (active_) {
        if (incoming.epoch() < active_->epoch())
            throw std::runtime_error("stale placement generation");
        if (incoming.epoch() == active_->epoch()) {
            if (incoming.fingerprint() != active_->fingerprint())
                throw std::runtime_error("conflicting placement at same generation");
            return;
        }
    }
    // Transport must authenticate authority and enforce handoff barrier.
    persist(incoming);
    active_ = incoming;
    pending_.reset();
}
void AutomaticPlacement::persist(const BalancedPlacement& value) const {
    if (auto dir = state_file_.parent_path(); !dir.empty())
        std::filesystem::create_directories(dir);
    value.save(state_file_);
}
} // namespace p2place::core
