#pragma once
#include "core/node_info.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <vector>

namespace p2place::core {
// P2Place's 256 canvas region IDs ARE the 256 fixed partitions.
// Ownership is a persistent, versioned mapping. Liveness never changes it.
class BalancedPlacement final {
public:
    static constexpr std::uint32_t PARTITIONS = 256;
    using Owners = std::array<NodeId, PARTITIONS>;

    // Deterministic initial round-robin placement, with count difference <= 1.
    static BalancedPlacement create(std::vector<NodeId> ids);
    static BalancedPlacement load(const std::filesystem::path& file);
    static BalancedPlacement from_snapshot(std::uint64_t generation,
        std::vector<NodeId> members, Owners owners,
        std::map<NodeId, std::uint64_t> admission = {});
    void save(const std::filesystem::path& file) const;

    // OFFLINE PLANNING operations. Do NOT activate until migration is complete.
    std::size_t plan_join(NodeId id);
    std::size_t plan_leave(NodeId id);

    bool contains(NodeId id) const;
    NodeId primary(std::uint32_t partition) const;
    std::vector<NodeId> preference(std::uint32_t partition,
                                   std::size_t limit) const;
    [[nodiscard]] std::vector<NodeId> members() const;
    [[nodiscard]] std::uint64_t admission_order(NodeId id) const;
    [[nodiscard]] const std::map<NodeId, std::uint64_t>& admissions() const noexcept { return admission_; }
    [[nodiscard]] std::map<NodeId, std::size_t> primary_counts() const;
    [[nodiscard]] std::uint64_t epoch() const noexcept { return epoch_; }
    [[nodiscard]] const Owners& owners() const noexcept { return owners_; }
    [[nodiscard]] std::uint64_t fingerprint() const noexcept;
    void validate() const;

private:
    std::uint64_t epoch_ = 0;
    std::vector<NodeId> members_;
    Owners owners_{};
    std::map<NodeId, std::uint64_t> admission_;
};
} // namespace p2place::core
