#pragma once
#include <cstdint>
#include <cstddef>
namespace p2place::replication {
struct QuorumConfig {
    std::size_t N=3,R=2,W=2;
    void validate() const;
};
// Same storage/transport manager for Strategies A, B and C.
// B/C will implement a different plan() without replacing the manager.
class ReplicationStrategy {
public:
    virtual ~ReplicationStrategy()=default;
    virtual QuorumConfig plan(std::uint32_t region_key) const=0;
};
class FixedReplicationStrategy final:public ReplicationStrategy {
public:
    explicit FixedReplicationStrategy(QuorumConfig config):config_(config){config_.validate();}
    QuorumConfig plan(std::uint32_t)const override{return config_;}
private:
    QuorumConfig config_;
};
}
