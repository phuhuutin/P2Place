#include "peer/metrics.hpp"
#include <array>
#include <ctime>
#include <system_error>
namespace p2place::peer {
namespace {
std::string node_suffix(std::uint32_t node) {
    return "node" + std::to_string(node) + ".csv";
}
constexpr std::uint32_t FLUSH_ROWS = 32;
}
Metrics::Metrics(std::string run_id, std::filesystem::path dir, std::uint32_t node)
    : run_id_(std::move(run_id)), node_(node) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    health_.open(dir / ("peer_health_" + node_suffix(node_)));
    policy_.open(dir / ("policy_" + node_suffix(node_)));
    ops_.open(dir / ("operations_" + node_suffix(node_)));
    if(!health_ || !policy_ || !ops_)
        throw std::runtime_error("cannot open metrics CSV files in " + dir.string());
    health_ << "run_id,time_s,node_id,state,observed_seconds,availability_score\n";
    policy_ << "run_id,time_s,placement_gen,policy_epoch,region,strategy,"
               "risk_score,desired_n,assigned_n,confirmed_live_copies\n";
    ops_ << "run_id,time_s,op,region,success,latency_ms,error\n";
    // Timeline series: absolute monotonic ns for runner-side clock alignment.
    replication_events_.out.open(dir / "region_replication_events.csv");
    if(replication_events_.out)
        replication_events_.out
            << "run_id,monotonic_ns,event_type,region,old_n,new_n,placement_gen,"
               "placement_fp,policy_epoch,authority_node,reason\n";
    placement_snapshots_.out.open(dir / "placement_snapshots.jsonl");
    backfill_events_.out.open(dir / "replica_backfill_events.csv");
    if(backfill_events_.out)
        backfill_events_.out
            << "run_id,monotonic_ns,node_id,region,placement_gen,policy_epoch,"
               "result,reason\n";
}
Metrics::~Metrics() = default;

std::uint64_t Metrics::monotonic_ns() {
    timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::uint64_t>(ts.tv_sec) * 1000000000ULL +
           static_cast<std::uint64_t>(ts.tv_nsec);
}

std::string Metrics::csv_escape(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 2);
    for(char c : s) {
        if(c == ',' || c == '"' || c == '\n')out += ' ';
        else out += c;
    }
    return out;
}

void Metrics::health(double time_s, std::uint32_t peer, std::string_view state,
                     double observed_seconds, double availability_score) {
    if(!health_)return;
    health_ << run_id_ << ',' << time_s << ',' << peer << ','
            << csv_escape(state) << ',' << observed_seconds << ','
            << availability_score << '\n';
    health_.flush();
}
void Metrics::policy(double time_s, std::uint64_t gen, std::uint64_t epoch,
                     std::uint32_t region, char strategy, double risk,
                     std::uint8_t desired_n, std::uint8_t assigned_n,
                     std::size_t confirmed_live_copies) {
    if(!policy_)return;
    policy_ << run_id_ << ',' << time_s << ',' << gen << ',' << epoch << ','
            << region << ',' << strategy << ',' << risk << ','
            << static_cast<unsigned>(desired_n) << ','
            << static_cast<unsigned>(assigned_n) << ','
            << confirmed_live_copies << '\n';
    policy_.flush();
}
void Metrics::operation(double time_s, std::string_view op, std::uint32_t region,
                        bool success, double latency_ms, std::string_view error) {
    if(!ops_)return;
    ops_ << run_id_ << ',' << time_s << ',' << csv_escape(op) << ','
         << region << ',' << (success ? "true" : "false") << ','
         << latency_ms << ',' << csv_escape(error) << '\n';
    ops_.flush();
}

void Metrics::replication_initial(const std::array<std::uint8_t, 256>& target_n,
                                  std::uint64_t gen, std::uint64_t fp,
                                  std::uint64_t epoch, std::uint32_t authority) {
    if(!replication_events_.out)return;
    const auto ns=monotonic_ns();
    std::string line;
    for(std::uint32_t r=0;r<target_n.size();++r) {
        line = run_id_ + ',' + std::to_string(ns) + ",initial," +
               std::to_string(r) + ',';
        line += ','; // old_n empty
        line += std::to_string(target_n[r]);
        line += ',' + std::to_string(gen) + ',' + std::to_string(fp) + ',' +
                std::to_string(epoch) + ',' + std::to_string(authority) +
                ",initial_snapshot\n";
        replication_events_.out << line; // one write per row: a SIGTERM
                                         // between rows truncates only the
                                         // final line (handled by the reader)
        if(++replication_events_.rows_since_flush>=FLUSH_ROWS)
            replication_events_.flush();
    }
}
void Metrics::replication_n_change(std::uint32_t region, std::uint8_t old_n,
                                   std::uint8_t new_n, std::uint64_t gen,
                                   std::uint64_t fp, std::uint64_t epoch,
                                   std::uint32_t authority, std::string_view reason) {
    if(!replication_events_.out)return;
    const std::string line =
        run_id_ + ',' + std::to_string(monotonic_ns()) + ",n_change," +
        std::to_string(region) + ',' + std::to_string(old_n) + ',' +
        std::to_string(new_n) + ',' + std::to_string(gen) + ',' +
        std::to_string(fp) + ',' + std::to_string(epoch) + ',' +
        std::to_string(authority) + ',' + csv_escape(reason) + '\n';
    replication_events_.out << line;
    replication_events_.flush(); // rare event: durable immediately
}
void Metrics::replication_placement_change(std::uint64_t gen, std::uint64_t fp,
                                           std::uint64_t epoch,
                                           std::uint32_t authority,
                                           std::string_view reason) {
    if(!replication_events_.out)return;
    const std::string line =
        run_id_ + ',' + std::to_string(monotonic_ns()) + ",placement_change,,,," +
        std::to_string(gen) + ',' + std::to_string(fp) + ',' +
        std::to_string(epoch) + ',' + std::to_string(authority) + ',' +
        csv_escape(reason) + '\n';
    replication_events_.out << line;
    replication_events_.flush(); // rare event: durable immediately
}
void Metrics::placement_snapshot(std::uint64_t gen, std::uint64_t fp,
                                 std::uint32_t authority,
                                 const std::vector<std::uint32_t>& members) {
    if(!placement_snapshots_.out)return;
    std::string ids;
    for(std::size_t i=0;i<members.size();++i) {
        if(i)ids += ',';
        ids += std::to_string(members[i]);
    }
    placement_snapshots_.out
        << "{\"monotonic_ns\":" << monotonic_ns()
        << ",\"placement_gen\":" << gen
        << ",\"placement_fp\":" << fp
        << ",\"authority_node\":" << authority
        << ",\"member_ids\":[" << ids << "]}\n";
    placement_snapshots_.flush(); // rare event: durable immediately
}
void Metrics::backfill(std::uint32_t region, bool ok, std::uint64_t gen,
                       std::uint64_t epoch, std::string_view reason) {
    if(!backfill_events_.out)return;
    const std::string line =
        run_id_ + ',' + std::to_string(monotonic_ns()) + ',' +
        std::to_string(node_) + ',' + std::to_string(region) + ',' +
        std::to_string(gen) + ',' + std::to_string(epoch) + ',' +
        (ok ? "ok" : "failed") + ',' + csv_escape(reason) + '\n';
    backfill_events_.out << line;
    backfill_events_.flush(); // rare event: durable immediately
}
}
