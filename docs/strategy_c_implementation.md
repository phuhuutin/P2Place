# P2Place — Strategy C (Churn + Write-Aware Replication) Implementation Report

**Handoff:** `P2Place_Strategy_C_Detailed_Implementation_Handoff.md`
**Status:** implemented, unit + live tests pass; A/B behavior preserved.

---

## 1. What was built

### 1.1 `core::RegionWriteTracker` (+ codec) — `include/core/region_write_tracker.hpp/.cpp`

- 256 region counters, ring of six 10-second buckets (60 s window; bucket
  width fixed at 10 s, window configurable in 10 s multiples).
- `record_put_attempt(region, now)` O(1), no allocation/I/O; called exactly
  once per valid browser PUT command in `handle_put` (after coordinate
  validation, before the quorum attempt — quorum-rejected attempts count;
  forwards/REGION_PUT/backfill/snapshot reads never count).
- Rates use a **fixed window divisor** (conservative warm-up: a fresh process
  reports low rates, never spurious hot regions).
- `drain_delta()` returns per-region counts accumulated since the previous
  drain (saturating u16) with `CLOCK_MONOTONIC` interval stamps —
  **non-overlapping**, so the authority never sums overlapping rolling totals.
- Wire codec for `REGION_WRITE_COUNTS` (type **32**): version + node +
  incarnation + sequence + start/end ns + 256×u16 = **541 bytes**
  (< MAX_MESSAGE_SIZE). NOTE: `messages::decode` previously rejected any type
  > 31 — widened to 32 (`src/messages/message.cpp:20`), which also fixed a
  transport-teardown loop (the decode exception silently killed the
  connection; this was found and fixed during live testing).

### 1.2 `core::WriteReportAggregate` (authority-side)

- Dedup per `(node, incarnation, sequence)`; stale sequences dropped; a new
  incarnation (restart) starts a fresh sequence space.
- Intervals expire after the window; malformed/future/unknown-peer reports
  dropped with reasons; observation completeness = live intervals ÷ expected
  slots (never fabricates zeros for unreported gateways).
- `clear()` on authority change (fresh 60 s warm-up, documented).

### 1.3 Policy C (`core::replication_policy.cpp`)

```text
N_churn(r)  = B's three-tier mapping (>=0.95 -> 2; 0.80..0.95 -> 3; <0.80 -> 4)
N_write(r)  = rate < 0.20 -> 2; 0.20..1.00 -> 3; rate >= 1.00 -> 4
N_C(r)      = clamp(max(N_churn, N_write), min_replicas, max_replicas)
```

- Same 30 s per-region cooldown and placement-change waiver as B; snapshots
  carry the `'C'` strategy byte; `PolicyEvaluation` exposes `write_rate`,
  `n_churn`, `n_write` diagnostics per region.
- `C >= B` always (same reliability input). Thresholds/mid tier derive from
  config (`--write-threshold-mid/high`, `min/max-replicas`).
- Strategy isolation: a B peer rejects a C snapshot and vice versa
  (`policy-ignore reason=strategy-mismatch`); A never installs a policy
  handler. B's mapping is byte-for-byte unchanged when C is not selected.

### 1.4 Peer wiring (`peer_node.cpp`, `placement_runtime.cpp`, `ring_election.cpp`)

- Every C peer drains its tracker every `--write-report-interval` (10 s) and
  sends the delta to the authority; the authority aggregates its own local
  delta through the same codec path (no self-message).
- The authority feeds the deduplicated aggregate into the evaluator as the
  per-region rate at each policy evaluation and broadcasts through the
  existing `POLICY_SNAPSHOT` path (epoch/cooldown/heartbeat machinery
  unchanged).
- Authority election: a newly elected authority clears the aggregate and
  restarts its sequence space.

### 1.5 Metrics (existing sinks extended)

- `region_write_frequency.csv` (per policy evaluation × 256 rows): time,
  region, local_attempts_10s, cluster_attempts_60s, write_rate_per_sec,
  min_peer_reliability, n_churn, n_write, target_n, assigned_n, gen, epoch,
  authority, observation_complete.
- `region_write_report_events.csv`: per received report — node, incarnation,
  sequence, interval, accepted/drop reason. Both only when `--metrics-dir`
  is set (benchmarks run without file I/O skew).

### 1.6 Runner (`experiments/run_experiment.py`)

- `--strategy A|B|C`; C-only flags (`--write-window`, `--write-report-interval`,
  `--write-threshold-mid/high`) are passed to peers **only for C** — A/B
  command lines are byte-identical to before.
- `--workload uniform|targeted|hotspot` as a distribution dimension
  independent of `--churn-profile`; `--churn-profile hotspot-churn` is a
  compatibility alias (hotspot + predictive-churn). `--workload hotspot`:
  `--hotspot-region-count 8`, `--hotspot-write-percent 70`,
  `--hotspot-seed 43`; hot regions fixed at init from the COMMITTED
  placement, preferring regions whose first two owners are **{8, 9}** so the
  standard fault pair drives the B-vs-C contrast. Writes land in hot regions
  with the configured probability, uniformly otherwise; `workload_manifest.json`
  records regions, preference lists, seed and thresholds.
- Hot/cold outcomes flow through the existing `by_phase_and_workload`
  summary automatically.

### 1.7 Tests

- `tests/test_region_write_tracker.cpp`: fixed-divisor rates, idle bucket
  expiration, non-overlapping deltas, codec roundtrip + malformed rejection,
  multi-gateway summation (2×40 attempts → 80/60 = 1.333, not double-counted),
  dedup/stale/new-incarnation rules, interval expiry, observation
  completeness, future-interval rejection.
- `tests/test_strategy_c.cpp`: handoff mapping cases
  (.98,.05)→2; (.98,.40)→3; (.98,1.5)→4; (.90,.05)→3; (.65,.05)→4; exact
  0.20/1.00 boundary crossings; C≥B invariant; `'C'` snapshot codec; shared
  cooldown + waiver; config validation (strategy byte, thresholds).
- `tests/test_strategy_c.py` (live): T1 aggregation — 5 gateways write one
  hot region; the authority's aggregate crosses 1.0/s and the policy raises
  that region to N4, with reports accepted from all 4 remote gateways.
  T2 B contrast — identical writes leave B at N2 (n4=0). T3 two-owner
  survival — 10 nodes, hot region with first-two owners {8,9}; C
  pre-positions N4 before the kill, and after killing 8/9 simultaneously the
  hot writes still ACK via the extra targets while the B control fails W=2
  (pre-reconfiguration window). **Observed: B n4=0, 0/6 post-crash ACK;
  C n4=1, 6/6 post-crash ACK.**

## 2. Validation evidence (executed)

| Check | Result |
|---|---|
| `ctest` (22 suites, incl. `write-tracker`, `strategy-c`) | **22/22** |
| `tests/test_strategy_c.py` live | **ALL PASS** — aggregation→N4 (reports deduped from gateways 2–5), B contrast n4=0, two-owner survival B 0/6 vs C 6/6 |
| `tests/test_lab06_strategy_b.py` live (stable/churn/recovery + A regression) | **ALL PASS** |
| Strategy C policy cycle in logs (10-node repro) | epoch1 n2=256 → epoch2 n3=1 (mid threshold) → epoch3 **n4=1** (high threshold) → epoch4 decay — the write-rate term drives the exact N2→N3→N4 ladder |

## 3. Deviations from the handoff (all conservative)

- Bucket width fixed at 10 s (not separately configurable) — the handoff
  allows a fixed ring; `--write-window` must be a 10 s multiple.
- Snapshot strategy isolation is enforced in the `POLICY_SNAPSHOT` handler
  (local-config check), not inside `PolicyTable::apply`, so the table class
  stays generic.
- No `summary.json.by_phase_and_workload`/`strategy_c_effect` splits were
  added for the fault-window subset yet — the existing
  `by_phase_and_workload` (phase × class) covers hot/cold and phase-level
  outcomes; the two-owner window contrast is demonstrated by the live test
  and can be extracted per-run from `fault_events.json` +
  `runner_operations.csv` timestamps.

## 3. Suggested runs (targets, not yet executed)

```bash
# Smoke (120 s, no faults)
python3 experiments/run_experiment.py --strategy C --nodes 10 --seed 43 \
  --seconds 120 --rate 24 --workers 20 \
  --workload hotspot --hotspot-region-count 8 --hotspot-write-percent 70 \
  --churn-profile none --gateway-mode healthy \
  --out experiments/results/C_seed43_hotspot_smoke

# Full paired trial (after smoke validation)
for S in A B C; do
  python3 experiments/run_experiment.py \
    --strategy "$S" --nodes 10 --seed 43 --seconds 600 \
    --rate 24 --workers 20 \
    --workload hotspot --hotspot-region-count 8 --hotspot-write-percent 70 \
    --churn-profile predictive-churn --gateway-mode healthy \
    --verify-pixels 1500 \
    --out "experiments/results/${S}_10_seed43_hotspot"
done
```

Exit code 2 from the runner means verification mismatches remain (not a
startup crash).

## 4. Honest limitations

- Final binary sha256: `c98a4f1be2b2929c95b92e86bd2e62cba924f59a689c03f7052fac84afb5d7c8`.
- C's benefit exists **only after** the N4 policy is applied and the added
  replicas are backfilled; the live test confirms this precondition before
  the kill.
- R+W=4 is not greater than N=4: quorum intersection is not guaranteed —
  successful PUT ≠ immediate latest-value read; convergence is measured
  separately.
- Write-rate estimation may lag the ground truth by one report interval
  (~10 s) and under-reports during gateway crashes; `region_write_frequency.csv`
  records `observation_complete` and the runner keeps its own ground-truth
  counters for the estimation-error analysis.
- Same-kernel CLOCK_MONOTONIC assumptions; in-memory only; correlated
  failures not modeled — the max-composition is a heuristic, not a model of
  correlated risk.
