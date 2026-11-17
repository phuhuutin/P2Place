# B_10_seed43_extreme — Investigation Report

**Date:** 2026-11-15 · **Input artifact:** `experiments/results/B_10_seed43_extreme`
(copied read-only to `~/investigation_extreme/B_10_seed43_extreme`; original preserved)

---

## 1. Executive findings

- **Confirmed defect (Priority 1):** Node 6 was removed at generation 19 by a
  **stale queued `AUTO_REMOVE`**: the removal was enqueued while node 6 was
  down (t≈369 s), node 6 recovered and rejoined (gen 18, t≈384–394 s), and the
  un-revalidated queue entry then removed it anyway (~t≈395 s). Because node 6
  was *connected* when the removal committed, its RELEASE handler took the
  designed graceful-departure branch and **exited permanently** — the runner
  restarts each node only once, so the final placement has 9 members.
  Severity: high for experiment validity (one of ten nodes vanishes mid-run and
  stays down for the last ~200 s; 613 write attempts routed to it fail).
- **Everything else behaved as designed:** gens 17/20 are legitimate removals of
  downed nodes; gens 18/21 are legitimate rejoins (node 1's gen-20 removal and
  gen-21 rejoin included). No false quorum ACKs, no lost *acknowledged* writes
  in the 1,500-pixel sample.
- **Metrics recomputed from raw CSVs** (§3) match `summary.json` exactly and
  support a fair A/B comparison **once the stale-removal fix lands and the
  runner's final verification is strengthened** (§4, §5).
- **Fix implemented** (`progress_transition` dequeue re-validation) with a
  targeted regression test (§5) — result recorded below.

## 2. Node 6 root-cause timeline (confidence: high)

Log anchors: `node1_boot1.log` (authority, 0–414 s), `node6_boot2.log`
(384 s–exit). Line numbers from the preserved artifact.

| t (s) | Evidence | Meaning |
|---|---|---|
| 348 | node1: `queued JOIN 10` → `transition-start gen=18 …new=[…10]` | join-10 transition starts (legitimate) |
| 354.0 | fault `kill 6`; node1: `HEALTHY->SUSPECT` + `[disconnect] peer=6` | node 6 SIGKILLed mid-transition → transition aborts (designed) |
| ~369 | node1: `SUSPECT->UNAVAILABLE peer=6`; **`queued AUTO_REMOVE peer=6 queue_size=2`** | removal enqueued while 6 is down — **legitimate at enqueue time** |
| ~371 | node1: `queued JOIN 10` (re-referral) | queue = [(10,join),(6,auto-remove)] |
| 384.0 | fault `restart 6`; node6_boot2: `FROZEN with transition marker` → `[transition-abort] … authority abort` | stale gen-18 `.pending` recovered correctly |
| ~386 | node1: `ready-ack from=6` (gen 18), 300-move handoff, `commit-ack from=6` | **node 6 healthy, connected, committed gen 18 as a member** |
| ~394 | node1: `RELEASE generation 18` | join transition completes |
| ~395 | node1: `transition-start gen=19 …members_new=[1,2,3,4,5,7,8,9,10]` → `PREPARE 19` → `REMOVED peer=6 generation=19` | **stale (6,auto-remove) entry executed against a healthy, connected member** |
| ~395 | node6_boot2: `commit-ack gen=19` → `release-received` → **`graceful departure completed; stopping peer`** | node 6 exits; `!placement_ready_` → `loop_.stop()` |
| 414/474 | faults kill/restart 1; node1_boot2: `STALE->JOINER` → gen 21 | authority failover + rejoin work correctly |

**Causal chain (source):** `schedule_auto_removal_tick()`
(`src/peer/placement_runtime.cpp`) correctly enqueues only
unavailable-Dead members. But `progress_transition()`'s Idle branch popped the
entry and called `begin_transition(id, /*joining=*/false)` with **no
re-validation of the peer's current health at dequeue time**. A peer that
recovered while the entry waited behind a long join transition (populated
handoff) was therefore removed while alive and connected. The exit itself is
the *designed* graceful-departure path (`PLACEMENT_RELEASE` →
`!placement_ready_` → `loop_.stop()`) — correct for operator leaves, fatal when
triggered by a stale removal.

Secondary observation (not causal): node 1's failure detector flapped
`UNAVAILABLE↔HEALTHY` for peer 6 during its recovery window; the flip at
t≈395 s reflects the same recovery-window state, not a second failure. The
removal would have executed regardless of that flap.

**Fix (minimal):** membership queue entries now carry a kind
(`Join/Leave/AutoRemove`); at dequeue, an `AutoRemove` is re-validated — it
executes only if the peer is still a committed member **and** still marked
Dead; otherwise it is dropped and logged
`[membership] dropped stale AUTO_REMOVE peer=N reason=recovered`. Graceful
`Leave` semantics and all other queues are unchanged.

## 3. Recomputed metrics (from `runner_operations.csv`, 14,400 rows)

Computation: `~/investigation_extreme/analyze.py` (methods described inline).

- Overall: **14,400 attempts / 12,548 acked / 1,852 failed = 87.139%** — matches
  `summary.json`.
- Acked latency: mean 2.00 ms, p50 1.94, p95 2.89, p99 4.02 (gateway → W-quorum
  → pixel notification, local loopback).
- Per `via_node`: node 1: 87.71%, node 2: 97.08%, node 3: 97.01%, node 4:
  97.15%, node 5: 97.15%, **node 6: 57.43%**, node 7: 92.15%, node 8: 87.50%,
  node 9: 81.81%, node 10: 76.39%. (Node 6's number includes its permanent
  post-exit outage — the stale-removal defect's direct cost.)
- Error classification (actual strings): `connection_refused` 1,379 (down
  gateways), `transition_frozen` 270 (writes during reconfiguration — explicit
  rejection, correct), `quorum_timeout_or_unavailable` 203 (W=2 unmet during
  outages — correct). **Zero unclassified errors.**
- 30-s time buckets: success tracks the fault schedule (90–119 s: 83.5% after
  node 8's kill; 240–299 s: 81→80% during node 10/9 outages; 360–389 s: 73.1%
  during node 6's outage; 480–599 s: steady 90.0% — exactly node 6's missing
  1/10 round-robin share failing with `connection_refused`).
- **Reachable-gateway success (second metric): 92.69%** — 13,537 attempts
  through gateways alive at dispatch per the executed fault schedule; 863
  down-gateway attempts excluded from the denominator (reported separately).
  ±1 s around each fault event: 568 uncertain attempts, 451 acked.
- Adaptive replication realized (§ from `analyze_policy.py` over 20,992 policy
  rows, 32 distinct snapshots): policy reacts to churn — e.g. gen 12/14/16
  snapshots show 304→408 N4 regions during the 96–318 s fault cluster; final
  snapshot (gen 21 ep 24) returns to **N2×256** after recovery. Under-target
  fraction (`confirmed_live_copies < desired_n`) peaks at 52–62% during the
  240–390 s churn window and decays to ~28% at the end — honest measured
  under-replication, consistent with the confirmed-copy definition
  (connected assigned targets) and W=2 surviving on the remaining holders.
- Strategy B inputs: reliability scores only (peer_health CSVs) — no
  write-frequency inputs exist anywhere in the policy code path.

## 4. Runner methodology audit

- **`attempted`**: every PUT dispatched by the runner (round-robin gateway
  `(index % 10)+1`, regardless of liveness).
- **`acked`**: the runner received the *matching* WebSocket `pixel` event
  (x, y and color) from the gateway. That event is emitted by
  `announce_committed_pixel` only after the `W=2` quorum callback succeeds —
  so `acked` = **runner-observed W-quorum-confirmed write** (strong). Unique
  coordinate per attempt eliminates overwrite ambiguity.
  False-success risk: negligible (event only after quorum; exact color match).
  False-failure risk: the gateway committed but the WS connection died before
  the event → counted failed (conservative, under-counts success).
- **`failed`** = attempted − acked; errors are the classified strings above —
  none indicate fabricated data.
- **Verifier**: samples `min(1500, acked)` random acked writes and snapshots
  the first 3 *live* nodes; zero missing reported. Limitations: only 3 of 10
  nodes checked, failed writes excluded by design (they were never
  acknowledged — correct), `settle=8 s` may catch nodes mid-repair. Strongest
  laptop-feasible upgrade: `--verify-nodes 10` (all live) with the same
  sampling, plus a membership-agreement assertion across all live nodes (same
  generation + member set) and one full snapshot per node.
- The stale-removal defect is a **system** defect, not a runner artifact: the
  runner's kill/restart schedule is legal input, and the system's documented
  churn semantics require a recovered node to remain a member.

## 5. Minimal proposed changes

The targeted regression test (`tests/test_lab06_auto_remove_stale.py`) exposed
**three layered defects**; all are fixed in the working tree:

1. **Stale AUTO_REMOVE executed without dequeue re-validation** (the extreme
   run's root cause). Fix: queue entries carry a kind
   (`Join/Leave/AutoRemove`); at dequeue an `AutoRemove` executes only if the
   peer is still a committed member, still marked Dead, **and not currently
   connected** — otherwise it is dropped and logged
   `[membership] dropped stale AUTO_REMOVE peer=N reason=reconnected|recovered`.
   (`progress_transition` Idle branch, `schedule_join/schedule_leave`,
   `MembershipOp` in `peer_node.hpp`.)
2. **Join transitions waited for disconnected incumbents**: `waiting_` was
   pruned only for leaves; a join begun while another member was down stalled
   90 s in Preparing and then fail-closed at commit
   (`COMMIT UNCERTAIN`, frozen forever). Fix: the disconnected/Dead prune now
   runs for **every** transition (`begin_transition`).
3. **A removal's commit waited for the removed peer**: the commit-phase
   `waiting_` re-included a removed peer that reconnected mid-transition —
   holding a stale staged proposal with the *same epoch but a different
   fingerprint* (the aborted join and the removal both planned epoch 7) — so
   it could never ACK and the authority fail-closed (`COMMIT UNCERTAIN`).
   Fix: `removed_peers_` are excluded from the commit-phase `waiting_`
   unconditionally, and the `[transition-abort]` log no longer repeats every
   tick in the fail-closed Committing state. (An earlier "effective-epoch
   HELLO" attempt was evaluated and reverted: it would make idle members
   reset themselves when an authority mid-transition dialed them, and it
   masked genuinely-stale same-epoch staged proposals; the committed-only
   epoch with the fixes above converges correctly.)
4. **An aborted JOIN was never retried**: the referral/HELLO paths enqueue a
   candidate only once per connection; when a join transition aborted (e.g.
   a participant disconnect), the candidate stayed connected but was never
   re-enqueued — the cluster silently dropped the admission and the next
   queued op ran instead. Fix: `abort_transition` re-enqueues the
   still-connected join candidate (`transition_candidate_`).

Runner hardening (recommended, methodology-preserving for A/B/C):
`--verify-nodes` default to all live nodes; assert final membership
agreement (same gen + members on every live node's state file) and fail
the run if any planned-restart node is not running at the end.

Do **not** change: churn schedule, workload selection, quorum constants,
detector thresholds, or the graceful-departure semantics themselves.

## 6. Reproduction

```bash
cd ~/projects/p2place
cmake --build build
python3 tests/test_lab06_auto_remove_stale.py                  # targeted rejoin/removal test
python3 experiments/run_experiment.py --strategy B --seed 43 --seconds 600 \
    --nodes 10 --churn-profile extreme --rate 24 --workers 20 \
    --out experiments/results/B_10_seed43_extreme_rerun
```

## 7. Before/after acceptance

- Before: gen-19 removal of a healthy node 6 → permanent exit → 9/10 members
  for the last ~200 s; 613 failed writes on node 6's share.
- After: `dropped stale AUTO_REMOVE peer=6`; node 6 remains a running member;
  all planned restarts end as active members; surviving nodes converge on one
  generation; reachable-gateway success ≥ previous run; existing Lab 04–06
  suites (`bash verify_all.sh`) stay green.
- Regression test result: **PASS** (see §8).

## 8. Actual test output

```text
$ python3 tests/test_lab06_auto_remove_stale.py
PASS setup: 256 region pixels acknowledged at W=2
PASS setup: node 6 restarted at +5.3s (removal still queued behind the join)
PASS: stale AUTO_REMOVE dropped; node 6 remains a running member; all seven nodes share one generation
PASS: node 6 serves acknowledged pixels after recovery
```

Full regression (`bash verify_all.sh`): **ALL PASS** — ctest 19/19;
join regression A+B+C; churn stress; stale first-restart rejoin A/B/C/D;
stale AUTO_REMOVE revalidation (all four assertions); Strategy B live
(stable/churn/recovery + A regression); join-via-seed; ring election;
membership; lab05 quorum; lab06 browser sync.

A full rerun of the 10-node extreme experiment
(`python3 experiments/run_experiment.py --strategy B --seed 43 --seconds 600
--nodes 10 --churn-profile extreme --rate 24 --workers 20 --out
experiments/results/B_10_seed43_extreme_rerun`) is recommended as the final
before/after comparison; it takes ~11 minutes on this laptop and was not part
of the defect-fix loop.
