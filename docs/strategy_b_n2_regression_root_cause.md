# Strategy B (N=2–4) Regression — Root Cause

**Status:** root cause **confirmed** (three coupled defects, each evidenced by
archived logs); fixes implemented; deterministic regressions pass; controlled
N=2 replay validates convergence.

**Evidence archive:** `experiments/investigations/n2_regression/B_10_seed43_stress50_300s_N=2`
(read-only copy of `B_10_seed43_stress50_300s_N=2.zip`, sha256
`5dac045e…`). Git HEAD `d4968d4` (working tree uncommitted).

---

## 1. Confirmed symptom timeline

| Time | Event | Evidence |
|---|---|---|
| 0–600 s | Workload; 101 regions N2→N3, 101 N3→N4 (churn-raise wave) | `region_replication_events.csv` (202 n_change rows ≤600 s) |
| 570 / 582 s | Last kill / restart of nodes 8, 9 | `fault_plan.json` (120 events) |
| ~600 s | **Immediate verification: 136 mismatches** (0 read errors) | `summary.json` |
| ~678 s | **101 regions N4→N3** (outages exit the 120 s window: 570+120≈690) | `region_replication_events.csv` (epoch 6) |
| ~708 s | **101 regions N3→N2** (30 s cooldown later) | `region_replication_events.csv` (epoch 7) |
| ~900 s | **After-settle verification: 663 mismatches** (+527 during the write-free window) | `summary.json` |

The +527 growth is temporally pinned to the two shrink waves: mismatch growth
is caused by the recovery-window policy shrink, not by N=2 per se and not by
new writes (the window is write-free).

## 2. Defect D1 — rejoin policy starvation (confirmed)

`PeerNode::evaluate_and_broadcast_policy`
(`src/peer/placement_runtime.cpp`) returned early when nothing changed:
`if(!ev.changed && !placement_changed)return;`. A member that restarts and
reconnects at the **same** placement generation therefore never receives the
current snapshot: its `PolicyTable` stays uninitialized, `plan()` falls back to
N=3, the activation-pull loop (which runs only on `POLICY_SNAPSHOT` apply)
never queues backfill, and the node serves **empty regions for every read for
the rest of the run**.

Evidence: `node8_boot9.log` (the 582 s restart) contains **zero** `[policy-*]`
lines and **zero** `[backfill]` lines across 318 s of uptime, yet it answers
snapshot GETs (`node1_boot1.log`: `request=1073742214 region=133 …
targets=8,9` → acks `from=9`, `from=8`). The authority's own log shows the
broadcasts it did send (`epoch=6`, `epoch=7` — both after 582 s); node 8
received neither.

## 3. Defect D2 — scale-down strands acknowledged copies (confirmed)

Pixel `(86,140)`, region 133, `#9857B6` (runner attempt 5157, t=214.9 s):

- Write: `[quorum] node=5 op=PUT request=1073742734 region=133 index=198 N=4
  targets=1,8,9,10`; nodes 8, 9 down (`failed-peer=8/9`); **W=2 ACKed by nodes
  1 and 10** (`[replica-put] … applied=1` on 1 and 10 only).
- Region 133 policy: N4 until t=678, then N4→N3, then N3→N2 (t=708).
- **Final N2 pair for region 133 = {8, 9}** (placement state: first two
  preferences of the region owner) — the two nodes that never held the pixel.
- At t≈900, reader 1's snapshot GET for region 133 targets {8,9} only and
  merges their two empty replies → the pixel is **absent on every gateway**
  although nodes 1 and 10 still hold it in RAM (both ran continuously,
  `node1_boot1.log` / `node10_boot1.log`; node 1 even pushed it via
  anti-entropy 10 times during the N4 phase, but the receivers restarted).

Reads and backfill consult only the **current** N targets, so copies on
replicas dropped by a shrink are invisible; anti-entropy pushes only from
current targets, so the holders never re-share after the shrink.

## 4. Defect D3 — backfill unsatisfiable at N=2 (confirmed by construction)

With `exclude_self`, an N=2 backfill has exactly **one** source but still
requires `R=2` responses → it can never complete (deadline → `give_up`).
This never fired in the archive only because D1 prevented backfill entirely;
it is covered by a new deterministic test.

## 4b. Defect D5 — partial local copies skip the re-pull (found in validation)

The first fixed-binary stress replay (`B_10_seed43_stress50_N2_regression_v2`)
reduced the symptom (58 immediate / 315 after-settle vs 136/663) but did not
eliminate it. Tracing pixel `(110,136)` (region 134) showed why: node 8's
final boot (boot34) applied the shrink epochs correctly (heartbeats working)
but its region-134 copy was **non-empty** — it held pixels written during its
up-windows — while `(110,136)` (holders 1, 10) was missing. The reconsider
loop skipped every owned region with `version()>0`, so partial copies were
never re-pulled, and the stranded pixel only arrived via the ~200 s
anti-entropy sweep. Fix: reconsider **all** owned regions on policy change;
`backfill_step` merges into non-empty stores (LWW-idempotent) instead of
skipping them. Deterministic regression: `test_backfill_completeness.cpp`
Test 8 (partial copy re-pulls and merges the stranded pixel while keeping
the newer local pixel).

## 5. The fix (minimal, four parts)

1. **Policy heartbeat** (`placement_runtime.cpp`): the authority re-broadcasts
   the current snapshot on every periodic evaluation (10 s) even when nothing
   changed. Receivers treat an unchanged epoch as idempotent and skip the
   pull loop (`(heartbeat)` log), so a reconnecting member re-syncs within one
   interval.
2. **Superset backfill discovery** (`replication_manager.cpp`,
   `replication_strategy.hpp`, `churn_replication_strategy.hpp`): backfill
   pulls from the **max-N preference superset** (`max_targets()`, 4 for B, 3
   for A) instead of the current N set, with `required = min(R, sources)`.
   Acknowledged writes are always held within the max-N preference set, so a
   shrink-strand copy is discoverable again. The GET responder authorization
   is widened to the same superset (reads only; PUT stays strict, so W=2
   remains on designated replicas only).
3. **Reconsider on policy change** (`placement_runtime.cpp`): after any real
   policy/placement change, every member re-queues **all** its owned regions
   (data-less, empty, or partial) via `reconsider_backfill()`, which clears
   the resolved/attempt state. After a shrink, the remaining pair re-pulls
   immediately — the superset pull finds both fully stranded and partially
   stranded copies.
4. **Background pulls excluded from quiescence gates**
   (`replication_manager.hpp`, `placement_runtime.cpp`): backfill GETs are
   marked `background`; the membership `[join-wait]` / `[transition-wait]` /
   READY gates use `foreground_pending()` instead of `pending_count()`. This
   fix was added after the first N2 validation replay reproduced a **new**
   join stall: the authority's boot-time backfill (enabled by fix 3) issues
   one GET per 500 ms tick, so `pending_count()!=0` was true almost
   continuously and node 3's admission timed out after 120 s
   (`B_deep_isolated_N2_admission_fail/node1_boot1.log`:
   `[join-wait] authority=1 reason=pending_quorums=1`).

Anti-entropy push targets and the snapshot read path are **unchanged**;
R=2/W=2 semantics and the wire format are unchanged.

## 6. Classification of the archived mismatches

- 136 immediate mismatches: first-R read race + backfill drain window — the
  restarted pair (8, 9) rejoin ~18 s before verification and their backfill
  queue (1 region per 500 ms tick) cannot reach high-key regions in time;
  reads racing to the two empty responders merge nothing. **Confirmed
  mechanism** (same as the earlier false-complete race, minus the false
  success). Not data loss.
- 663 after-settle mismatches: deterministic absence after the N2 shrink
  (D1+D2): the pair {8,9} never pulled the stranded copies. **Confirmed**.
  Not data loss — holders 1/10 retained the pixels.
- **No confirmed data loss** for the traced pixels; global loss remains
  unproven (H6: `null`, not false).

## 7. Regression evidence

- `tests/test_backfill_completeness.cpp` extended: N=2 healthy W=2/R=2; N=2
  one-target-down fails without a false ACK; **N4 write with the future N2
  pair down → shrink → superset backfill recovers the stranded copy → N=2
  read correct → read-repair propagates to the other pair member**.
- `ctest`: 20/20.
- `tests/test_lab06_strategy_b.py` (live, N2 baseline): ALL PASS.
- Controlled N=2 replay (`tests/test_replication_convergence_deep.py
  isolated --min-replicas 2 --max-replicas 4`): policy cycle exercised
  (epoch 2: 76 regions N4; epoch 4: shrink back to n2=256) with heartbeats
  re-broadcasting each epoch; node 8's post-restart backfills show
  `classification=complete` (nonzero versions) for the targeted regions via
  superset GETs (e.g. `region=13 … fanout=superset targets=1,9,10`) and
  `empty_valid attempts=1` for genuinely empty regions. **All 4 recovery
  rounds × 1,080 comparisons × 10 gateways = 0 mismatches, 0 read errors,
  including round 0** (`B_deep_isolated_N2/recovery/probe_summary.json`).
- Full 900 s stress replay (fresh out dirs, same seed/fault plan):

| Run (B, N=2–4, seed 43, stress50-spread) | Immediate | After 300 s |
|---|---:|---:|
| Pre-fix archive `…_N=2` | 136 | **663** (+527) |
| v2 (heartbeat + superset + reconsider + gate fix) | 58 | 315 |
| **v3 (all five fixes, incl. partial-copy re-pull)** | **54** | **0** |
| Reference: B N=3–4 (pre-fix era) | 189 | 0 |

  v3: `B_10_seed43_stress50_N2_regression_v3` — 14,362/14,400 ACK (38
  training-phase failures), immediate 54 (reader 3 only; readers 1/2 = 0,
  drain-window artifact), **after 300 s: 0/0/0 across readers 1/2/3**,
  `net_reduction=54`, timeline conflicts=0, runner exit 0.
- Machine-readable diagnostics:
  `experiments/diagnostics/strategy_b_n2_regression/`
  (`sampled_pixels.csv`, `verification_all_gateways.csv`,
  `local_replica_inventory.csv`, `quorum_response_trace.csv`,
  `policy_transition_trace.csv`, `backfill_repair_trace.csv`,
  `convergence_checkpoints.csv`, `case_summary.json`, `node_logs/`;
  builder: `experiments/build_n2_diagnostics.py`).

## 8. Verdict

The N=2 policy **is safe to keep** with the five fixes in place: the archived
deterioration was caused by the rejoin policy starvation (D1), the shrink
stranding (D2), the N=2 backfill quorum impossibility (D3), the background-
pull gate stall (D4), and the partial-copy re-pull gap (D5) — not by the N=2
quorum itself, which is strict (W=2/R=2 on two designated replicas, no false
ACKs) and now converges to **0 gateway mismatches after 300 s** on the exact
stress profile that previously degraded to 663. Immediate-checkpoint
mismatches (54, reader 3 only in the final run) remain possible within the
backfill drain window (~2 s per region) after a restart; they are read-time
artifacts, not loss, and converge. Remaining limits: drain rate (1 region per
500 ms tick), no direct per-replica RAM inspection in production builds, and
the pre-existing gap for data on replicas outside the max-N preference set —
impossible for ACKed writes (they always land inside the current plan.N ≤
max-N set), so no confirmed data loss exists in any traced case
(`confirmed_data_loss: null` = unproven, not false).
