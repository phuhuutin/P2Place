# Strategy C Blind Fault Test — Fix & Verification Report

**Handoff:** `P2Place_Strategy_C_Fault_Backfill_Investigation_Handoff.md`
**Evidence:** archived failure `strategy_c_blind/run_20261117T092443Z/fault/C/` (region 192)
**Status:** root cause confirmed in two layers; fixes implemented; blind fault case passes with a real fault injection after verified readiness.

---

## 1. Root cause, separated by layer

### 1.1 C++ scheduling (policy-activation backfill priority inversion)

Confirmed from source and the archived run:

- Before the fix, a real policy change re-queued **every owned region** via
  `reconsider_backfill()` (the earlier N2 shrink-reconciliation design), and
  `backfill_step()` always took `backfill_queue_.begin()` — the **smallest
  numeric region id** — one region per 500 ms tick.
- The archived run shows the consequence directly: the two newly selected N4
  targets node 1 and node 2 drained only up to region **112 / 126** in their
  backfill queues (`[backfill] node=… region=…`, 169/166 lines); **region 192
  was never reached**, producing zero backfill events for it.
- Region 192's N4 epoch lasted from t_ns 37,064,907,508,520 to
  37,121,087,077,091 — **56.2 s**, just under the 60 s write window — because
  the test's heater stopped, and the queue could not reach region 192 in time.
  N legitimately fell back to N3 (epoch 4, t 37,121,087,077,091) and N2
  (epoch 5, t 37,125,100,607,229) while the harness was still waiting for
  backfill. The timeout correctly reported `authority target N=2` as the end
  state.
- This is an **activation-latency/priority defect, not data corruption**: the
  pulls were valid and idempotent, and no acknowledged write was lost.

### 1.2 Test harness

- `trial_fault()` heated the region with a 90-write burst and then
  **stop-and-waited** for backfill. With no further writes, the 60 s window
  decays by design; the readiness wait could legitimately never succeed.
- `completed_backfill()` accepted **any** historical `result=ok` row for the
  region, with no generation/epoch binding — a stale boot-time initial-sync
  row from an earlier placement could have been mistaken for current-epoch
  readiness (proven by the negative control, §4.3 — the first version of the
  fixed harness failed its own negative test because of exactly this).
- The readiness evidence also ignored the authority: node 1 applies snapshots
  without a `POLICY_SNAPSHOT` message round-trip, so it never emits
  `[policy-apply]`. Any extra target equal to node 1 could never satisfy a
  `[policy-apply]`-based check (this failed the first fixed run; §4.2).

## 2. Fixes

### 2.1 C++ (`ReplicationManager`, `PeerNode`, policy helper)

1. **Urgent activation queue** (`replication_manager.{hpp,cpp}`): a
   deduplicated FIFO (`priority_queue_` + `priority_queued_`) for regions a
   member was **newly selected** to hold by an N increase.
   - `backfill_step()` prefers urgent work; after `kUrgentBurst = 4`
     consecutive urgent picks (when normal work is waiting) exactly one
     normal region runs — repeated urgent failure cannot starve normal work;
     urgent items are also bounded by the existing 4-attempt budget.
   - Promotion: a normal request for a region already urgent is a no-op; a
     normal→urgent request moves it between queues (one entry total).
   - Retries return to the **same queue kind**, urgent retries go to the
     **back** of the FIFO (other urgent regions are not blocked).
   - `reset_repair_state()` clears both queues; the `state_epoch_` guard still
     drops stale in-flight callbacks (no fabricated readiness).
2. **Targeted activation** (`placement_runtime.cpp` +
   `policy_activation_action()` in `replication_policy.hpp`): on a real
   change, each member acts only on regions whose target set changed:
   - newly added suffix position (`new_n > old_n && position >= old_n`) →
     **urgent**;
   - remaining target of a changed set → **normal reconciliation** (the N2
     shrink-strand fix — former holders' acknowledged copies are re-pulled
     from the max-N superset);
   - unchanged N → nothing (no heartbeat floods);
   - initial sync / placement change → normal, data-less owned regions only
     (the original activation pull).
3. **Readiness metrics** (`metrics.{hpp,cpp}` +
   `set_repair_progress`): `replica_repair_events.csv` with
   `run_id,monotonic_ns,node_id,region,placement_gen,policy_epoch,event,
   classification,attempts`, events `urgent_enqueued` / `backfill_started` /
   `backfill_completed` / `backfill_retry` / `backfill_failed` /
   `enqueued`. `N4_activation_latency = max(new-target
   backfill_completed_ns) − N4 policy_apply_ns` is derivable by joining this
   file with `region_replication_events.csv`
   (`experiments/analyze_blind_fault.py` prints it).

Unchanged: R=2/W=2, target range 2–4, reliability/write thresholds,
`get(complete_all, exclude_self, fanout=max_targets, background)` LWW pull
semantics, anti-entropy cadence, wire format.

### 2.2 Blind harness (`tests/test_strategy_c_blind.py`)

- **Background heater**: a thread writes valid, distinct client PUTs to the
  selected region at `--heater-rate 3.0/s` through the living gateway, from
  warm-up through the fault probes (never stop-and-wait).
- **Strict readiness**: N=4 sustained with a 2 s dwell; each added target
  must have applied the committed generation at (or past) **the epoch it
  joined the target set**; must have a genuine completed backfill
  (`complete`, bound to the current generation and that epoch — never
  `complete_partial`/`empty_partial_accepted`/stale rows); and a client read
  must confirm known heater pixel values.
- **POLICY_DECAY_BEFORE_READY** fails fast if N falls below 4 after N4 was
  observed while the heater is active.
- **Authority evidence**: when a target has no `[policy-apply]` line (the
  authority), its `[policy-broadcast] gen/epoch` line is used — the same
  accepted snapshot.
- **Matched B control**: C runs first; B replays the **same heater
  duration**, same region, pair, gateway and probe order; deviations are
  recorded in the results JSON.
- **Post-fault survival read-back**: after the probes the killed owners are
  restarted (append-only logs, same ports/state) and a client read must show
  every acknowledged probe pixel with its written color. (A read before
  rejoin is impossible: other cold regions owned by the same pair legitimately
  fail their R=2 read and abort the whole-canvas snapshot — reported, never
  faked.)
- New cases: `idle` (legitimate policy decay after writes stop, separate from
  backfill timeout) and `negative` (the readiness guard must refuse to
  certify protection without completed backfill — the guard self-test).

## 3. Archived Region 192 timeline (the failure this report fixes)

`region_replication_events.csv` (authority, monotonic ns):

```text
37060893750580  region=192  2->3  gen=6 epoch=2
37064907508520  region=192  3->4  gen=6 epoch=3
37121087077091  region=192  4->3  gen=6 epoch=4
37125100607229  region=192  3->2  gen=6 epoch=5
```

N4 lifetime: 56.18 s. The two added targets (nodes 1, 2) produced **zero**
backfill events for region 192 (`event_file=True region_events=0
backfill_logs=0`); their queues reached regions 112/126. The harness stopped
writing after the 90-write burst, so the window decayed while it waited.

## 4. Verification evidence (all executed)

### 4.1 Unit + regression

| Check | Result |
|---|---|
| `ctest` (23 suites; new `backfill-priority` with 6 cases, extended `backfill-completeness` shrink cases) | **23/23 PASS** |
| `tests/test_strategy_c.py` (aggregation, B contrast, two-owner survival) | **ALL PASS** — B 0/6 vs C 6/6 |

`tests/test_backfill_priority.cpp` covers: urgent jumps a normal backlog
`[1,2,3,192]`; dedup + promotion (one entry); bounded urgent burst gives
normal work a slot; the activation-action table (only new N-increase suffix
positions urgent; shrink reconciles; unchanged = none; initial sync normal);
started/completed events with classification; stale-epoch drop for urgent
pulls. Placement-change behavior remains integration-covered (every blind
boot performs six placement transitions; no regressions in any suite).

### 4.2 Blind fault trial — seed 38731, region 192, owners {5,6} (real kill)

Same seed and region selection as the archived failure (blind discovery from
the committed placement; nothing hardcoded).

| Metric | Value |
|---|---|
| Heater | 3.0/s, 68 ACKed / 0 failed, stopped after the fault phase (22.5 s heat) |
| C target N at kill instant | **4** (pre_fault_n=4) |
| Node 1 (joined at N3 epoch 2) urgent pipeline | enqueued at epoch 2 → started ~120 µs later → completed **~2 ms**, `complete` |
| Node 2 (joined at N4 epoch 3) urgent pipeline | enqueued +2 ms after the N4 apply → started +117 ms → completed **+118 ms**, `complete` |
| N4 activation latency (max new-target completion − N4 apply) | **118 ms** |
| Fault | killed both original owners {5,6} simultaneously; placement gen did not change during probes |
| **B (N2)** | **0/4 post-crash ACKs** |
| **C (N4)** | **4/4 post-crash ACKs** |
| Acknowledged-pixel survival | all 4 ACKed probe pixels read back with their written colors after rejoining the owners: `0:202=#3CC9E4, 1:202=#3CE8D3, 2:202=#3D07C2, 3:202=#3D26B1` |

Run IDs: fault-only validation `run_20261117T095314Z`; archived failure
`run_20261117T092443Z`.

### 4.3 Blind cases (seed 38731)

| Case | Result |
|---|---|
| cold | PASS — cooled region stayed N2 (`region 140`, n distribution 256/0/0) |
| hot | PASS — hot region 12 reached N4 with the heater |
| fault | **PASS — B 0/4, C 4/4, region 192, killed {5,6}** |
| idle | PASS — region 125 decayed N4→N2 in **58.1 s** after writes stopped (write-window expiry, cooldown respected; separate from backfill) |
| negative | PASS — the readiness guard refused to certify without completed backfill evidence |

### 4.4 First fixed runs (recorded failures, for honesty)

- The very first negative-control smoke run (`run_20261117T094431Z`) **failed**
  ("readiness was certified without completed backfill evidence") because
  `completed_backfill()` accepted a stale boot-time row (gen 2/epoch 1) for
  the region — fixed by binding evidence to the current generation and the
  target's join epoch. The negative control caught its own harness's
  weakness, as intended.
- The first full fixed run (`run_20261117T094629Z`) failed the fault case
  with `node 1: not at gen/epoch (last_apply=None, n4_epoch=3)` — the
  authority-completeness gap (§1.2), fixed by the broadcast-evidence
  fallback. cold/hot/idle/negative passed in that run.

## 5. Second blind seed (90210) and a read-visibility finding

Run `run_20261117T095836Z` (all five cases): cold PASS (region 179), hot PASS
(region 201), idle PASS (region 141, 58.1 s decay), negative PASS (region
200), **fault FAIL — and the failure is informative**:

- The fault trial reached verified N4 and killed the owners, but the
  post-rejoin **single-shot** read-back missed the four acknowledged probe
  pixels: the canvas read at N=4/R=2 merged the **two restarted (empty)**
  replicas, whose responses won the first-R race; the acknowledged values
  lived on the two surviving extras (the probe ACKs were W=2 from those
  two). This is the read-side counterpart of the backfill first-R race —
  an ordinary-snapshot read-time artifact under R+W<=N, **not data loss**
  (no holder ever lost the value; the rejection message is retained).
- The harness now **polls the read to a bounded convergence window (30 s)**
  and records `readback_s` instead of declaring loss from one instant —
  matching the documented eventual-consistency contract of the read path
  (deliberately left first-R in the earlier fix).

Seed-90210 fault rerun (`run_20261117T100248Z`): **PASS**. Blind region
selection differs from seed 38731 as expected — region **126**, owners
**{2,3}** discovered from the committed placement:

| Metric | Value |
|---|---|
| Heater | 3.0/s, 68 ACKed / 0 failed, 22.46 s heat (B matched at 22.47 s) |
| C target N at kill | 4 |
| Node 4 (joined at N3 epoch 2) | urgent completed **292 ms**, `complete`; N4 reconcile also completed (+9.3 s) |
| Node 5 (joined at N4 epoch 3) | enqueued +2 ms after the N4 apply → completed **+172 ms**, `complete` |
| Fault | killed owners {2,3}; placement gen stable during probes |
| **B (N2)** | **0/4 ACKs** |
| **C (N4)** | **4/4 ACKs** |
| Survival read-back | all 4 pixels readable with written values; **converged 8.6 s after rejoin** (first shot had hit the empty-replica race — the failure above) |

Full five-case suite at seed 90210 re-recorded after the polled read-back
fix — `run_20261117T100500Z`: **cold, hot, fault, idle, negative all PASS**.
Fault re-verified in that run: region 126, owners {2,3} killed, **B 0/4, C
4/4**, survival read-back converged **2.2 s** after rejoin; urgent
completions 293 ms (N3-join) and 168 ms (N4-join, +170 ms after the N4
apply).

The finding does not change the C++ fix package; it tightens what the
harness may claim from one instant, and documents a pre-existing read-path
characteristic of R+W<=N at N=4 (a rejoined replica can transiently answer
reads with an empty region until its backfill/anti-entropy completes).

## 6. Remaining limitations

- Readiness evidence is gateway-metrics based (backfill classifications,
  apply lines) plus an end-to-end read-back; it is not a direct per-replica
  RAM dump. `confirmed_live_copies` is never used as readiness proof.
- **Post-rejoin read visibility:** at N=4/R=2 an ordinary canvas read can
  transiently miss acknowledged values because R=2 may select a rejoined
  (still empty) replica; the value is on surviving holders and becomes
  readable once the rejoined pair backfills (measured: 8.6 s in seed 90210).
  This is the read path's documented eventual-consistency contract (R+W<=N);
  the harness polls to a 30 s bound and records `readback_s`.
- The per-target requirement is "synced at the epoch it joined the target
  set" (not the final N4 epoch): a member added at N3 that completed its
  pull then receives every later write as a designated target, so requiring
  a redundant N4-epoch pull would only measure queue latency, not data
  readiness. Documented deviation from a literal reading of "same epoch".
- The blind harness's auto-removal (>15 s) can reconfigure membership if
  probes run long; the harness asserts placement stability and reports
  INCONCLUSIVE instead of mislabeling.
- B-vs-C comparisons remain single-machine/WSL with shared CPU; latency
  numbers are dominated by the local machine. Policy/write thresholds and
  R/W are unchanged from A/B/C's fixed contract.
- Urgent bursts are bounded (4) — a pathological flood of *new* targets
  across many regions would still take ~0.5 s per urgent region; correctness
  holds, activation latency grows linearly with the number of simultaneously
  new targets.
