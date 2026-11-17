# Strategy B Convergence Investigation — Backfill False-Complete

Status: root cause **confirmed** with archived log evidence and a deterministic
reproduction; patch validated; residual limits documented.

---

## 1. Version identity

| Item | Value |
|---|---|
| Git HEAD | `d4968d4681c54cd6c7a7bbd51ac02a0a42750cab` ("Complete Strategy A: fixed replication (N=3) and replica recovery") |
| Working tree | Dirty (Strategy B policy + timeline instrumentation, uncommitted) |
| Peer binary (tested) | `build/bin/p2place-peer` sha256 `57d78819614b94af507cdf39b92e520b52b8a67e03c2c129f465eadcb0a2f63a` (pre-patch) |
| Python runner | `experiments/run_experiment.py` sha256 `f86ff4aa6c39007b7abd015b9fe5889f2b1d185f4291652c7a2ae0b5a19f6d03` |
| Isolated deep test | `tests/test_replication_convergence_deep.py` (analyze / probe / isolated) |
| Build | `cmake --build build`; C++20; tests via `ctest --test-dir build` |
| Config under test | Strategy B, R=2, W=2. **2026-11-16: the N2 policy was re-applied** (three-tier mapping 0.95/0.80 → N2/N3/N4, initial N=2, runner default `--min-replicas 2`). A `--min-replicas 3 --max-replicas 4` config still collapses to the earlier two-tier N3/N4 behavior (mid tier = min_n), preserving A/B comparison baselines. The backfill false-complete fix below applies to all tiers. |

Archived result directories (`experiments/results/`) are preserved read-only:
`B_10_seed43_stress50`, `A_10_seed43_stress50`, `B_deep_isolated_01` (pre-patch
evidence used throughout).

## 2. Confirmed observations

### 2.1 Symptom (from `B_deep_isolated_01` archive)

- 108 acknowledged pixels, 0 write failures. Baseline probe: 0 mismatches.
- Recovery probe (4 rounds, 10 gateways, 1,080 comparisons/round): round 0 = **90
  `absent`**, rounds 1–3 = **0**. All 90 confined to regions **23 and 24**
  (readers 4,7,8,9 ×18; 1,3 ×9).
- Stress50 B (seed 43): immediate 230 → after-60s **168** pixel×reader
  mismatches; A: 109 → 89. Backfill events 690, `anti-entropy` log lines 3,431,
  `[read-repair]` 357 (keyword counts, not repairs).

### 2.2 `version=0` semantics — CONFIRMED (not a bug by itself)

`Region::version()` (`src/core/region.cpp:30`) is `max(pixel.lamport)` over all
256 pixels; `0` therefore means **no pixel with a nonzero Lamport timestamp**,
i.e. the region carries no data. A `GET_OK` reply for a region the node does not
hold sends a default-constructed empty `Region{}`
(`src/replication/replication_manager.cpp:236`). So `[backfill] … version=0`
means **the quorum-merged result contained zero pixels**. The protocol has no
way to distinguish "region is genuinely empty cluster-wide" (`empty_valid`) from
"I hold nothing / my sources held nothing" (`missing_source`), and a merged
empty region is inserted into the store
(`RegionStore::merge_region`, `src/core/region_store.cpp:65`) so the node
afterwards *looks* populated.

### 2.3 False-complete mechanism — CONFIRMED with archived logs

Read quorum completes on the **first R responses** and merges only those
(`src/replication/replication_manager.cpp:257–262`); later responses are
discarded (`[quorum-ignore] reason=late-or-unknown`, line 242–246). The
backfilling node counts its **own synchronous empty reply** toward R
(dispatch-local path, line 238–239). `backfill_step()` then logs `success=true`
and pops the region permanently (lines 97–115); its guard
`if(store_.get_region(key))return true;` (line 102) blocks any future re-pull
because the empty merge created the entry. The only remaining repair is the
anti-entropy **push** (one region per node per 2 s,
`src/peer/peer_node.cpp:862–866`), which is why isolated round 0→1 converges but
Stress50 still shows 168 mismatches after 60 s (a full per-node sweep of ~77–102
owned regions takes 2–3.5 minutes).

Archived evidence, `node8_boot7.log` (identical pattern in `node9_boot2.log`):

```text
[quorum] node=8 op=GET request=1073741828 region=23 N=3 assigned=3 R=2 targets=8,9,10,
[quorum-ack] node=8 op=GET request=1073741828 from=8 count=1/2      # self, empty
[quorum-ack] node=8 op=GET request=1073741828 from=9 count=2/2      # other restarted node, empty
[quorum-done] node=8 op=GET request=1073741828 ok=1 ack=2/2 error=
[quorum-ignore] node=8 reason=late-or-unknown request=1073741828 sender=10   # populated reply DISCARDED
[backfill] node=8 region=23 success=true version=0
```

Node 10 held the data (`[replica-repair] node=8 from=10 region=23
version=1794794664934338`) and repaired node 8 later via anti-entropy push. The
first recovery-round gateway snapshots hit the same race (e.g.
`node8_boot7.log` request 1073741918: acks from 8+9 only) — that is the round-0
absence on readers 4/7/8/9; readers 1/3 owned one of the two regions (9
absences), and node 10's own snapshot merged its local copy (0 absences).

### 2.4 Inferred (high confidence, not directly logged)

- The same mechanism explains a large share of Stress50's 168 persistent
  mismatches: after a double-outage the two restarted gateways/replicas answer
  reads with empty regions, R=2 completes on the two empty answers, and a
  populated third/fourth reply is discarded; anti-entropy push cannot sweep all
  256 regions within the 60 s settle window.
- `[backfill] success=true version=0` lines for genuinely empty regions (e.g.
  `node8_boot2.log` regions 6/7/8/13/20) are `empty_valid` — same signature, no
  defect.

### 2.5 Not tested / open

- Whether *all* Stress50 mismatches share this cause (the archived run cannot
  be re-probed; a fresh stress rerun is the T6 validation).
- Direct per-replica RAM inspection (no debug command exists yet).
- Whether any acknowledged write ever lost its last physical copy (nothing in
  the evidence supports loss; nothing proves absence of loss either).

## 3. Data-flow timeline (source-referenced)

### 3.1 Isolated coordinates — region 23 / region 24 (pre-patch archive `B_deep_isolated_01`)

1. Acknowledged writes (baseline + pre_outage) reach W=2; `[replica-put] node=8
   from=2 … region=23 … applied=1` etc. (node8_boot1.log).
2. Nodes 8 and 9 are SIGKILLed and restart empty (`reset_to_joiner` →
   `region_store_.clear()`, `peer_node.cpp:448`).
3. Policy snapshot re-adoption queues backfill for owned regions
   (`placement_runtime.cpp:915–924`); `backfill_step()` drains one region per
   500 ms tick (`peer_node.cpp:880–882`).
4. Region 23 backfill on node 8 (`node8_boot7.log` request 1073741828):
   `targets=8,9,10`, acks `from=8` (self, empty) then `from=9` (other
   restarted node, empty) → `finish_get` at R=2 → `[quorum-ignore] sender=10`
   (populated reply discarded) → `[backfill] … success=true version=0`.
   Same on node 9 (request 1073741835, acks 9+8, sender 10 discarded).
5. Recovery probe round 0: gateway snapshot GETs for regions 23/24 (e.g.
   node 8 request 1073741918, acks 8+9) merge two empty replies → all sampled
   pixels `absent` on readers 4/7/8/9 (18 each) and 1/3 (9 each); node 10
   merges its own populated copy → 0 absences.
6. Between rounds: node 10's anti-entropy push (2 s cadence)
   `[anti-entropy] node=10 region=23 to=8 …` → `[replica-repair] node=8
   from=10 region=23 version=1794794664934338`; rounds 1–3 all clean.

### 3.2 Stress50 coordinate — pixel (23,134), region 129 (archive `B_10_seed43_stress50`)

1. `runner_operations.csv` attempt 6897, t=287.4 s, phase `double_failure`,
   targeted write `#9E2A8A` via gateway 5, ACKed.
2. `[quorum] node=5 op=PUT request=1073743046 region=129 index=103 N=4 W=2
   targets=7,8,9,10`; `failed-peer=8`, `failed-peer=9` (both down, killed at
   t=285); `[quorum-ack] from=7 … from=10` → W=2 satisfied by 7 and 10;
   `[replica-put] … applied=1` on both 7 and 10. **Both holders survived the
   rest of the run.**
3. Nodes 8/9 restart every ~12 s through the churn phase and drain only
   ~19–24 backfills per up-window (1 region per 500 ms tick); region 129 is
   never drained (0 backfill lines for region 129 across all boots), so they
   keep answering `GET_OK` with an empty `Region{}` for region 129.
4. After-settle verification: `[quorum] node=2 op=GET request=1073742164
   region=129 N=4 R=2 targets=7,8,9,10` completes on the two empty responders
   (8, 9) and logs `[quorum-ignore] … sender=7` — node 7's populated reply is
   discarded, so reader 2 reports the pixel absent although nodes 7 and 10
   still hold it. **Read-time incomplete merge, not data loss.**

This is the same confirmed defect in two variants: (a) the backfill-side false
complete (`version=0` merged from empty responders, never retried — isolated
case), and (b) the snapshot-read-side first-R race over nodes whose backfill
queue never drained (Stress50 case).

### 3.3 PUT / snapshot path notes (unchanged by the patch)

PUT path: WS `put` → `PeerNode` handler (`src/peer/peer_node.cpp:1047`) →
`ReplicationManager::put` (`replication_manager.cpp:45`) → `targets()` = ring
successors under `strategy_.plan(key).N` (line 25) → `dispatch` to each target
(local runs the same authenticated response path, line 36) → replica applies via
`on_message` PUT branch (line 214–226, authorization = membership in current
preference set) → coordinator `on_message` PUT_OK branch counts **distinct**
senders (`p.responded` set, line 254) → `finish_put` at W=2 (line 256) →
browser `pixel` event. A `pixel` event therefore means **2 distinct designated
replicas applied the versioned write**; enqueue-only is never an ACK.

Crash/rejoin: `region_store_.clear()` on stale-generation reset
(`peer_node.cpp:448`); policy snapshot application queues a backfill for every
region the node newly prefers and lacks (`placement_runtime.cpp:915–924`) →
`backfill_step()` one region per 500 ms tick (`peer_node.cpp:880–882`) →
quorum `get()` with **R=2, first-R merge** (the defect) → empty merge marked
`success=true version=0` and never retried → anti-entropy push (2 s cadence) or
a later GET's read-repair (`replication_manager.cpp:138–150`) is the only
recovery.

Snapshot path: browser `get_canvas` → `start_snapshot` → `request_next_region`
iterates all 256 regions with a quorum `get()` (R=2 of current targets, first-R
merge) → `deliver_region` streams only `lamport != 0` pixels
(`peer_node.cpp:471–519`). A gateway snapshot is therefore a per-region R=2
merged view at read time, **not** a local-only or owner-only dump; an empty
merged region is silently delivered as "no pixels" (no read error), which is
how the false-complete backfill becomes a verifier-visible absence.

## 4. Root-cause verdict

**CONFIRMED** (deterministic reproduction, archived logs, code path):

> A read quorum completes on the first R responses and merges only those;
> the backfilling node's own empty reply (and that of any other
> still-empty rejoining node) satisfies R=2; the populated reply that
> arrives moments later is discarded as late; `backfill_step` then records
> `success=true version=0`, inserts the empty region into the store, and
> never retries. Only the slow anti-entropy push eventually repairs it.
> Snapshot reads on gateways reproduce the same race at read time.

This violates invariant I3 (complete backfill semantics) and explains the
isolated first-round absence and the persistent Stress50 mismatches; it does
**not** imply acknowledged-write loss.

## 5. Patch (smallest viable)

`src/replication/replication_manager.{hpp,cpp}` only (+ one call site in
`src/peer/peer_node.cpp` reset path):

1. `get()` gains defaulted flags `complete_all=false`, `exclude_self=false`
   (existing callers unchanged). With `complete_all`, a read finishes only when
   every expected target has responded or failed (or at the 3 s deadline, which
   succeeds with the best available merge if ≥R responses were collected).
2. Backfill uses `complete_all=true, exclude_self=true` — the node never counts
   its own empty store, and late populated replies are always merged.
3. `backfill_step()` validates the merge and classifies it: `version>0` with
   every source seen → `complete`; `version>0` with silent/failed sources →
   `complete_partial` (data present; anti-entropy backstop); `version==0` with
   every source responding empty → `empty_valid` (accepted once, no retry);
   `version==0` with silent/failed sources → `retry` up to 4 bounded attempts
   then `empty_partial_accepted`. The old `get_region` guard is replaced by an
   explicit `backfill_resolved_` set so an empty merge can no longer block
   re-pulls.
4. `reset_repair_state()` (clears queue/attempts/resolved, bumps a state epoch)
   is called from `reset_to_joiner`; backfill callbacks from a previous state
   epoch are dropped, closing the stale-callback hole (§4.1 of the handoff).

Unchanged: R/W quorum semantics, PUT path, snapshot read path, message wire
format, metrics CSV columns (classification rides the existing `reason`
column), policy evaluation.

## 6. Regression evidence

- New deterministic C++ test `tests/test_backfill_completeness.cpp` (4-node
  synchronous cluster, N=4/R=2/W=2): (a) old-style first-R GET reproduces the
  exact `version=0` false complete from the archive (log-for-log:
  `from=1 count=1/2 … from=2 count=2/2 … quorum-ignore sender=4`);
  (b) `complete_all+exclude_self` backfill merges the populated fourth replica
  (`classification=complete version=50`); (c) genuinely empty region retries 4
  times then `classification=empty_accepted`, and never re-pulls afterwards;
  (d) stale store epoch drops in-flight backfill callbacks and re-pull works
  after reset.
- Full `ctest` run: **20/20 pass** (19 pre-existing + the new
  `backfill-completeness` test). No A/B strategy behavior changed.
- Strategy B live suite (`tests/test_lab06_strategy_b.py`, stable/churn/
  recovery + A regression): **ALL PASS** with the patched binary.
- Isolated deep test (T4), final binary, run `B_deep_isolated_03`: **all four
  recovery rounds clean — 1,080 comparisons/round × 10 gateways = 0
  mismatches, 0 read errors, including round 0** (pre-patch archive: 90
  round-0 absences on regions 23/24). `probe_summary.json` now carries
  `per_round` diagnostics. An earlier post-patch run (`B_deep_isolated_02`,
  first patch revision) had the same clean result.
- Machine-readable diagnostics: `experiments/diagnostics/strategy_b_case_01/`
  (10 files; `all_reader_converged=true`, 339 classified backfill events with
  the `complete`/`complete_partial`/`empty_valid`/`empty_partial_accepted`
  taxonomy).

## 7. Remaining limits

- **Backfill drain rate:** activation backfill still drains one region per
  500 ms tick, so a node that restarts with all 256 regions queued takes up to
  ~128 s to re-pull everything (each pulled region is now complete and
  honestly classified). Under repeated 12 s churn windows, high-key regions
  may stay empty on the rejoined node until an idle window — reads racing to
  them can still see absences, which read-repair and anti-entropy then
  correct. A bounded multi-region drain (e.g. 4/tick) is a possible follow-up
  if the fixed 60 s checkpoint must converge under this profile; it was not
  changed here to keep load behavior stable for A/B comparison.
- Anti-entropy push remains the only repair for a node that restarts while
  its sources are unreachable; a full per-node sweep of ~77–102 owned regions
  is ~2–3.5 minutes, so a fixed 60 s settle is a checkpoint, not a convergence
  proof. Convergence progress is diagnosable per region/reader/round.
- Placement-rotated surplus replicas: data held only by nodes that are no
  longer in a region's current preference set is invisible to backfill and
  anti-entropy pushes of that region (W=2 holders can rotate out after N
  changes). This pre-existing I5 gap is outside the confirmed defect and was
  not changed.
- Physical-replica completeness is not directly inspectable from gateways;
  "0 gateway mismatches" ≠ "every designated replica holds the value".
- A fresh Stress50 A/B rerun (T6/T7, fresh dirs, same seed/fault plan) is the
  remaining recommended validation and was not run as part of this patch.

## 8. Research interpretation

Write availability (W=2 distinct applied ACKs) was never in question — the
confirmed defect sits in **repair/backfill completeness and read-time
visibility**, not in the write quorum. The observed mismatches are transient
read-time absences on gateway snapshots caused by incomplete backfill merging;
they are not evidence of acknowledged-write loss. The fix makes backfill
completion meaningful (complete merge, or explicit bounded-verified empty) and
restores the intended contract: after a rejoining node has backfilled a region,
every R=2 read of that region merges populated copies.

## 9. Machine-readable diagnostics

`experiments/diagnostics/strategy_b_case_01/` — `environment.json`,
`case_summary.json`, `mismatch_all.csv`, `per_region_summary.csv`,
`per_reader_summary.csv`, `backfill_trace.csv`, `repair_trace.csv`,
`policy_epoch_trace.csv`, `convergence_rounds.csv`, `node_logs/` (see
`experiments/build_diagnostics.py` for provenance).
