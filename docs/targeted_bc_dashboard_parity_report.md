# Targeted B/C Dashboard Parity Report

**Date:** 2026-11-18
**Scope:** make `experiments/run_targeted_bc.py` outputs load in the real
`p2place_dashboard` using the same contract as `experiments/run_experiment_A_vs_B.py`.
**Status:** exporter and schema tests **PASS**. The final 300 s B/C pair was **not
executed** (stopped by user request); a 90 s B/C smoke pair and a synthetic
300 s fixture were used for validation.

---

## 1. Bottom line

`experiments/run_targeted_bc.py` now writes the canonical A/B files with the
exact headers, a real per-incarnation process/log/metrics layout, accepted-event
deduplication, 1 Hz cluster and per-node replication timelines, planned-vs-actual
fault separation, honest targeted verification scope, and deterministic
dashboard ZIPs.

The dashboard import flow was audited and exercised in local Chrome:

- the existing A/B example ZIP still loads and renders every tab;
- an individual B ZIP and an individual C ZIP each load as **one** profile;
- a combined ZIP loads B and C as two profiles that can be compared;
- every view (overview, fault timeline, latency, nodes/health, placement,
  replication, details, compare) rendered with no JS exceptions or console
  errors.

No A/B runner, C++ policy, core replication, quorum, placement, write-cadence,
or fault-timing behavior was changed.

---

## 2. Dashboard audit — actual import contract

Inspected from the installed dashboard, not guessed:

`C:\Users\phuhu\Downloads\p2place_experiment_lab\p2place_dashboard\index.html`,
`app.js`, `README.md`, `dashboard_test.js`.

**Supported imports:** `.zip` and `.json` via the file input. A ZIP may contain
one `summary.json` root or several nested roots (e.g. `B/summary.json`,
`C/summary.json`), which create one profile per root.

**Important limitation found:** the “Import folder contents” control sets
`webkitdirectory`, but `importFiles()` filters the selection to `.zip`/`.json`
files and imports them individually. It is **not** a folder-as-profile import.
The practical local-file route is a ZIP per profile (or a combined ZIP with
B/C roots). The README’s “each experiment folder with its own summary.json
becomes one profile” applies to ZIP contents.

**Profile files the dashboard actually reads:**

| File | Dashboard use |
|---|---|
| `summary.json` | metadata, attempts, verification checkpoints, final placement |
| `config.json`, `fault_plan.json` | settings, planned events |
| `runner_operations.csv` | per-attempt success/latency/phase charts |
| `metrics/*/operations_*.csv` | per-peer operation records |
| `metrics/*/peer_health_*.csv` | health transitions/scores (reporter-local time) |
| `metrics/*/policy_*.csv` | policy observations/risk/copies |
| `replication_summary.csv` | cluster N distribution, desired/assigned copies |
| `node_replication_timeline.csv` | per-node assignments, membership, health |
| `region_replication_events.csv` | 256-region N heatmap / step chart |
| `placement_snapshots.jsonl` | committed membership generations |
| `replica_backfill_events.csv` | completion/failure markers |
| `*.state` | saved balanced placement membership |
| `*.log` | selected log warnings/membership lines |
| `node_process_events.csv` | not parsed by the dashboard, but part of the A/B contract and node-liveness reconstruction |

**Dashboard path gotcha fixed:** `readZip` excludes nested
`.../metrics/...` copies of replication files only when the string contains
`/metrics/`. A root-level `metrics/` folder does **not** match. Individual
targeted ZIPs therefore put the profile under a top-level `B/` or `C/` folder,
so `B/metrics/...` is correctly excluded and the root canonical CSVs are read
once. Without this, placement snapshots/events were double-counted
(e.g. 18 instead of 9 placement snapshots).

---

## 3. Parity matrix

| A/B producer artifact | Dashboard reader | Targeted producer (now) | Status |
|---|---|---|---|
| `config.json` | settings | same, targeted CLI values | exact shape |
| `summary.json` | metadata/metrics/verification/final placement | merged A/B keys + targeted evidence | compatible, targeted keys preserved |
| `runner_operations.csv` | attempt charts | every `puts.csv` row (scheduled + immediate probes) | exact header |
| `node_process_events.csv` | A/B contract; liveness reconstruction | source-recorded launch/kill/restart with real PIDs/boots | exact header for new runs; legacy marked reconstructed |
| `fault_plan.json` | planned event table + timeline | planned config kills/restarts | planned only |
| `region_replication_events.csv` | N event stream | accepted initial/n_change/placement events, deduped | exact header |
| `placement_snapshots.jsonl` | placement generations | real committed snapshots with `time_s` added | exact fields |
| `replication_summary.csv` | cluster N/copies | 1 Hz rows, all 256 regions | exact header |
| `node_replication_timeline.csv` | per-node N/assignments/health | 1 Hz × 10 nodes | exact header |
| `replica_backfill_events.csv` | backfill markers | canonical C++ `replica_backfill_events.csv`, converted to `time_s`; repair events only as fallback | compatible |
| `node<N>_boot<K>.log` | log warnings | real per-boot logs; `node<N>/node.log` remains a hard link to current boot | A/B naming |
| `metrics/node<N>_boot<K>/*` | ops/health/policy | per-incarnation metrics copies | A/B layout |
| `node<N>.state` | placement/membership | real state files from the peer | A/B naming |

### Exact headers written

```text
runner_operations.csv
attempt,time_s,phase,workload_class,via_node,region,x,y,color,success,latency_ms,error,error_category

node_process_events.csv
time_s,node_id,boot,action,pid

region_replication_events.csv
run_id,time_s,event_type,region,old_n,new_n,placement_gen,placement_fp,policy_epoch,authority_node,reason

replication_summary.csv
run_id,time_s,placement_gen,placement_fp,policy_epoch,n2_regions,n3_regions,n4_regions,avg_target_n,total_desired_copies,total_assigned_copies,underassigned_regions,processes_running,members_committed

node_replication_timeline.csv
run_id,time_s,node_id,process_running,membership_state,health,placement_gen,placement_fp,policy_epoch,assigned_regions,n2_regions,n3_regions,n4_regions,avg_target_n,desired_region_copies,connected_assigned_regions

replica_backfill_events.csv (root, converted)
run_id,time_s,node_id,region,placement_gen,policy_epoch,result,reason
```

`success` / `process_running` are written as `True`/`False` (the dashboard’s
`truth()` accepts `true`/`1`/`True`).

---

## 4. Before / after result tree

**Before** (targeted run only):

```text
<out-root>/seed<seed>/B/
  config.json summary.json puts.csv replication_timeline.csv
  write_buckets.csv gets.csv events.csv readiness.json readback.json
  ws_raw.jsonl comparison.md
  node1..node10/node.log
  node1..node10/*.csv            # live targeted metrics
```

**After** (original files preserved; dashboard artifacts added):

```text
<out-root>/seed<seed>/
  B_targeted_seed<seed>.zip
  C_targeted_seed<seed>.zip
  targeted_BC_seed<seed>.zip      # combined: B/ + C/ profiles
  B/
    # original targeted files above, untouched
    summary.targeted.json         # pre-export original summary
    runner_operations.csv
    node_process_events.csv
    fault_plan.json
    region_replication_events.csv
    placement_snapshots.jsonl
    replication_summary.csv
    node_replication_timeline.csv
    replica_backfill_events.csv
    node1.state .. node10.state
    node1_boot1.log .. node10_boot1.log
    node3_boot2.log node4_boot2.log
    metrics/node1_boot1/... .. metrics/node10_boot1/...
    metrics/node3_boot2/... metrics/node4_boot2/...
    node1..node10/                # retained targeted live logs/metrics
  C/  (same layout)
```

The dashboard ZIP is a clean subset: canonical root files, state files,
per-boot logs, and per-boot metrics. It does not include the targeted-only
diagnostics (they remain in the result folder).

---

## 5. Validation evidence

### 5.1 90 s B/C smoke pair (real peers, real faults)

Region 147, owners 3/4, seed 712367, placement generation 10, 10 nodes.
Timeline shortened to 90 s (kill 30, rejoin 45, heat until 60); C’s idle
verdict is FAIL there only because the 60 s write window cannot decay before
t=90.

| Metric | B | C |
|---|---|---|
| Attempts / client ACKs | 351 / 273 | 351 / 351 |
| Immediate probes | 0/8 ACK | 8/8 ACK |
| Fault records (kill+restart) | 4 | 4 |
| `replication_summary.csv` rows | 91 | 91 |
| `node_replication_timeline.csv` rows | 910 | 910 |
| Accepted policy events | 577 | 576 |
| Placement snapshots | 9 | 9 |
| Backfill events (canonical) | 1,339 | 1,341 |
| Peer-health records | 323 | 318 |
| Saved state files | 10 | 10 |
| Per-boot logs | 12 | 12 |
| Verification | no ACKed probes → N/A | 8 sampled, 0 missing, reader node 1 |

Both exporter runs reported `dashboard_export.complete=true`.

### 5.2 300 s contract fixture (synthetic, exact A/B shape)

`experiments/tests/test_targeted_dashboard_export.py` builds a structurally
complete 10-node/300-second run and exports it. **46/46 checks PASS**, including:

- `replication_summary.csv`: **301 rows**, t=0.0 … 300.0
- `node_replication_timeline.csv`: **3,010 rows**
- `n2_regions + n3_regions + n4_regions == 256` every sample
- per-node `assigned_regions` sum == cluster `total_assigned_copies` every sample
- operation rows/attempt IDs/ACKs reconcile with `puts.csv`
- 256 initial policy events, no duplicate accepted events, sorted times
- `fault_plan.json` = 4 **planned** events at configured 48/78 s
- `summary.executed_faults` = 4 **actual** events with planned/actual times
- verification scope is probe-only; `sampled_pixels=8`, `missing=0`,
  `checked_nodes=[1]`; **no fabricated `verification_immediate`**
- `final_placement` has 10 A/B-style `[generation, members]` entries
- original targeted `final_placement` preserved as `targeted_final_placement`
- `summary.targeted.json` is the untouched pre-export summary
- profile ZIP contains the canonical files, `node1.state`, `node1_boot1.log`,
  and `metrics/node1_boot1/policy_node1.csv`
- two consecutive exports are **byte-identical** (9 canonical files + ZIP)

### 5.3 Idempotence

Two consecutive `--export-only` runs on the smoke root produced identical
SHA-256 hashes for:

```text
summary.json, runner_operations.csv, node_process_events.csv,
region_replication_events.csv, replication_summary.csv,
node_replication_timeline.csv, replica_backfill_events.csv,
fault_plan.json, placement_snapshots.jsonl
+ B/C individual ZIPs + combined ZIP
```

ZIP entries use a fixed timestamp; no export timestamp is injected.

### 5.4 Actual dashboard UI import (local Chrome, executed before the stop request)

Evidence under `C:\Claude\P2Place\dashboard_import_test\`:
`evidence_A\`, `evidence_BC_individual_v2\`, `evidence\`, screenshots and
`ui_import_result.json` per run.

- **A/B regression:** existing `A_10_seed43_stress50.zip` loads as one profile;
  14,400 attempts, 601 cluster samples, 6,010 node samples, 120 faults, 7 tabs,
  5 overview plots, 5 replication plots, no JS errors.
- **Individual B ZIP:** one profile; 351 attempts, 91/910 rows, 4 faults,
  9 placement snapshots, 7 tabs, 4 overview plots (verification chart correctly
  absent), 6 replication plots, no JS errors.
- **Individual C ZIP:** one profile; 351/351 attempts, 91/910 rows, 4 faults,
  9 snapshots, 7 tabs, 5 overview plots, 6 replication plots, no JS errors.
- **Combined ZIP:** B and C load as separate profiles; compare view covers all
  imported profiles; all charts render.
- Import increments confirmed each individual ZIP adds exactly **one** profile.

---

## 6. What changed

### `experiments/run_targeted_bc.py`

1. **Source process logging** — `ProcessRecorder` writes
   `node_process_events.csv` during the run: real launch/kill/restart PIDs,
   boot numbers, CLOCK_MONOTONIC times relative to the workload anchor.
   Pre-anchor launches are negative, not back-dated.
2. **Per-incarnation evidence** —
   - peer stdout: `node<N>_boot<K>.log`; `node<N>/node.log` is a hard link to
     the current boot,
   - metrics copied to `metrics/node<N>_boot<K>/` at boot end,
   - `--state-file <out>/node<N>.state` passed to every peer.
3. **Dashboard exporter** —
   - operations from `puts.csv` (pixel-echo ACKs only),
   - accepted policy events deduped by authority-accepted key, sorted, stale
     epochs dropped,
   - placement snapshots deduped by generation/fingerprint/members,
   - initial target fallback **N=2** for B/C (documented minimum), not A’s N=3,
   - all 256 regions in cluster and node timelines,
   - health aligned from `peer_health` per boot (UNKNOWN only when absent),
   - `replica_backfill_events.csv` from the canonical C++ stream,
   - 301/3,010 row checks and assignment invariants,
   - planned `fault_plan.json` vs actual `summary.executed_faults`,
   - probe-only verification scope; no fabricated immediate checkpoint,
   - preserved original targeted summary and final placement.
4. **Deterministic ZIPs** — individual `B/`/`C/` profile ZIPs and combined
   `B/`+`C/` ZIP; fixed ZIP timestamps; `--export-only` idempotent.
5. **A/B-style progress lines** — `[node] start/restart`, `[fault] SIGKILL`,
   `[dashboard]` export lines, plus existing targeted progress.
6. **`--export-only` and `--compare-only`** — export from existing B/C dirs,
   build individual + combined ZIPs, return nonzero when export is incomplete.
7. **Aborted-run handling** — observed runtime limits the timeline; `partial_run`
   and `duration_source` mark partial profiles instead of inventing 300 s.

### `experiments/tests/test_targeted_dashboard_export.py` (new)

Standalone 46-check schema/invariant/idempotence test, including the exact
301/3,010 row contract and fault/verification semantics.

### Unchanged

- `experiments/run_experiment_A_vs_B.py`
- all C++ policy/replication/placement/quorum code
- the targeted experiment’s science: 10 nodes, hot region, matched B/C seed and
  workload, strict readiness, simultaneous original-owner SIGKILL, pixel-echo
  client ACK, 8 immediate probes, rejoin, read-back, idle decay, PASS/FAIL/
  INCONCLUSIVE.

---

## 7. Known unknowns / provenance (not fabricated)

| Item | Behavior |
|---|---|
| `verification_immediate` | **Not recorded** for targeted runs; intentionally omitted. Only ACKed immediate-probe pixels were read back after rejoin. |
| `verification_after_settle` | Probe-only scope: 8 ACKed pixels via gateway node 1. C: 8 sampled/0 missing; B: 0 ACKed probes → N/A. Not a full-canvas or all-reader check. |
| `checked_nodes` | `[1]` for C (gateway reader); the dashboard warns reader coverage is 1 of 10. |
| Health | Aligned from real `peer_health` rows per boot when present; otherwise `UNKNOWN`. |
| Legacy exports | If no source `node_process_events.csv`, launches/PIDs are marked `reconstructed_legacy`; no invented PIDs. |
| Owner table | Read from real `node*.state`; if missing, assignment columns are blank and the export records an error. |
| Initial N without events | Documented N=2 fallback for B/C, with warning; no successful-convergence claim. |
| Stale/duplicate policy events | Deduped by accepted-event key; per-region policy epochs must be nondecreasing, stale drops counted. |
| Partial/aborted runs | `partial_run=true`, timeline ends at observed runtime; no fabricated full-duration samples. |
| Dashboard duration tail | The targeted run stops offered writes at 180 s while the experiment runs to 300 s; the dashboard’s workload-success buckets after 180 s are empty by design, while replication charts continue to 300 s. |

---

## 8. Commands used / ready

**Executed (90 s smoke + export):**

```bash
python3 -u experiments/run_targeted_bc.py --strategy B --seed 712367 \
  --out-root experiments/results/dashboard_smoke \
  --seconds 90 --kill-at 30 --rejoin-at 45 --heat-until 60 \
  --stabilize 5 --readback-timeout 25

python3 -u experiments/run_targeted_bc.py --strategy C --seed 712367 \
  --out-root experiments/results/dashboard_smoke \
  --seconds 90 --kill-at 30 --rejoin-at 45 --heat-until 60 \
  --stabilize 5 --readback-timeout 25

python3 -u experiments/run_targeted_bc.py --export-only --seed 712367 \
  --out-root experiments/results/dashboard_smoke

python3 -u experiments/tests/test_targeted_dashboard_export.py
```

**Not executed (stopped by user request) — ready:**

```bash
python3 -u experiments/run_targeted_bc.py --strategy B --seed 712367 \
  --out-root experiments/results/bc_targeted_dashboard_final
python3 -u experiments/run_targeted_bc.py --strategy C --seed 712367 \
  --out-root experiments/results/bc_targeted_dashboard_final
python3 -u experiments/run_targeted_bc.py --compare-only --seed 712367 \
  --out-root experiments/results/bc_targeted_dashboard_final
```

**Historical `--export-only` command:** `experiments/results/bc_targeted_v5`
does not exist in the current tree (the earlier v4/B/C result dirs were absent
when this work began). The command is implemented and was validated on
`experiments/results/dashboard_smoke`. To use the handoff’s exact command,
point `--out-root` at a seed root that contains `B/` and/or `C/`; it launches
no peers and is idempotent.

---

## 9. Files

- `experiments/run_targeted_bc.py` — hardened runner/exporter
- `experiments/tests/test_targeted_dashboard_export.py` — new 46-check test
- `experiments/results/dashboard_smoke/seed712367/` — 90 s B/C smoke results,
  `summary.targeted.json` originals, canonical CSVs/JSONL, per-boot logs/metrics,
  individual and combined ZIPs
- `C:\Claude\P2Place\dashboard_import_test\` — UI test script, ZIPs, evidence,
  screenshots, `ui_import_result.json`
- `C:\Claude\P2Place\Targeted_BC_Dashboard_Parity_Report.md` — this report

No claim is made that a full 300 s targeted B/C dashboard profile was generated
in this session. The 300 s shape is proven by the exact-row synthetic fixture
(301/3,010), and the real 90 s B/C profiles import and render every dashboard
view.
