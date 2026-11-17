#!/bin/bash
# Timeline live smoke: short A and B runs + invariant checks on the outputs.
set -u
cd ~/projects/p2place
rm -rf experiments/results/TL_smoke_A experiments/results/TL_smoke_B
python3 experiments/run_experiment.py --strategy A --seed 7 --seconds 25 \
  --churn-profile none --out experiments/results/TL_smoke_A 2>&1 | tail -n 4
python3 experiments/run_experiment.py --strategy B --seed 7 --seconds 25 \
  --churn-profile none --out experiments/results/TL_smoke_B 2>&1 | tail -n 4
python3 - <<'EOF'
import csv, json, pathlib
for name, strat in (('TL_smoke_A','A'), ('TL_smoke_B','B')):
    out = pathlib.Path('experiments/results') / name
    summary = json.load(open(out/'summary.json'))
    tl = summary.get('replication_timeline', {})
    assert tl.get('complete'), f'{name}: timeline incomplete'
    with open(out/'replication_summary.csv') as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 26, f'{name}: expected 26 samples, got {len(rows)}'
    for r in rows:
        assert int(r['n2_regions'])+int(r['n3_regions'])+int(r['n4_regions']) == 256
    with open(out/'node_replication_timeline.csv') as f:
        nrows = list(csv.DictReader(f))
    assert len(nrows) == 26*10, f'{name}: expected 260 node rows, got {len(nrows)}'
    t5 = [x for x in nrows if float(x['time_s'])==5.0]
    assert len(t5) == 10, f'{name}: all ten nodes must appear at t=5'
    with open(out/'region_replication_events.csv') as f:
        ev = list(csv.DictReader(f))
    print(f'{name} ({strat}): samples={len(rows)} node_rows={len(nrows)} '
          f'events={len(ev)} policy_changes={tl.get("policy_changes")} '
          f'placement_changes={tl.get("placement_changes")} conflicts={tl.get("conflicts")} '
          f'complete={tl.get("complete")}')
    if strat == 'A':
        assert all(r['n3_regions']=='256' for r in rows), 'A must stay N3'
        assert all(e['event_type'] in ('placement_change', 'initial') for e in ev), \
            'A must emit no n_change events'
        assert sum(1 for e in ev if e['event_type'] == 'initial') == 256, \
            'A must emit the 256-region initial baseline'
        print(f'  PASS: A constant N3 verified over {len(rows)} samples '
              '(256 initial rows + placement events only)')
    else:
        n2_any = any(int(r['n2_regions'])>0 for r in rows)
        print(f'  n2_regions at t=20: {rows[20]["n2_regions"]} (B adapts: {n2_any})')
print('SMOKE PASS: live A and B timelines complete with invariants intact')
EOF
