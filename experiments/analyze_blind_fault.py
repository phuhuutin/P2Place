#!/usr/bin/env python3
"""Extract the blind fault-trial evidence bundle from one run directory.

Usage: python3 experiments/analyze_blind_fault.py <run_dir>/fault
Prints: region N timeline, per-added-target urgent enqueue/start/completion
times (monotonic ns + latency vs the N4 epoch), B/C ACK counts, read-back.
"""
import csv
import glob
import json
import pathlib
import re
import sys


def main() -> int:
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else
                        'experiments/results/strategy_c_blind')
    if (root / 'fault').is_dir():
        root = root / 'fault'
    results = {}
    if (root.parent / 'blind_results.json').exists():
        results = json.loads((root.parent / 'blind_results.json').read_text())
    fault = results.get('fault', {})
    region = (fault.get('C') or {}).get('region')
    print(f'run: {root.parent.name}  region: {region}')
    if region is None:
        print('no fault outcome recorded')
        return 1
    # --- Authority N timeline for the region ------------------------------
    print('\n== region N timeline (authority metrics) ==')
    path = root / 'C' / 'metrics1' / 'region_replication_events.csv'
    n4_ts = None
    for r in csv.DictReader(path.open()):
        if r['event_type'] in ('initial', 'n_change') and int(r['region']) == region:
            print(f"  t_ns={r['monotonic_ns']} {r['event_type']} "
                  f"{r.get('old_n','')}->{r['new_n']} epoch={r['policy_epoch']}")
            if int(r['new_n']) == 4 and n4_ts is None:
                n4_ts = int(r['monotonic_ns'])
    # --- Per-target urgent pipeline ---------------------------------------
    print('\n== added-target repair pipeline ==')
    for strat_dir in ('C',):
        nodes = set()
        for p in glob.glob(str(root / strat_dir / 'metrics*')):
            m = re.search(r'metrics(\d+)', p)
            if m:
                nodes.add(int(m.group(1)))
        for node in sorted(nodes):
                f = root / strat_dir / f'metrics{node}' / 'replica_repair_events.csv'
                if not f.exists():
                    continue
                rows = [r for r in csv.DictReader(f.open())
                        if r['region'] == str(region)]
                if not any(r['event'] in ('urgent_enqueued', 'backfill_started',
                                          'backfill_completed') for r in rows):
                    continue
                print(f'-- node {node}:')
                enq = sub = done = None
                for r in rows:
                    ts = int(r['monotonic_ns'])
                    rel = f'{(ts - n4_ts) / 1e6:+.0f}ms vs N4' if n4_ts else ''
                    print(f"   ts={ts} {rel:>16} {r['event']:<20} "
                          f"{r['classification']:<24} epoch={r['policy_epoch']} "
                          f"attempts={r['attempts']}")
                    if r['event'] == 'urgent_enqueued' and enq is None:
                        enq = ts
                    elif r['event'] == 'backfill_started' and sub is None:
                        sub = ts
                    elif r['event'] == 'backfill_completed' and done is None:
                        done = ts
                if n4_ts and done:
                    print(f'   LATENCY urgent_enqueued->completed: '
                          f'{(done - enq) / 1e6:.0f}ms; '
                          f'N4_apply->completed: {(done - n4_ts) / 1e6:.0f}ms')
    # --- B/C ACK outcomes --------------------------------------------------
    print('\n== fault outcomes ==')
    for strat in ('C', 'B'):
        r = fault.get(strat) or {}
        print(f"  {strat}: region={r.get('region')} killed={r.get('killed')} "
              f"pre_fault_n={r.get('pre_fault_n')} ACK={r.get('acked')}/{r.get('total')} "
              f"heat={r.get('heat_s')}s heater={r.get('heater')}")
        print(f"     read-back: {r.get('snapshot_observed')}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
