#!/usr/bin/env python3
"""Schema/invariant test for the targeted B/C dashboard exporter.

Builds a small synthetic but structurally complete 10-node, 300-second run,
exports it, and checks the A/B dashboard contract:

  * exact CSV headers and row counts (301 cluster / 3,010 node samples),
  * n2+n3+n4 == 256 and per-node assignment sums == cluster assigned copies,
  * operation/ACK reconciliation with puts.csv,
  * planned vs actual fault separation,
  * verification scope (probe read-back only; no fabricated immediate check),
  * per-boot metrics/logs and deterministic, idempotent ZIP output.

Run: python3 experiments/tests/test_targeted_dashboard_export.py
"""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
import zipfile

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'experiments'))
import run_targeted_bc as rtb  # noqa: E402

NODES = 10
DURATION = 300
ANCHOR_NS = 5_000_000_000  # arbitrary CLOCK_MONOTONIC value
GEN = 10
OWNERS = [3, 4]
MEMBERS = list(range(1, NODES + 1))


def write_csv(path: Path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def state_text():
    owner_values = [((region * 7) % NODES) + 1 for region in range(256)]
    lines = ['P2PLACE_BALANCED_V2 10 10',
             ' '.join(map(str, MEMBERS)),
             ' '.join(map(str, MEMBERS))]
    for start in range(0, 256, 16):
        lines.append(' '.join(map(str, owner_values[start:start + 16])))
    lines.append('CHECKSUM 13788559444936516889')
    return '\n'.join(lines) + '\n'


def build_fixture(root: Path) -> Path:
    out = root / 'B'
    out.mkdir(parents=True)
    (out / 'config.json').write_text(json.dumps({
        'strategy': 'B', 'seed': 123, 'nodes': NODES, 'seconds': DURATION,
        'rate': 6.0, 'kill_at': 48.0, 'rejoin_at': 78.0,
        'heat_until': 180.0, 'probes': 8,
    }, indent=2) + '\n')

    puts = []
    for i in range(1, 21):
        ok = i != 7
        puts.append({
            'time_s': f'{i * 0.5:.3f}',
            'phase': 'immediate_fault_probe' if i == 19 else
                     'fault_window' if 96 <= i * 0.5 < 156 else 'heat',
            'sequence': i, 'region': 147 + (i % 2),
            'x': 48 + (i % 16), 'y': 144 + (i % 16),
            'color': f'#{i:06X}', 'ack': 1 if ok else 0,
            'result': 'ack' if ok else 'nak',
            'reason': 'pixel_echo' if ok else 'error:write quorum unavailable',
            'latency_ms': f'{1.0 + i / 10:.3f}', 'total_ms': '2.0',
            'offered_offset_s': f'{i * 0.5:.3f}',
        })
    write_csv(out / 'puts.csv',
              ('time_s', 'phase', 'sequence', 'region', 'x', 'y', 'color',
               'ack', 'result', 'reason', 'latency_ms', 'total_ms',
               'offered_offset_s'), puts)

    # Real source-style process events (negative pre-anchor launches).
    events = [{'time_s': f'{-20 + n * 0.5:.3f}', 'node_id': n, 'boot': 1,
               'action': 'launch', 'pid': 1000 + n} for n in MEMBERS]
    events += [{'time_s': '48.000', 'node_id': n, 'boot': 1, 'action': 'kill',
                'pid': 1000 + n} for n in OWNERS]
    events += [{'time_s': '78.000', 'node_id': n, 'boot': 2, 'action': 'launch',
                'pid': 2000 + n} for n in OWNERS]
    write_csv(out / 'node_process_events.csv',
              ('time_s', 'node_id', 'boot', 'action', 'pid'), events)

    for n in MEMBERS:
        (out / f'node{n}.state').write_text(state_text())
        (out / f'node{n}_boot1.log').write_text(f'node {n} boot 1 log\n')
    for n in OWNERS:
        (out / f'node{n}_boot2.log').write_text(f'node {n} boot 2 log\n')

    for n in MEMBERS:
        boot = out / 'metrics' / f'node{n}_boot1'
        boot.mkdir(parents=True)
        event_rows = []
        for region in range(256):
            event_rows.append({
                'run_id': 'B_seed123',
                'monotonic_ns': ANCHOR_NS - 5_000_000_000,
                'event_type': 'initial', 'region': region, 'old_n': '',
                'new_n': 2, 'placement_gen': GEN, 'placement_fp': '111',
                'policy_epoch': 1, 'authority_node': 1,
                'reason': 'initial_snapshot',
            })
        event_rows.append({
            'run_id': 'B_seed123', 'monotonic_ns': ANCHOR_NS + 10_000_000_000,
            'event_type': 'n_change', 'region': 147, 'old_n': 2, 'new_n': 4,
            'placement_gen': GEN, 'placement_fp': '111', 'policy_epoch': 2,
            'authority_node': 1, 'reason': 'policy_apply',
        })
        event_rows.append({
            'run_id': 'B_seed123', 'monotonic_ns': ANCHOR_NS + 20_000_000_000,
            'event_type': 'n_change', 'region': 147, 'old_n': 4, 'new_n': 2,
            'placement_gen': GEN, 'placement_fp': '111', 'policy_epoch': 3,
            'authority_node': 1, 'reason': 'policy_apply',
        })
        write_csv(boot / 'region_replication_events.csv',
                  ('run_id', 'monotonic_ns', 'event_type', 'region', 'old_n',
                   'new_n', 'placement_gen', 'placement_fp', 'policy_epoch',
                   'authority_node', 'reason'), event_rows)
        (boot / 'placement_snapshots.jsonl').write_text(json.dumps({
            'monotonic_ns': ANCHOR_NS - 10_000_000_000, 'placement_gen': GEN,
            'placement_fp': '111', 'authority_node': 1,
            'member_ids': MEMBERS,
        }) + '\n')
        write_csv(boot / f'peer_health_node{n}.csv',
                  ('run_id', 'time_s', 'node_id', 'state', 'observed_seconds',
                   'availability_score'),
                  [{'run_id': 'B_seed123', 'time_s': 1.0, 'node_id': n,
                    'state': 'HEALTHY', 'observed_seconds': 1.0,
                    'availability_score': 1.0}])
        write_csv(boot / f'operations_node{n}.csv',
                  ('run_id', 'time_s', 'op', 'region', 'success', 'latency_ms', 'error'),
                  [{'run_id': 'B_seed123', 'time_s': 1.0, 'op': 'PUT',
                    'region': 147, 'success': 'true', 'latency_ms': 1.0, 'error': ''}])
        write_csv(boot / f'policy_node{n}.csv',
                  ('run_id', 'time_s', 'placement_gen', 'policy_epoch', 'region',
                   'strategy', 'risk_score', 'desired_n', 'assigned_n',
                   'confirmed_live_copies'),
                  [{'run_id': 'B_seed123', 'time_s': 1.0, 'placement_gen': GEN,
                    'policy_epoch': 1, 'region': 147, 'strategy': 'B',
                    'risk_score': 0.0, 'desired_n': 2, 'assigned_n': 2,
                    'confirmed_live_copies': 2}])
        write_csv(boot / 'replica_backfill_events.csv',
                  ('run_id', 'monotonic_ns', 'node_id', 'region', 'placement_gen',
                   'policy_epoch', 'result', 'reason'),
                  [{'run_id': 'B_seed123', 'monotonic_ns': ANCHOR_NS + 2_000_000_000,
                    'node_id': n, 'region': 147, 'placement_gen': GEN,
                    'policy_epoch': 1, 'result': 'ok', 'reason': 'backfill_ok'}])
        write_csv(boot / 'replica_repair_events.csv',
                  ('run_id', 'monotonic_ns', 'node_id', 'region', 'placement_gen',
                   'policy_epoch', 'event', 'classification', 'attempts'),
                  [{'run_id': 'B_seed123', 'monotonic_ns': ANCHOR_NS + 2_100_000_000,
                    'node_id': n, 'region': 147, 'placement_gen': GEN,
                    'policy_epoch': 1, 'event': 'backfill_completed',
                    'classification': 'complete', 'attempts': 1}])

    readback_visible = {f'{48 + i},{144 + i}': 90.0 for i in range(8)}
    summary = {
        'strategy': 'B', 'seed': 123, 'status': 'PASS',
        'status_basis': 'synthetic fixture',
        'config': {'nodes': NODES, 'seconds': DURATION, 'rate': 6.0,
                   'kill_at': 48.0, 'rejoin_at': 78.0, 'heat_until': 180.0},
        'timing': {'start_monotonic': ANCHOR_NS / 1e9, 'runtime_s': DURATION,
                   'kill_time_s': 48.0, 'rejoin_time_s': 78.0,
                   'heat_until_s': 180.0},
        'time_base': {'workload_start_monotonic_ns': ANCHOR_NS,
                      'source': 'fixture'},
        'process_events_source': 'recorded',
        'initial_placement_gen': GEN,
        'original_owners': OWNERS,
        'kill_time_s': 48.0, 'rejoin_time_s': 78.0,
        'final_placement': {'gen': GEN, 'fp': '111', 'owners': NODES},
        'readback': {'acked_probes': 8, 'visible': readback_visible,
                     'all_visible': True, 'status': 'ok',
                     'deadline_s': 40.0},
        'probe_summary': {'fixed_generation_acks': 8,
                          'fixed_generation': 8, 'total': 8},
    }
    (out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    return out


def read_rows(path: Path):
    with path.open(newline='') as handle:
        return list(csv.DictReader(handle))


def header(path: Path):
    with path.open(newline='') as handle:
        return next(csv.reader(handle))


def sha(path: Path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    failures = []

    def check(name, condition, detail=''):
        if condition:
            print(f'PASS: {name}')
        else:
            failures.append(name)
            print(f'FAIL: {name}' + (f' — {detail}' if detail else ''))

    with tempfile.TemporaryDirectory(prefix='p2place_dashboard_test_') as tmp:
        root = Path(tmp)
        out = build_fixture(root)
        info = rtb.export_targeted_dashboard(out)
        print(f"export info: {json.dumps(info)[:500]}")

        check('operations header', header(out / 'runner_operations.csv') ==
              list(rtb.DASHBOARD_OPERATION_COLUMNS), header(out / 'runner_operations.csv'))
        operations = read_rows(out / 'runner_operations.csv')
        puts = read_rows(out / 'puts.csv')
        check('operation rows reconcile with puts', len(operations) == len(puts))
        check('attempt ids unique', len({r['attempt'] for r in operations}) == len(operations))
        check('ACK count reconciles',
              sum(r['success'] == 'True' for r in operations) ==
              sum(r['ack'] == '1' for r in puts))
        check('phase/workload columns present',
              all(r['phase'] and r['workload_class'] for r in operations))

        check('node_process_events header',
              header(out / 'node_process_events.csv') ==
              ['time_s', 'node_id', 'boot', 'action', 'pid'])
        proc = read_rows(out / 'node_process_events.csv')
        check('real PIDs recorded', all(r['pid'] for r in proc))
        check('negative pre-anchor launch times', any(float(r['time_s']) < 0 for r in proc))
        check('kill/restart events recorded',
              {(r['action'], r['node_id']) for r in proc} >=
              {('kill', '3'), ('kill', '4'), ('launch', '3'), ('launch', '4')})
        check('restart uses boot 2', any(r['action'] == 'launch' and r['boot'] == '2'
                                         for r in proc))

        check('region events header',
              header(out / 'region_replication_events.csv') ==
              list(rtb.DASHBOARD_REGION_COLUMNS))
        events = read_rows(out / 'region_replication_events.csv')
        check('256 initial events', sum(r['event_type'] == 'initial' for r in events) == 256)
        keys = [(r['event_type'], r['region'], r['old_n'], r['new_n'],
                 r['placement_gen'], r['placement_fp'], r['policy_epoch'])
                for r in events]
        check('no duplicate accepted events', len(keys) == len(set(keys)))
        times = [float(r['time_s']) for r in events]
        check('event times sorted', times == sorted(times))

        check('replication_summary header',
              header(out / 'replication_summary.csv') ==
              list(rtb.DASHBOARD_CLUSTER_COLUMNS))
        check('node_replication_timeline header',
              header(out / 'node_replication_timeline.csv') ==
              list(rtb.DASHBOARD_NODE_COLUMNS))
        cluster = read_rows(out / 'replication_summary.csv')
        nodes = read_rows(out / 'node_replication_timeline.csv')
        check('301 cluster samples', len(cluster) == DURATION + 1, len(cluster))
        check('3010 node samples', len(nodes) == (DURATION + 1) * NODES, len(nodes))
        check('n2+n3+n4 == 256 every sample',
              all(int(r['n2_regions']) + int(r['n3_regions']) + int(r['n4_regions']) == 256
                  for r in cluster))
        sums_ok = True
        for index, row in enumerate(cluster):
            if row['total_assigned_copies'] == '':
                continue
            total = sum(int(nodes[index * NODES + offset]['assigned_regions'] or 0)
                        for offset in range(NODES))
            if total != int(row['total_assigned_copies']):
                sums_ok = False
                break
        check('node assignment sums equal cluster total', sums_ok)
        check('t=0 and t=300 samples present',
              cluster[0]['time_s'] == '0.0' and cluster[-1]['time_s'] == '300.0')
        check('initial target N=2 at t=0',
              int(cluster[0]['n2_regions']) == 256)

        plan = json.loads((out / 'fault_plan.json').read_text())
        summary = json.loads((out / 'summary.json').read_text())
        check('planned fault plan has 4 entries', len(plan) == 4, plan)
        check('planned times are configured, not actual',
              {p['at_s'] for p in plan} == {48.0, 78.0})
        executed = summary.get('executed_faults') or []
        check('executed faults separate from plan', len(executed) == 4)
        check('executed has planned + actual',
              all('planned_at_s' in e and 'actual_at_s' in e for e in executed))

        verification = summary.get('verification_after_settle') or {}
        check('verification scope is probe-only',
              'probes only' in str(verification.get('scope', '')).lower()
              or 'ACKed immediate probes' in str(verification.get('scope', '')))
        check('verification sampled 8 / missing 0',
              verification.get('sampled_pixels') == 8
              and verification.get('missing') == 0
              and verification.get('checked_nodes') == [1], verification)
        check('no fabricated immediate checkpoint',
              'verification_immediate' not in summary)

        final = summary.get('final_placement') or {}
        check('final_placement has 10 A/B-style entries', len(final) == NODES
              and all(isinstance(v, list) and len(v) == 2 for v in final.values()), final)
        check('targeted final placement preserved',
              summary.get('targeted_final_placement') ==
              {'gen': GEN, 'fp': '111', 'owners': NODES})
        check('targeted summary preserved without export keys',
              (out / 'summary.targeted.json').exists()
              and 'dashboard_export' not in json.loads(
                  (out / 'summary.targeted.json').read_text()))

        export = summary.get('dashboard_export') or {}
        check('dashboard export complete', export.get('complete') is True, export)
        check('timeline complete', summary.get('replication_timeline', {}).get('complete') is True)
        check('backfill root file present',
              (out / 'replica_backfill_events.csv').exists()
              and len(read_rows(out / 'replica_backfill_events.csv')) > 0)

        profile_zip = root / 'B_targeted_seed123.zip'
        check('profile ZIP exists', profile_zip.exists())
        with zipfile.ZipFile(profile_zip) as archive:
            names = set(archive.namelist())
        for expected in ('B/summary.json', 'B/runner_operations.csv',
                         'B/replication_summary.csv', 'B/node_replication_timeline.csv',
                         'B/node_process_events.csv', 'B/node1.state',
                         'B/node1_boot1.log',
                         'B/metrics/node1_boot1/policy_node1.csv'):
            check(f'ZIP contains {expected}', expected in names)

        tracked = ('summary.json', 'runner_operations.csv',
                   'node_process_events.csv', 'region_replication_events.csv',
                   'replication_summary.csv', 'node_replication_timeline.csv',
                   'replica_backfill_events.csv', 'fault_plan.json',
                   'placement_snapshots.jsonl')
        hashes_before = {name: sha(out / name) for name in tracked}
        zip_hash_before = sha(profile_zip)
        rtb.export_targeted_dashboard(out)
        hashes_after = {name: sha(out / name) for name in tracked}
        check('export is idempotent (files)', hashes_before == hashes_after)
        check('export is idempotent (ZIP)', zip_hash_before == sha(profile_zip))

    print('-' * 60)
    if failures:
        print(f'RESULT: FAIL ({len(failures)} check(s))')
        return 1
    print('RESULT: PASS')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
