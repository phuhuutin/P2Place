#!/usr/bin/env python3
"""Build the machine-readable diagnostic CSVs for the Strategy B N=2
regression investigation (deliverable 4 of the N2 handoff).

Usage:
  python3 experiments/build_n2_diagnostics.py \
      --archive experiments/investigations/n2_regression/B_10_seed43_stress50_300s_N=2 \
      --replay experiments/results/B_deep_isolated_N2 \
      --out experiments/diagnostics/strategy_b_n2_regression

Produces: sampled_pixels.csv, verification_all_gateways.csv,
local_replica_inventory.csv, quorum_response_trace.csv,
policy_transition_trace.csv, backfill_repair_trace.csv,
convergence_checkpoints.csv, case_summary.json.

All inputs are read-only; --out must be a new directory.
"""
from __future__ import annotations
import argparse
import collections
import csv
import json
import pathlib
import random
import re
import shutil
import sys

TRACED = [
    # (x, y, region, index, write_attempt, write_t, via, expected)
    (86, 140, 133, 198, 5157, 214.882, 5, '#9857B6'),
]

BACKFILL_RE = re.compile(
    r'\[backfill\] node=(\d+) region=(\d+) success=(true|false)'
    r'(?: version=(\d+))?(?: error=([^ ]+(?: [^ ]+)*))?')
REPAIR_RE = re.compile(
    r'\[replica-repair\] node=(\d+) from=(\d+) region=(\d+) version=(\d+)')
ANTI_RE = re.compile(
    r'\[anti-entropy\] node=(\d+) region=(\d+) to=(\d+) bytes=(\d+) success=(\w+)')
READ_REPAIR_RE = re.compile(
    r'\[read-repair\] node=(\d+) region=(\d+) target=(\d+)')
PUT_RE = re.compile(
    r'\[quorum\] node=(\d+) op=PUT request=(\d+) region=(\d+) index=(\d+) '
    r'N=(\d+) assigned=(\d+) W=(\d+) targets=([0-9,]*),')
REPLICA_PUT_RE = re.compile(
    r'\[replica-put\] node=(\d+) from=(\d+) request=(\d+) region=(\d+) '
    r'index=(\d+) version=(\d+) applied=(\d)')
ACK_RE = re.compile(
    r'\[quorum-ack\] node=(\d+) op=(PUT|GET) request=(\d+) from=(\d+) count=(\d+)/(\d+)')
GET_RE = re.compile(
    r'\[quorum\] node=(\d+) op=GET request=(\d+) region=(\d+) N=(\d+) '
    r'assigned=(\d+) R=(\d+).*targets=([0-9,]*),?')
IGNORE_RE = re.compile(
    r'\[quorum-ignore\] node=(\d+) reason=late-or-unknown request=(\d+) sender=(\d+)')


def boot_of(name):
    m = re.search(r'node(\d+)_boot(\d+)\.log', name)
    return (int(m.group(1)), int(m.group(2))) if m else (0, 0)


def region(x, y):
    return (y // 16) * 16 + x // 16


def write_csv(path, rows, cols):
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)


def reconstruct_sample(archive):
    conf = json.loads((archive / 'config.json').read_text())
    ops = list(csv.DictReader((archive / 'runner_operations.csv').open()))
    ack = [r for r in ops if str(r['success']).lower() in ('true', '1')]
    count = min(int(conf.get('verify_pixels', 1500)), len(ack))
    rng = random.Random(int(conf['seed']) + 10007)
    selected = rng.sample(ack, count)
    return [{'attempt': r['attempt'], 'x': r['x'], 'y': r['y'],
             'region': r['region'], 'expected': str(r['color']).upper(),
             'write_time_s': r['time_s'], 'via_node': r['via_node'],
             'phase': r['phase']} for r in selected]


def parse_logs(archive, out_dir):
    """Extract backfill/repair trace + the per-request quorum trace."""
    out_dir.mkdir(parents=True, exist_ok=True)
    backfills, repairs, gets, ack_rows, ignores = [], [], [], [], []
    seq = collections.Counter()
    for log in sorted(archive.glob('node*_boot*.log')):
        node, boot = boot_of(log.name)
        shutil.copy2(log, out_dir / log.name)
        for line in log.open(errors='replace'):
            seq[(node, boot)] += 1
            s = seq[(node, boot)]
            if '[backfill]' in line:
                m = BACKFILL_RE.search(line)
                if m:
                    backfills.append({'node': int(m.group(1)), 'boot': boot,
                                      'line_seq': s, 'region': int(m.group(2)),
                                      'success': m.group(3),
                                      'version': m.group(4) or '',
                                      'error': (m.group(5) or '').strip()})
            if '[replica-repair]' in line:
                m = REPAIR_RE.search(line)
                if m:
                    repairs.append({'kind': 'replica-repair', 'node': int(m.group(1)),
                                    'from': int(m.group(2)), 'region': int(m.group(3)),
                                    'version': int(m.group(4)), 'boot': boot,
                                    'line_seq': s})
            if '[anti-entropy]' in line:
                m = ANTI_RE.search(line)
                if m:
                    repairs.append({'kind': 'anti-entropy', 'node': int(m.group(1)),
                                    'to': int(m.group(2)), 'region': int(m.group(3)),
                                    'bytes': int(m.group(4)), 'boot': boot,
                                    'line_seq': s})
            if '[read-repair]' in line:
                m = READ_REPAIR_RE.search(line)
                if m:
                    repairs.append({'kind': 'read-repair', 'node': int(m.group(1)),
                                    'region': int(m.group(2)), 'target': int(m.group(3)),
                                    'boot': boot, 'line_seq': s})
            if '[quorum]' in line and ' op=GET ' in line:
                m = GET_RE.search(line)
                if m:
                    gets.append({'node': int(m.group(1)), 'request': int(m.group(2)),
                                 'region': int(m.group(3)), 'N': int(m.group(4)),
                                 'assigned': int(m.group(5)), 'R': int(m.group(6)),
                                 'targets': m.group(7), 'boot': boot, 'line_seq': s})
            if '[quorum-ack]' in line:
                m = ACK_RE.search(line)
                if m:
                    ack_rows.append({'node': int(m.group(1)), 'op': m.group(2),
                                     'request': int(m.group(3)), 'from': int(m.group(4)),
                                     'count': m.group(5) + '/' + m.group(6),
                                     'boot': boot, 'line_seq': s})
            if '[quorum-ignore]' in line and 'late-or-unknown' in line:
                m = IGNORE_RE.search(line)
                if m:
                    ignores.append({'node': int(m.group(1)),
                                    'request': int(m.group(2)),
                                    'sender': int(m.group(3)), 'boot': boot,
                                    'line_seq': s})
    return backfills, repairs, gets, ack_rows, ignores


def trace_pixels(archive):
    """End-to-end quorum_response_trace rows for the TRACED pixels."""
    rows = []
    for (x, y, reg, idx, attempt, t, via, color) in TRACED:
        # 1. The write.
        put = None
        for log in sorted(archive.glob('node*_boot*.log')):
            if boot_of(log.name)[0] != via:
                continue
            for line in log.open(errors='replace'):
                m = PUT_RE.search(line)
                if m and int(m.group(3)) == reg and int(m.group(4)) == idx:
                    put = {'node': int(m.group(1)), 'request': int(m.group(2)),
                           'targets': m.group(8)}
        rows.append({'pixel': f'({x},{y})', 'region': reg, 'stage': 'write',
                     'evidence': f'runner_operations attempt={attempt} t={t:.1f}s via={via}',
                     'detail': f'PUT request={put["request"] if put else "?"} '
                               f'targets={put["targets"] if put else "?"}'})
        # 2. Which replicas applied it.
        holders = []
        for log in sorted(archive.glob('node*_boot*.log')):
            for line in log.open(errors='replace'):
                m = REPLICA_PUT_RE.search(line)
                if m and int(m.group(4)) == reg and int(m.group(5)) == idx:
                    holders.append((boot_of(log.name), int(m.group(1)), m.group(6)))
        for (n, b), node, ver in sorted(holders):
            rows.append({'pixel': f'({x},{y})', 'region': reg, 'stage': 'apply',
                         'evidence': f'node{node}_boot{b}.log',
                         'detail': f'replica-put node={node} version={ver} applied=1'})
        # 3. Region N transitions.
        ev = list(csv.DictReader((archive / 'region_replication_events.csv').open()))
        for e in ev:
            if e['region'] == str(reg) and e['event_type'] == 'n_change':
                rows.append({'pixel': f'({x},{y})', 'region': reg, 'stage': 'n_change',
                             'evidence': 'region_replication_events.csv',
                             'detail': f"t={float(e['time_s']):.1f}s "
                                       f"{e['old_n']}->{e['new_n']} epoch={e['policy_epoch']}"})
        # 4. Final N2 pair from placement state.
        fields = (archive / 'node1.state').read_text().split()
        count = int(fields[2])
        members = [int(v) for v in fields[3:3 + count]]
        offset = 3 + count * (2 if fields[0].endswith('V2') else 1)
        owners = [int(v) for v in fields[offset:offset + 256]]
        o = owners[reg]
        pair = [members[(members.index(o) + j) % count] for j in range(2)]
        quad = [members[(members.index(o) + j) % count] for j in range(4)]
        rows.append({'pixel': f'({x},{y})', 'region': reg, 'stage': 'final_targets',
                     'evidence': 'node1.state',
                     'detail': f'N2 pair={pair} N4 set={quad} (holders '
                               f'{sorted({n for (_, n, _) in holders})})'})
        # 5. Verification-time GET for this region from the readers.
        for log in sorted(archive.glob('node1_boot*.log')):
            node, boot = boot_of(log.name)
            if node not in (1, 2, 3):
                continue
            for line in log.open(errors='replace'):
                m = GET_RE.search(line)
                if m and int(m.group(3)) == reg:
                    rows.append({'pixel': f'({x},{y})', 'region': reg,
                                 'stage': 'snapshot_get',
                                 'evidence': f'node{node}_boot{boot}.log',
                                 'detail': f"reader={node} request={m.group(2)} "
                                           f"N={m.group(4)} targets={m.group(7)}"})
    return rows


def local_inventory(archive):
    """Log-derived per-node holding evidence for the traced regions (a direct
    RAM inspection is not available in production builds; the C++ unit tests
    perform the equivalent direct-store check)."""
    rows = []
    for (x, y, reg, idx, *_rest) in TRACED:
        holders = collections.defaultdict(list)
        for log in sorted(archive.glob('node*_boot*.log')):
            (node, boot) = boot_of(log.name)
            for line in log.open(errors='replace'):
                if ('replica-put' in line and f'region={reg} index={idx} ' in line
                        and 'applied=1' in line):
                    holders[node].append(f'apply(boot{boot})')
                elif ('[backfill]' in line and f'region={reg} ' in line
                      and 'success=true version=' in line
                      and 'version=0' not in line):
                    holders[node].append('backfill(nonempty)')
                elif ('[replica-repair]' in line and f'region={reg} ' in line
                      and 'version=0' not in line):
                    holders[node].append('anti-entropy-received')
        for node in range(1, 11):
            rows.append({'pixel': f'({x},{y})', 'region': reg, 'node': node,
                         'holding_evidence': ';'.join(holders.get(node, [])) or 'none',
                         'note': 'log-derived; not a direct RAM inspection'})
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--archive', type=pathlib.Path, required=True)
    ap.add_argument('--replay', type=pathlib.Path, default=None)
    ap.add_argument('--out', type=pathlib.Path, required=True)
    args = ap.parse_args()
    if args.out.exists():
        ap.error('--out must be a new path')
    args.out.mkdir(parents=True)

    write_csv(args.out / 'sampled_pixels.csv', reconstruct_sample(args.archive),
              ['attempt', 'x', 'y', 'region', 'expected', 'write_time_s',
               'via_node', 'phase'])

    backfills, repairs, gets, acks, ignores = parse_logs(
        args.archive, args.out / 'node_logs')
    write_csv(args.out / 'backfill_repair_trace.csv',
              [{'kind': 'backfill', **b} for b in backfills] +
              [{'kind': r['kind'], **r} for r in repairs],
              ['kind', 'node', 'boot', 'line_seq', 'region', 'from', 'to',
               'target', 'version', 'bytes', 'success', 'error'])
    write_csv(args.out / 'quorum_response_trace.csv', trace_pixels(args.archive),
              ['pixel', 'region', 'stage', 'evidence', 'detail'])
    write_csv(args.out / 'local_replica_inventory.csv', local_inventory(args.archive),
              ['pixel', 'region', 'node', 'holding_evidence', 'note'])
    ev = list(csv.DictReader((args.archive / 'region_replication_events.csv').open()))
    write_csv(args.out / 'policy_transition_trace.csv', ev,
              ['time_s', 'event_type', 'region', 'old_n', 'new_n',
               'placement_gen', 'policy_epoch', 'reason'])

    # Verification + convergence from the controlled replay (all gateways).
    convergence, verification_rows = [], []
    replay_rounds = None
    if args.replay and (args.replay / 'recovery/snapshot_comparisons.csv').exists():
        rows = list(csv.DictReader(
            (args.replay / 'recovery/snapshot_comparisons.csv').open()))
        per_round = collections.defaultdict(
            lambda: {'comparisons': 0, 'mismatches': 0, 'absent': 0,
                     'wrong_color': 0, 'read_errors': 0})
        for r in rows:
            c = per_round[int(r['round'])]
            c['comparisons'] += 1
            if r['status'] == 'snapshot_error':
                c['read_errors'] += 1
            elif r['status'] != 'correct':
                c['mismatches'] += 1
                c['absent'] += int(r['status'] == 'absent')
                c['wrong_color'] += int(r['status'] == 'wrong_color')
            verification_rows.append(r)
        convergence = [{'round': k, **v} for k, v in sorted(per_round.items())]
        replay_rounds = sorted(per_round)
    write_csv(args.out / 'convergence_checkpoints.csv', convergence,
              ['round', 'comparisons', 'mismatches', 'absent', 'wrong_color',
               'read_errors'])
    if verification_rows:
        write_csv(args.out / 'verification_all_gateways.csv', verification_rows,
                  ['round', 'reader', 'x', 'y', 'region', 'expected', 'actual',
                   'status', 'error'])

    s = json.loads((args.archive / 'summary.json').read_text())
    summary = {
        'scenario': 'B_stress50_300s_N2_regression',
        'archive_sha256_note': 'see environment.json in strategy_b_case_01',
        'immediate_mismatches': s.get('verification_immediate', {}).get('missing'),
        'after_settle_mismatches': s.get('verification_after_settle', {}).get('missing'),
        'root_cause': 'confirmed: (D1) rejoin policy starvation - authority never '
                      're-broadcasts the current snapshot so restarted members run '
                      'on the fallback N with an empty store; (D2) scale-down '
                      'strands ACKed copies on replicas dropped from the N2 pair '
                      '(traced pixel (86,140) region 133: holders 1,10 vs final '
                      'pair 8,9); (D3) backfill at N=2 with exclude_self had one '
                      'source vs R=2 and could never complete',
        'fix': 'policy heartbeat re-broadcast; backfill superset pull '
               '(max-N preference set, required=min(R,sources)); reconsider '
               'data-less owned regions on policy change',
        'confirmed_data_loss': None,
        'replay': args.replay and str(args.replay) or None,
        'replay_rounds': replay_rounds,
        'replay_last_round_mismatches': (convergence[-1]['mismatches']
                                         if convergence else None),
    }
    (args.out / 'case_summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))
    print(f'wrote diagnostics to {args.out}')


if __name__ == '__main__':
    main()
