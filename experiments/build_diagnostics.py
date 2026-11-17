#!/usr/bin/env python3
"""Assemble the machine-readable diagnostics bundle for the Strategy B
backfill false-complete case.

Usage:
  python3 experiments/build_diagnostics.py --result <isolated-run-dir> \
      --out experiments/diagnostics/strategy_b_case_01 \
      [--stress50 <stress50-run-dir>] [--label <name>]

Produces environment.json, case_summary.json, mismatch_all.csv,
per_region_summary.csv, per_reader_summary.csv, backfill_trace.csv,
repair_trace.csv, policy_epoch_trace.csv, convergence_rounds.csv, node_logs/.

All inputs are read-only; the out directory must not already exist.
"""
from __future__ import annotations
import argparse
import collections
import csv
import hashlib
import json
import pathlib
import platform
import re
import shutil
import subprocess
import sys

BACKFILL_RE = re.compile(
    r'\[backfill\] node=(\d+) region=(\d+) success=(true|false)'
    r'(?: version=(\d+))?(?: error=([^ ]+(?: [^ ]+)*))?'
    r'(?: classification=(\w+))?(?: attempts=(\d+))?')
REPAIR_RE = re.compile(
    r'\[replica-repair\] node=(\d+) from=(\d+) region=(\d+) version=(\d+)')
ANTI_ENTROPY_RE = re.compile(
    r'\[anti-entropy\] node=(\d+) region=(\d+) to=(\d+) bytes=(\d+) success=(\w+)')
POLICY_APPLY_RE = re.compile(
    r'\[policy-apply\] node=(\d+) gen=(\d+) epoch=(\d+) strategy=(\w)')
POLICY_BROADCAST_RE = re.compile(
    r'\[policy-broadcast\]')
READ_REPAIR_RE = re.compile(
    r'\[read-repair\] node=(\d+) region=(\d+) target=(\d+)')
QUORUM_GET_RE = re.compile(
    r'\[quorum\] node=(\d+) op=GET request=(\d+) region=(\d+) N=(\d+) assigned=(\d+) R=(\d+)')


def region(x, y):
    return (y // 16) * 16 + x // 16


def git_head(repo):
    try:
        out = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=repo,
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip()
    except Exception:
        return 'unknown'


def sha(p):
    try:
        return hashlib.sha256(p.read_bytes()).hexdigest()
    except Exception:
        return 'missing'


def boot_of(name):
    m = re.search(r'node(\d+)_boot(\d+)\.log', name)
    return (int(m.group(1)), int(m.group(2))) if m else (0, 0)


def parse_node_logs(result, out_logs):
    """Return (backfills, repairs, policy, get_events) parsed from node logs;
    copy logs into out_logs."""
    out_logs.mkdir(parents=True, exist_ok=True)
    backfills, repairs, policy, gets = [], [], [], []
    seq = collections.Counter()
    for log in sorted(result.glob('node*_boot*.log')):
        node, boot = boot_of(log.name)
        seq[(node, boot)] = 0
        shutil.copy2(log, out_logs / log.name)
        for line in log.open(errors='replace'):
            seq[(node, boot)] += 1
            s = seq[(node, boot)]
            if '[backfill]' in line:
                m = BACKFILL_RE.search(line)
                if not m:
                    continue
                backfills.append({
                    'node': int(m.group(1)), 'boot': boot,
                    'line_seq': s, 'region': int(m.group(2)),
                    'success': m.group(3), 'version': m.group(4) or '',
                    'error': (m.group(5) or '').strip(),
                    'classification': m.group(6) or '',
                    'attempts': m.group(7) or ''})
            if '[replica-repair]' in line:
                m = REPAIR_RE.search(line)
                if m:
                    repairs.append({'kind': 'replica-repair', 'node': int(m.group(1)),
                                    'from': int(m.group(2)), 'region': int(m.group(3)),
                                    'version': int(m.group(4)), 'boot': boot, 'line_seq': s})
            if '[anti-entropy]' in line:
                m = ANTI_ENTROPY_RE.search(line)
                if m:
                    repairs.append({'kind': 'anti-entropy', 'node': int(m.group(1)),
                                    'to': int(m.group(2)), 'region': int(m.group(3)),
                                    'bytes': int(m.group(4)), 'success': m.group(5),
                                    'boot': boot, 'line_seq': s})
            if '[read-repair]' in line:
                m = READ_REPAIR_RE.search(line)
                if m:
                    repairs.append({'kind': 'read-repair', 'node': int(m.group(1)),
                                    'region': int(m.group(2)), 'target': int(m.group(3)),
                                    'boot': boot, 'line_seq': s})
            if '[policy-apply]' in line:
                m = POLICY_APPLY_RE.search(line)
                if m:
                    policy.append({'kind': 'apply', 'node': int(m.group(1)),
                                   'gen': int(m.group(2)), 'epoch': int(m.group(3)),
                                   'strategy': m.group(4), 'boot': boot, 'line_seq': s})
            if '[policy-broadcast]' in line:
                policy.append({'kind': 'broadcast', 'boot': boot, 'line_seq': s})
            if '[quorum]' in line and ' op=GET ' in line:
                m = QUORUM_GET_RE.search(line)
                if m:
                    gets.append({'node': int(m.group(1)), 'request': int(m.group(2)),
                                 'region': int(m.group(3)), 'N': int(m.group(4)),
                                 'assigned': int(m.group(5)), 'R': int(m.group(6)),
                                 'boot': boot, 'line_seq': s})
    return backfills, repairs, policy, gets


def mismatch_tables(result):
    """Derive mismatch_all.csv + per-region/per-reader summaries + convergence
    from recovery/snapshot_comparisons.csv (or baseline/ if recovery absent)."""
    rows, path = [], None
    for cand in ('recovery/snapshot_comparisons.csv', 'baseline/snapshot_comparisons.csv'):
        p = result / cand
        if p.exists():
            path = p
            break
    if not path:
        return None
    with path.open(newline='') as f:
        for r in csv.DictReader(f):
            r['mismatch'] = '0' if r.get('status') == 'correct' else '1'
            r['round'] = int(r.get('round', 0))
            r['reader'] = int(r.get('reader', 0)) if r.get('reader', '') else ''
            rows.append(r)
    per_region = collections.defaultdict(
        lambda: {'comparisons': 0, 'mismatches': 0, 'absent': 0, 'wrong_color': 0,
                 'rounds_with_mismatches': set()})
    per_reader = collections.defaultdict(
        lambda: {'comparisons': 0, 'mismatches': 0, 'absent': 0, 'wrong_color': 0})
    per_round = collections.defaultdict(lambda: {'comparisons': 0, 'mismatches': 0,
                                                 'absent': 0, 'wrong_color': 0,
                                                 'read_errors': 0})
    pixel_first_clean = {}
    for r in rows:
        reg = r.get('region', '') or ''
        if reg != '':
            pr = per_region[reg]
            pr['comparisons'] += 1
            if r['status'] != 'correct':
                pr['mismatches'] += 1
                pr['rounds_with_mismatches'].add(r['round'])
            if r['status'] == 'absent':
                pr['absent'] += 1
            if r['status'] == 'wrong_color':
                pr['wrong_color'] += 1
        if r['reader'] != '':
            rd = per_reader[r['reader']]
            rd['comparisons'] += 1
            if r['status'] != 'correct':
                rd['mismatches'] += 1
            if r['status'] == 'absent':
                rd['absent'] += 1
            if r['status'] == 'wrong_color':
                rd['wrong_color'] += 1
        prd = per_round[r['round']]
        prd['comparisons'] += 1
        if r['status'] == 'snapshot_error':
            prd['read_errors'] += 1
        elif r['status'] != 'correct':
            prd['mismatches'] += 1
            if r['status'] == 'absent':
                prd['absent'] += 1
            if r['status'] == 'wrong_color':
                prd['wrong_color'] += 1
        key = (r['x'], r['y'])
        if r['status'] == 'correct':
            pixel_first_clean.setdefault(key, r['round'])
    conv = []
    for key, first in sorted(pixel_first_clean.items()):
        conv.append({'x': key[0], 'y': key[1], 'region': region(int(key[0]), int(key[1])),
                     'first_all_readers_correct_round': first})
    return {
        'rows': rows,
        'per_region': per_region, 'per_reader': per_reader, 'per_round': per_round,
        'convergence': conv, 'source': str(path)}


def write_csv(path, rows, cols):
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--result', type=pathlib.Path, required=True)
    ap.add_argument('--out', type=pathlib.Path, required=True)
    ap.add_argument('--stress50', type=pathlib.Path, default=None)
    ap.add_argument('--label', default='B_isolated_T4_convergence')
    args = ap.parse_args()
    if args.out.exists():
        ap.error('--out must be a new path')
    args.out.mkdir(parents=True)
    repo = args.result.resolve().parents[2]  # experiments/results/<run> -> repo
    backfills, repairs, policy, gets = parse_node_logs(args.result, args.out / 'node_logs')
    tables = mismatch_tables(args.result)
    write_csv(args.out / 'backfill_trace.csv', backfills,
              ['node', 'boot', 'line_seq', 'region', 'success', 'version',
               'error', 'classification', 'attempts'])
    write_csv(args.out / 'repair_trace.csv', repairs,
              ['kind', 'node', 'from', 'to', 'target', 'region', 'version',
               'bytes', 'success', 'boot', 'line_seq'])
    write_csv(args.out / 'policy_epoch_trace.csv', policy,
              ['kind', 'node', 'gen', 'epoch', 'strategy', 'boot', 'line_seq'])
    if tables:
        write_csv(args.out / 'mismatch_all.csv', tables['rows'],
                  ['round', 'reader', 'x', 'y', 'region', 'expected', 'actual',
                   'status', 'error', 'mismatch'])
        pr_region = [{'region': k, 'comparisons': v['comparisons'],
                      'mismatches': v['mismatches'], 'absent': v['absent'],
                      'wrong_color': v['wrong_color'],
                      'rounds_with_mismatches': sorted(v['rounds_with_mismatches'])}
                     for k, v in sorted(tables['per_region'].items(), key=lambda kv: int(kv[0]))]
        write_csv(args.out / 'per_region_summary.csv', pr_region,
                  ['region', 'comparisons', 'mismatches', 'absent', 'wrong_color',
                   'rounds_with_mismatches'])
        pr_reader = [{'reader': k, **{kk: vv for kk, vv in v.items()}}
                     for k, v in sorted(tables['per_reader'].items(), key=lambda kv: str(kv[0]))]
        write_csv(args.out / 'per_reader_summary.csv', pr_reader,
                  ['reader', 'comparisons', 'mismatches', 'absent', 'wrong_color'])
        pr_round = [{'round': k, **v} for k, v in sorted(tables['per_round'].items())]
        write_csv(args.out / 'convergence_rounds.csv', pr_round,
                  ['round', 'comparisons', 'mismatches', 'absent', 'wrong_color',
                   'read_errors'])
    else:
        sys.stderr.write('WARNING: no snapshot_comparisons.csv found; '
                         'mismatch tables skipped\n')
    # case_summary.json — honest verdicts; null means unproven, not false.
    probe_summary = {}
    for cand in ('recovery/probe_summary.json', 'baseline/probe_summary.json'):
        p = args.result / cand
        if p.exists():
            probe_summary = json.loads(p.read_text())
            break
    acked = []
    apix = args.result / 'acknowledged_pixels.json'
    if apix.exists():
        acked = json.loads(apix.read_text())
    stress_ref = None
    if args.stress50 and (args.stress50 / 'summary.json').exists():
        s = json.loads((args.stress50 / 'summary.json').read_text())
        stress_ref = {
            'immediate_mismatches': s.get('verification_immediate', {}).get('missing'),
            'after_settle_mismatches': s.get('verification_after_settle', {}).get('missing'),
            'sample': s.get('verification_after_settle', {}).get('sampled_pixels'),
            'note': 'historical Stress50 checkpoint values (context, not a re-run)'}
    summary = {
        'scenario': args.label,
        'result_dir': str(args.result),
        'acknowledged_pixels': len(acked),
        'probe': probe_summary,
        'per_round_mismatches': [{'round': k, **v} for k, v in
                                 sorted((tables or {}).get('per_round', {}).items())],
        'backfill_events': len(backfills),
        'backfill_classifications': dict(collections.Counter(
            b['classification'] for b in backfills)),
        'empty_version0_backfills': sum(1 for b in backfills if b['version'] == '0'),
        'repair_events': len(repairs),
        'policy_events': len(policy),
        'stress50_reference': stress_ref,
        'root_cause': ('confirmed: backfill read quorum completed on the first R '
                       'responses (including the backfilling node\'s own empty reply), '
                       'merged only those, discarded the populated late response, and '
                       'marked the empty merge success=true version=0 without retry; '
                       'gateway snapshot reads reproduced the same first-R race'),
        'fix': ('backfill uses complete_all+exclude_self GET (merge every reachable '
                'source); classification: complete / complete_partial / empty_valid '
                '(all sources agree empty, accepted once) / retry up to 4 bounded '
                'attempts then empty_partial_accepted; stale store epochs drop '
                'in-flight callbacks'),
        'confirmed_data_loss': None,
        'all_reader_converged': bool(tables and
                                     tables['per_round'].get(
                                         max(tables['per_round']), {}).get('mismatches', 1) == 0
                                     and not probe_summary.get('last_round_mismatches', 1)),
    }
    (args.out / 'case_summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    (args.out / 'environment.json').write_text(json.dumps({
        'git_head': git_head(repo),
        'binary_sha256': sha(args.result.parent.parent / 'build/bin/p2place-peer')
        if (args.result.parent.parent / 'build/bin/p2place-peer').exists() else 'missing',
        'runner_sha256': sha(args.result.parent.parent / 'experiments/run_experiment.py'),
        'deep_test_sha256': sha(args.result.parent.parent /
                                'tests/test_replication_convergence_deep.py'),
        'python': sys.version.split()[0],
        'platform': platform.platform(),
        'generated_by': 'experiments/build_diagnostics.py',
        'note': 'per-node log timestamps are boot-relative line sequences; '
                'monotonic_ns lives in metrics/*/replica_backfill_events.csv per node',
    }, indent=2) + '\n')
    print(json.dumps(summary, indent=2))
    print(f'wrote diagnostics to {args.out}')


if __name__ == '__main__':
    main()
