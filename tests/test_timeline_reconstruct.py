#!/usr/bin/env python3
"""Offline tests for the replication-N timeline reconstruction.

Builds synthetic per-incarnation event files, runs the runner's
merge_timeline(), and checks the documented invariants:
  - Strategy A yields a constant N=3 table (no policy events).
  - Strategy B initial snapshot + n_change rows reconstruct the exact table.
  - Duplicate snapshot acceptances deduplicate; conflicts are flagged.
  - Placement changes with unchanged N still change assignments.
  - Under-membership distinguishes desired vs assigned (R/W untouched).
  - n2+n3+n4 == 256 and SUM_node(assigned_regions) == total_assigned_copies.
  - Two incarnations of one node align on the single workload clock.
"""
import csv
import json
import pathlib
import sys
import tempfile
import types

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'experiments'))
from experiments.run_experiment import (  # noqa: E402
    merge_timeline, preference_for_region,
)


def write_event(dirpath, run_id, ns, etype, region, old_n, new_n, gen, fp,
                epoch, authority, reason):
    with open(dirpath / 'region_replication_events.csv', 'a', newline='') as f:
        w = csv.writer(f)
        if f.tell() == 0:
            w.writerow(['run_id', 'monotonic_ns', 'event_type', 'region',
                        'old_n', 'new_n', 'placement_gen', 'placement_fp',
                        'policy_epoch', 'authority_node', 'reason'])
        w.writerow([run_id, ns, etype, region, old_n, new_n, gen, fp, epoch,
                    authority, reason])


def write_snapshot(dirpath, ns, gen, fp, authority, members):
    with open(dirpath / 'placement_snapshots.jsonl', 'a') as f:
        f.write(json.dumps({'monotonic_ns': ns, 'placement_gen': gen,
                            'placement_fp': fp, 'authority_node': authority,
                            'member_ids': list(members)}) + '\n')


def write_state(out, gen, members, owners):
    fields = ['P2PLACE_BALANCED_V2', str(gen), str(len(members))]
    fields += [str(m) for m in members]
    fields += ['1'] * len(members)  # admission orders
    fields += [str(o) for o in owners]
    fields.append('CHECKSUM')
    fields.append('0')
    (out / 'node1.state').write_text(' '.join(fields) + '\n')


def make_args(out, seconds=60):
    return types.SimpleNamespace(
        strategy='B', seed=42, seconds=seconds, nodes=10, out=out,
        replication_timeline=True, no_replication_timeline=False,
        replication_sample_interval=1.0)


def base_summary(anchor):
    return {'strategy': 'B', 'seed': 42,
            'time_base': {'workload_start_monotonic_ns': anchor}}


def test_strategy_a_constant():
    with tempfile.TemporaryDirectory() as td:
        out = pathlib.Path(td)
        anchor = 1_000_000_000_000
        (out / 'metrics/node1_boot1').mkdir(parents=True)
        write_snapshot(out / 'metrics/node1_boot1', anchor - 10_000_000_000,
                       1, 111, 1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        write_snapshot(out / 'metrics/node1_boot1', anchor + 10_000_000_000,
                       2, 222, 1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        owners = [(i % 10) + 1 for i in range(256)]
        write_state(out, 2, tuple(range(1, 11)), tuple(owners))
        summary = base_summary(anchor)
        summary['strategy'] = 'A'
        args = make_args(out)
        args.strategy = 'A'
        merge_timeline(args, summary)
        assert summary['replication_timeline']['complete']
        with open(out / 'replication_summary.csv') as f:
            rows = list(csv.DictReader(f))
        assert all(r['n3_regions'] == '256' for r in rows), \
            'Strategy A must keep N=3 for all regions'
        with open(out / 'region_replication_events.csv') as f:
            ev = list(csv.DictReader(f))
        assert all(r['event_type'] == 'placement_change' for r in ev), \
            'Strategy A must emit only placement events (no policy events)'
    print('PASS: Strategy A timeline is constant N=3 with placement events only')


def test_strategy_b_reconstruction_and_dedup():
    with tempfile.TemporaryDirectory() as td:
        out = pathlib.Path(td)
        anchor = 1_000_000_000_000
        boot = out / 'metrics/node1_boot1'
        boot.mkdir(parents=True)
        # Initial snapshot: all N2 at gen 10, epoch 1 (t = -5s relative).
        write_event(boot, 'B_seed42', anchor - 5_000_000_000, 'initial', 0, '',
                    '2', 10, 1111, 1, 1, 'initial_snapshot')
        for r in range(1, 256):
            write_event(boot, 'B_seed42', anchor - 5_000_000_000, 'initial', r,
                        '', '2', 10, 1111, 1, 1, 'initial_snapshot')
        # Region 42: 2->3 at t=10 (epoch 2), duplicated on another boot.
        for bootname in ('node1_boot1', 'node2_boot1'):
            b = out / 'metrics' / bootname
            b.mkdir(parents=True, exist_ok=True)
            write_event(b, 'B_seed42', anchor + 10_000_000_000, 'n_change', 42,
                        '2', '3', 10, 1111, 2, 1, 'policy_apply')
        # Region 42: 3->4 at t=20 (epoch 3).
        write_event(boot, 'B_seed42', anchor + 20_000_000_000, 'n_change', 42,
                    '3', '4', 10, 1111, 3, 1, 'policy_apply')
        write_snapshot(boot, anchor - 5_000_000_000, 10, 1111, 1,
                       list(range(1, 11)))
        owners = [(i % 10) + 1 for i in range(256)]
        write_state(out, 10, tuple(range(1, 11)), tuple(owners))
        summary = base_summary(anchor)
        merge_timeline(make_args(out), summary)
        tl = summary['replication_timeline']
        assert tl['complete']
        assert tl['policy_changes'] == 2, \
            f'duplicate acceptance must not double-count ({tl["policy_changes"]})'
        with open(out / 'replication_summary.csv') as f:
            rows = list(csv.DictReader(f))
        assert rows[10]['n2_regions'] == '255' and rows[10]['n3_regions'] == '1', \
            '2->3 change must be visible at t=10'
        assert rows[20]['n3_regions'] == '0' and rows[20]['n4_regions'] == '1', \
            '3->4 change must be visible at t=20'
    print('PASS: Strategy B initial+change events reconstruct the exact table; '
          'duplicates dedupe')


def test_conflict_flagged_and_invariants():
    with tempfile.TemporaryDirectory() as td:
        out = pathlib.Path(td)
        anchor = 1_000_000_000_000
        boot = out / 'metrics/node1_boot1'
        boot.mkdir(parents=True)
        write_event(boot, 'B_seed42', anchor, 'initial', 0, '', '2', 10, 1, 1,
                    1, 'initial_snapshot')
        for r in range(1, 256):
            write_event(boot, 'B_seed42', anchor, 'initial', r, '', '2', 10, 1,
                        1, 1, 'initial_snapshot')
        # Conflicting event: claims old_n=3 while the table has 2.
        write_event(boot, 'B_seed42', anchor + 5_000_000_000, 'n_change', 7,
                    '3', '4', 10, 1, 2, 1, 'policy_apply')
        write_snapshot(boot, anchor, 10, 1, 1, list(range(1, 11)))
        owners = [(i % 10) + 1 for i in range(256)]
        write_state(out, 10, tuple(range(1, 11)), tuple(owners))
        summary = base_summary(anchor)
        merge_timeline(make_args(out), summary)
        tl = summary['replication_timeline']
        assert tl['conflicts'] == 1, 'conflicting old_n must be flagged'
        with open(out / 'replication_summary.csv') as f:
            rows = list(csv.DictReader(f))
        r = rows[5]
        assert int(r['n2_regions']) + int(r['n3_regions']) + \
            int(r['n4_regions']) == 256
        with open(out / 'node_replication_timeline.csv') as f:
            nrows = [x for x in csv.DictReader(f) if float(x['time_s']) == 5.0]
        assert len(nrows) == 10, 'all ten nodes must have rows (incl. inactive)'
        assigned_sum = sum(int(x['assigned_regions']) for x in nrows)
        assert assigned_sum == int(r['total_assigned_copies']), \
            'SUM_node(assigned_regions) must equal total_assigned_copies'
    print('PASS: conflicts flagged; n2+n3+n4=256; per-node assigned sums match '
          'the cluster total')


def test_under_membership_desired_vs_assigned():
    with tempfile.TemporaryDirectory() as td:
        out = pathlib.Path(td)
        anchor = 1_000_000_000_000
        boot = out / 'metrics/node1_boot1'
        boot.mkdir(parents=True)
        # All N4, but only 3 committed members -> assigned 3 everywhere.
        write_event(boot, 'B_seed42', anchor, 'initial', 0, '', '4', 5, 1, 1,
                    1, 'initial_snapshot')
        for r in range(1, 256):
            write_event(boot, 'B_seed42', anchor, 'initial', r, '', '4', 5, 1,
                        1, 1, 'initial_snapshot')
        write_snapshot(boot, anchor, 5, 1, 1, [1, 2, 3])
        owners = [(i % 3) + 1 for i in range(256)]
        write_state(out, 5, (1, 2, 3), tuple(owners))
        summary = base_summary(anchor)
        merge_timeline(make_args(out), summary)
        with open(out / 'replication_summary.csv') as f:
            row = next(csv.DictReader(f))
        assert int(row['total_desired_copies']) == 256 * 4
        assert int(row['total_assigned_copies']) == 256 * 3
        assert int(row['underassigned_regions']) == 256
        with open(out / 'node_replication_timeline.csv') as f:
            nrows = [x for x in csv.DictReader(f)
                     if float(x['time_s']) == 0.0 and x['node_id'] == '4']
        assert nrows and int(nrows[0]['assigned_regions']) == 0, \
            'node 4 is not a member: zero assignments'
    print('PASS: under-membership keeps desired_n distinct from assigned_n')


def test_two_incarnations_aligned():
    with tempfile.TemporaryDirectory() as td:
        out = pathlib.Path(td)
        anchor = 1_000_000_000_000
        b1 = out / 'metrics/node8_boot1'
        b2 = out / 'metrics/node8_boot2'
        b1.mkdir(parents=True)
        b2.mkdir(parents=True)
        write_event(b1, 'B_seed42', anchor - 5_000_000_000, 'initial', 0, '',
                    '2', 10, 1, 1, 1, 'initial_snapshot')
        for r in range(1, 256):
            write_event(b1, 'B_seed42', anchor - 5_000_000_000, 'initial', r,
                        '', '2', 10, 1, 1, 1, 'initial_snapshot')
        # Boot 2 (new authority) emits a rebase n_change at t=40 with its own
        # monotonic base; alignment must place it at +40s on the run clock.
        write_event(b2, 'B_seed42', anchor + 40_000_000_000, 'n_change', 9,
                    '2', '3', 10, 1, 2, 1, 'policy_apply')
        write_snapshot(b1, anchor - 5_000_000_000, 10, 1, 1, list(range(1, 11)))
        owners = [(i % 10) + 1 for i in range(256)]
        write_state(out, 10, tuple(range(1, 11)), tuple(owners))
        summary = base_summary(anchor)
        merge_timeline(make_args(out), summary)
        with open(out / 'replication_summary.csv') as f:
            rows = list(csv.DictReader(f))
        assert rows[39]['n3_regions'] == '0' and rows[40]['n3_regions'] == '1', \
            'second incarnation event must land at t=40, not its local clock'
    print('PASS: two incarnations of one node align on the workload clock')


def test_preference_matches_cpp():
    members = (1, 2, 3, 4, 5)
    owners = tuple((i % 5) + 1 for i in range(256))
    # owners[0] == 1 -> preference starts at 1: [1,2,3] for N=3.
    assert preference_for_region(members, owners, 0, 3) == [1, 2, 3]
    # owners[3] == 4 -> [4,5,1] for N=3 (wraps).
    assert preference_for_region(members, owners, 3, 3) == [4, 5, 1]
    # N larger than members caps at len(members).
    assert preference_for_region(members, owners, 0, 99) == [1, 2, 3, 4, 5]
    print('PASS: Python preference walk matches BalancedPlacement::preference')


def main():
    test_strategy_a_constant()
    test_strategy_b_reconstruction_and_dedup()
    test_conflict_flagged_and_invariants()
    test_under_membership_desired_vs_assigned()
    test_two_incarnations_aligned()
    test_preference_matches_cpp()
    print('ALL PASS: replication-N timeline reconstruction')


if __name__ == '__main__':
    main()
