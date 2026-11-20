#!/usr/bin/env python3
"""Dashboard parity export for P2Place targeted B/C runner.

Reuses the exact timeline-reconstruction logic/schema in run_experiment_A_vs_B.py,
while deriving grounded inputs from the targeted runner's own file layout.
No guessed process/health measurements: UNKNOWN is preserved.
"""
from __future__ import annotations
import csv
import json
import pathlib
import shutil
import time
from types import SimpleNamespace

def parse_state_full(path):
    """(generation, members, owners) from a placement.state file."""
    try:
        fields = path.read_text().split()
        if fields[0] not in ('P2PLACE_BALANCED_V1', 'P2PLACE_BALANCED_V2'):
            return None
        generation, count = int(fields[1]), int(fields[2])
        members = tuple(map(int, fields[3:3 + count]))
        offset = 3 + count * (2 if fields[0].endswith('V2') else 1)
        owners = tuple(map(int, fields[offset:offset + 256]))
        if len(owners) != 256:
            return None
        return generation, members, owners
    except (OSError, ValueError, IndexError):
        return None


def preference_for_region(members, owners, region, n):
    """Deterministic replica preference identical to
    BalancedPlacement::preference: walk the sorted member list starting at
    the region owner, taking up to n members."""
    if not members or n == 0:
        return []
    idx = members.index(owners[region])
    return [members[(idx + j) % len(members)] for j in range(min(n, len(members)))]


class TimelineRecorder:
    """Runner ground truth: process lifetime events + workload clock anchor.

    node_process_events.csv uses time_s relative to the workload start (rows
    before the anchor get negative values, which is deliberate and
    documented)."""

    def __init__(self, out, enabled):
        self.out = out
        self.enabled = enabled
        self.anchor_ns = None
        self.file = None
        self.events = []
        if enabled:
            self.file = (out / 'node_process_events.csv').open('w', newline='')
            self.file.write('time_s,node_id,boot,action,pid\n')

    def set_anchor(self, monotonic_ns):
        self.anchor_ns = monotonic_ns

    def record(self, node_id, boot, action, pid):
        if not self.enabled:
            return
        t = (time.monotonic_ns() - self.anchor_ns) / 1e9 if self.anchor_ns else 0.0
        self.events.append((t, node_id, boot, action, pid))
        self.file.write(f'{t:.3f},{node_id},{boot},{action},{pid}\n')
        self.file.flush()

    def close(self):
        if self.file:
            self.file.close()


def _timeline_events(metrics_root):
    """Yield (time_s, row) for every region_replication_events.csv row."""
    for p in sorted(metrics_root.glob('node*_boot*/region_replication_events.csv')):
        try:
            with open(p, newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield float(row['monotonic_ns']), row
        except OSError:
            continue


def _timeline_snapshots(metrics_root):
    for p in sorted(metrics_root.glob('node*_boot*/placement_snapshots.jsonl')):
        try:
            for line in p.read_text().splitlines():
                obj = json.loads(line)
                yield obj
        except OSError:
            continue


def merge_timeline(args, summary):
    """Post-run reconstruction: consolidate per-incarnation event files into
    the four canonical outputs. Runs AFTER the workload and verification so
    it can never change the experiment itself."""
    tl = {'enabled': bool(args.replication_timeline and not args.no_replication_timeline),
          'complete': False, 'sample_interval_s': args.replication_sample_interval,
          'initial_policy_time_s': None, 'policy_changes': 0,
          'placement_changes': 0, 'authority_changes': 0,
          'conflicts': 0, 'missing_intervals_s': 0.0, 'metrics_dropped': 0,
          'outputs': ['region_replication_events.csv', 'placement_snapshots.jsonl',
                      'node_replication_timeline.csv', 'replication_summary.csv']}
    summary['replication_timeline'] = tl
    if not tl['enabled']:
        return
    anchor = (summary.get('time_base') or {}).get('workload_start_monotonic_ns')
    if not anchor:
        print('[timeline] WARNING: no workload time_base (run failed during '
              'startup?); timeline skipped', flush=True)
        return
    out = args.out

    # ---- gather + align + dedupe raw events ------------------------------
    raw = []
    dropped = 0
    for ns, row in _timeline_events(out / 'metrics'):
        # A SIGKILL/SIGTERM between row writes can truncate the final line:
        # skip malformed rows with a warning instead of guessing data.
        if (row.get('placement_gen') is None or row.get('policy_epoch') is None
                or row.get('event_type') is None or row.get('new_n') is None):
            dropped += 1
            continue
        raw.append({'time_s': (ns - anchor) / 1e9, **row})
    tl['metrics_dropped'] = dropped
    if dropped:
        print(f'[timeline] WARNING: dropped {dropped} malformed/truncated rows',
              flush=True)
    if not raw:
        # Strategy A emits no policy events by design: the reconstruction
        # below uses the documented constant N=3 fallback table instead of
        # inventing rows.
        print('[timeline] NOTE: no region_replication_events found; using '
              'constant N=3 baseline (Strategy A)', flush=True)
    raw.sort(key=lambda r: (r['time_s'], r['placement_gen'], r['policy_epoch']))
    seen = set()
    events = []
    for r in raw:
        key = (r['event_type'], r['region'], r['new_n'], r['placement_gen'],
               r['placement_fp'], r['policy_epoch'])
        if key in seen:
            continue  # duplicate snapshot acceptance on another peer/boot
        seen.add(key)
        events.append(r)
    tl['policy_changes'] = sum(1 for r in events if r['event_type'] == 'n_change')
    tl['placement_changes'] = sum(1 for r in events if r['event_type'] == 'placement_change')
    authorities = set()
    for r in events:
        if r.get('authority_node'):
            authorities.add(int(r['authority_node']))
    tl['authority_changes'] = len(authorities)

    snapshots = []
    for obj in _timeline_snapshots(out / 'metrics'):
        snapshots.append({'time_s': (obj['monotonic_ns'] - anchor) / 1e9, **obj})
    snapshots.sort(key=lambda s: s['time_s'])
    if not snapshots:
        print('[timeline] WARNING: no placement_snapshots.jsonl found', flush=True)
        return

    # ---- canonical state reconstruction ----------------------------------
    target = [3] * 256  # documented fallback (Strategy A constant baseline)
    tl['initial_policy_time_s'] = min(
        (r['time_s'] for r in events if r['event_type'] == 'initial'),
        default=0.0)
    conflicts = 0
    event_ptr = 0

    # Committed membership timeline from placement snapshots.
    member_events = []
    for s in snapshots:
        member_events.append((s['time_s'], tuple(sorted(s['member_ids'])),
                              s['placement_gen'], s['placement_fp'],
                              s['authority_node']))
    # Owners from the freshest state file (fixed partition table).
    best = None
    for n in range(1, args.nodes + 1):
        st = parse_state_full(out / f'node{n}.state')
        if st and (best is None or st[0] > best[0]):
            best = st
    if best:
        owners = best[2]
    else:
        print('[timeline] WARNING: cannot read owners table; assignments '
              'default to empty', flush=True)

    # ---- process liveness + health ---------------------------------------
    procs = []  # (time_s, node, action)
    for t, node, boot, action, pid in recorder_events(args):
        procs.append((t, node, action))
    procs.sort()

    health = {}  # node -> list of (time_s, state) aligned to the workload clock
    boot_anchor = {}
    for t, node, boot, action, pid in recorder_events(args):
        if action == 'launch':
            boot_anchor[(node, boot)] = t
    for p in sorted((out / 'metrics').glob('node*_boot*/peer_health_node*.csv')):
        try:
            parts = p.parent.name.split('_boot')
            node = int(parts[0].replace('node', ''))
            boot = int(parts[1])
            anchor_t = boot_anchor.get((node, boot))
            if anchor_t is None:
                continue
            with open(p, newline='') as f:
                for row in csv.DictReader(f):
                    health.setdefault(int(row['node_id']), []).append(
                        (anchor_t + float(row['time_s']), row['state']))
        except (OSError, ValueError):
            continue

    def node_running(node, t):
        state = False
        for pt, pn, action in procs:
            if pn != node or pt > t:
                continue
            state = action == 'launch'
        return state

    def node_health(node, t):
        rows = sorted(health.get(node, []))
        best = None
        for rt, state in rows:
            if rt > t:
                break
            best = state
        return best if best else 'UNKNOWN'

    def node_membership(node, members):
        if not members:
            return 'unknown'
        return 'member' if node in members else 'removed'

    # ---- 1 Hz samples -----------------------------------------------------
    member_ptr = 0
    with (out / 'region_replication_events.csv').open('w', newline='') as ef:
        w = csv.writer(ef)
        w.writerow(['run_id', 'time_s', 'event_type', 'region', 'old_n', 'new_n',
                    'placement_gen', 'placement_fp', 'policy_epoch',
                    'authority_node', 'reason'])
        for r in events:
            w.writerow([summary['strategy'] + '_seed' + str(args.seed), r['time_s'],
                        r['event_type'], r['region'], r.get('old_n', ''),
                        r.get('new_n', ''), r['placement_gen'], r['placement_fp'],
                        r['policy_epoch'], r.get('authority_node', ''), r['reason']])
    with (out / 'placement_snapshots.jsonl').open('w') as sf:
        for s in snapshots:
            sf.write(json.dumps(s) + '\n')

    interval = args.replication_sample_interval
    nt = int(args.seconds / interval) + 1
    with (out / 'node_replication_timeline.csv').open('w', newline='') as nf, \
         (out / 'replication_summary.csv').open('w', newline='') as cf:
        nw = csv.writer(nf)
        nw.writerow(['run_id', 'time_s', 'node_id', 'process_running',
                     'membership_state', 'health', 'placement_gen',
                     'placement_fp', 'policy_epoch', 'assigned_regions',
                     'n2_regions', 'n3_regions', 'n4_regions', 'avg_target_n',
                     'desired_region_copies', 'connected_assigned_regions'])
        cw = csv.writer(cf)
        cw.writerow(['run_id', 'time_s', 'placement_gen', 'placement_fp',
                     'policy_epoch', 'n2_regions', 'n3_regions', 'n4_regions',
                     'avg_target_n', 'total_desired_copies',
                     'total_assigned_copies', 'underassigned_regions',
                     'processes_running', 'members_committed'])
        run = f'{summary["strategy"]}_seed{args.seed}'
        for i in range(nt):
            t = i * interval
            # Progressive reconstruction: apply policy events as t advances.
            while event_ptr < len(events) and events[event_ptr]['time_s'] <= t:
                r = events[event_ptr]
                if r['event_type'] in ('initial', 'n_change'):
                    region = int(r['region'])
                    new_n = int(r['new_n'])
                    if r['event_type'] == 'n_change' and \
                            target[region] != int(r['old_n']):
                        conflicts += 1  # conflicting old_n; newest wins, kept
                    target[region] = new_n
                event_ptr += 1
            tl['conflicts'] = conflicts
            while member_ptr + 1 < len(member_events) and member_events[member_ptr + 1][0] <= t:
                member_ptr += 1
            members, gen, fp, authority = member_events[member_ptr][1:5]
            assigned = {n: 0 for n in range(1, args.nodes + 1)}
            n2 = n3 = n4 = other = 0
            desired = assigned_total = 0
            per_node_buckets = {n: [0, 0, 0] for n in range(1, args.nodes + 1)}
            per_node_n_sum = {n: 0 for n in range(1, args.nodes + 1)}
            for region in range(256):
                n_r = min(max(target[region], 1), 256)
                if n_r == 2:
                    n2 += 1
                elif n_r == 3:
                    n3 += 1
                elif n_r == 4:
                    n4 += 1
                else:
                    other += 1
                desired += n_r
                assigned_n = min(n_r, len(members)) if members else 0
                assigned_total += assigned_n
                if owners and members:
                    for node in preference_for_region(members, owners, region, assigned_n):
                        assigned[node] += 1
                        if n_r == 2:
                            per_node_buckets[node][0] += 1
                        elif n_r == 3:
                            per_node_buckets[node][1] += 1
                        elif n_r == 4:
                            per_node_buckets[node][2] += 1
                        per_node_n_sum[node] += n_r
            cw.writerow([run, f'{t:.1f}', gen, fp,
                         'unknown', n2, n3, n4,
                         f'{desired / 256:.4f}', desired, assigned_total,
                         sum(1 for region in range(256)
                             if min(max(target[region], 1), 256) >
                             (min(max(target[region], 1), len(members)) if members else 0)),
                         sum(1 for n in range(1, args.nodes + 1) if node_running(n, t)),
                         len(members)])
            for n in range(1, args.nodes + 1):
                a = assigned[n]
                nw.writerow([run, f'{t:.1f}', n, node_running(n, t),
                             node_membership(n, members), node_health(n, t),
                             gen, fp, 'unknown', a,
                             per_node_buckets[n][0], per_node_buckets[n][1],
                             per_node_buckets[n][2],
                             f'{per_node_n_sum[n] / a:.4f}' if a else '',
                             per_node_n_sum[n], a if node_running(n, t) else 0])
    tl['complete'] = True
    print(f'[timeline] merged: {len(events)} events, {len(snapshots)} snapshots, '
          f'{nt} samples, conflicts={conflicts}', flush=True)


def recorder_events(args):
    """Yield (time_s, node_id, boot, action, pid) from the recorded process
    events; regenerated lazily to keep merge_timeline self-contained."""
    p = args.out / 'node_process_events.csv'
    if not p.exists():
        return []
    out = []
    with open(p, newline='') as f:
        for row in csv.DictReader(f):
            out.append((float(row['time_s']), int(row['node_id']),
                        int(row['boot']), row['action'], row['pid']))
    return out




def error_category(error):
    if not error:return 'none'
    s=str(error).lower()
    if 'connection refused' in s or 'connect' in s or 'unreachable' in s:return 'gateway_connection'
    if 'quorum' in s or 'replica' in s:return 'quorum_or_replica'
    if 'timed out' in s or 'timeout' in s:return 'timeout_or_missing_ack'
    if 'closed' in s or 'reset' in s:return 'connection_closed'
    return 'other'


OPS_FIELDS = ('attempt', 'time_s', 'phase', 'workload_class', 'via_node', 'region',
              'x', 'y', 'color', 'success', 'latency_ms', 'error', 'error_category')


def convert_operations(out):
    original=out/'puts.csv'
    if not original.exists():return 0
    count=0
    with original.open(newline='') as src, (out/'runner_operations.csv').open('w',newline='') as dest:
        reader=csv.DictReader(src)
        writer=csv.DictWriter(dest,fieldnames=OPS_FIELDS);writer.writeheader()
        for row in reader:
            err='' if row.get('ack')=='1' else (row.get('reason') or 'no matching client pixel ACK')
            writer.writerow({'attempt':row.get('sequence',''),
                             'time_s':row.get('time_s',''),
                             'phase':row.get('phase',''),
                             'workload_class':('targeted' if row.get('region') == str(_target_region(out)) else 'uniform'),
                             'via_node':row.get('gateway',''),
                             'region':row.get('region',''),
                             'x':row.get('x',''),'y':row.get('y',''),'color':row.get('color',''),
                             'success':row.get('ack')=='1',
                             'latency_ms':row.get('latency_ms',''),
                             'error':err,'error_category':error_category(err)})
            count+=1
    return count


def _target_region(out):
    try:return json.loads((out/'summary.json').read_text()).get('region')
    except (OSError,ValueError):return None


def collect_native_metrics(out, count, boot_counts):
    """Copy peer-native rows into A/B per-incarnation dirs without changing native files."""
    metrics=out/'metrics';metrics.mkdir(exist_ok=True)
    for n in range(1,count+1):
        nd=out/f'node{n}'
        boot=boot_counts.get(n,1)
        dst=metrics/f'node{n}_boot{boot}';dst.mkdir(exist_ok=True)
        for path in nd.iterdir() if nd.exists() else []:
            if path.is_file() and path.suffix in ('.csv','.jsonl'):
                shutil.copy2(path,dst/path.name)
        # Existing stable state is under the node's CWD; do not fabricate a placement.
        state=nd/'state'/f'node{n}'/'placement.state'
        if state.exists():shutil.copy2(state,out/f'node{n}.state')


def normalize_verification(summary):
    """Translate targeted checkpoint counts to A/B's summary shape, with honest UNKNOWNs."""
    checks=summary.get('verification') or {}
    def one(name):
        r=checks.get(name) or {}
        c=r.get('counts') or {}
        return {'sampled_pixels':r.get('sampled_pixels',0),
                'checked_nodes':r.get('reader_nodes',[]),
                'missing':int(c.get('missing',0))+int(c.get('mismatch',0)),
                'read_failures':([{'error':'unreadable_samples='+str(c['unreadable'])}] if c.get('unreadable') else []),
                'examples':[], 'status':r.get('status','UNKNOWN')}
    summary['verification_immediate']=one('immediate')
    summary['verification_after_settle']=one('after_recovery')
    summary['verification_checkpoints']=checks
    summary['verification']=summary['verification_after_settle']
    summary['verification_comparison']={
        'same_sample':True,'requested_settle_s':max(0,summary.get('spec',{}).get('seconds',0)-summary.get('spec',{}).get('heat_until',0)),
        'immediate_missing_comparisons':summary['verification_immediate']['missing'],
        'followup_missing_comparisons':summary['verification_after_settle']['missing'],
        'net_mismatch_reduction':summary['verification_immediate']['missing']-summary['verification_after_settle']['missing'],
        'note':'Counts are per-pixel reader comparisons; unreadable snapshots are failures, not zeros.'}


def export_dashboard(out, summary, args, boot_counts):
    """Run only AFTER test teardown, as in reference A/B runner."""
    out=pathlib.Path(out)
    collect_native_metrics(out,args.nodes,boot_counts)
    # This function intentionally leaves original targeted verification metadata intact.
    total=convert_operations(out)
    if not (out/'fault_plan.json').exists():
        waves=[tuple(map(float,pair.split(':'))) for pair in args.waves.split(',')]
        plan=[{'at_s':t,'action':kind,'node':n} for a,b in waves for t,kind in ((a,'kill'),(b,'restart')) for n in (3,4)]
        (out/'fault_plan.json').write_text(json.dumps(sorted(plan,key=lambda r:(r['at_s'],r['action'],r['node'])),indent=2)+'\n')
    process_path=out/'node_process_events.csv'
    executed=[]
    if process_path.exists():
        with process_path.open(newline='') as fh:
            for row in csv.DictReader(fh):
                if row['action']=='kill' or (row['action']=='launch' and int(row['boot'])>1):
                    executed.append({'time_s':row['time_s'],'node':int(row['node_id']),
                                     'action':('restart' if row['action']=='launch' else 'kill'),
                                     'boot':int(row['boot'])})
    summary.update({'duration_s':args.seconds,'nodes':args.nodes,'churn_profile':'targeted-dual-owner-900',
        'rate_target':args.rate,'gateway_mode':'node1', 'targeted_write_percent':90,
        'planned_faults':4*len(args.waves.split(',')),
        'executed_faults':executed,
        'startup_ok':bool(summary.get('initial_placement_gen')),
        'failed':summary.get('attempts',0)-summary.get('acks',0),
        'acked':summary.get('acks',0),
        'success_pct':summary.get('put_success_pct'),
        'dashboard_schema':'run_experiment_A_vs_B',
        'dashboard_operations':total})
    normalize_verification(summary)
    by_group={}
    with (out/'runner_operations.csv').open(newline='') as fh:
        for row in csv.DictReader(fh):
            key=row['phase']+'/'+row['workload_class']
            g=by_group.setdefault(key,{'attempts':0,'acked':0,'failed':0,'error_categories':{}})
            g['attempts']+=1
            if row['success']=='True':g['acked']+=1
            else:
                g['failed']+=1
                cat=row['error_category'];g['error_categories'][cat]=g['error_categories'].get(cat,0)+1
    for group in by_group.values():
        group['success_pct']=round(100*group['acked']/group['attempts'],2)
    summary['by_phase_and_workload']=by_group
    summary['targeted_regions']=[summary['region']] if 'region' in summary else []
    summary['pixel_distribution']='90pct_hot_10pct_background_repeated_coords'
    # A/B merge reads the captured monotonic anchor and peer event/metric files.
    settings=SimpleNamespace(out=out,replication_timeline=True,no_replication_timeline=False,
           replication_sample_interval=1.0,seconds=args.seconds,nodes=args.nodes,seed=args.seed)
    merge_timeline(settings,summary)
    summary['dashboard_compatibility']={
        'version':1,'operation_rows':total,
        'complete':bool(summary.get('replication_timeline',{}).get('complete')),
        'warning':None if summary.get('replication_timeline',{}).get('complete') else
          'Historical membership/policy metrics absent: timeline was NOT reconstructed; see logs.'}
    (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(f'[dashboard] wrote {total} operations; timeline complete={summary["dashboard_compatibility"]["complete"]}',flush=True)

