#!/usr/bin/env python3
"""P2Place 10-node A/B stress experiment. Python stdlib + repository tests helpers.

Run from repository root (or from anywhere). Isolated ports, placement files,
metrics per incarnation and logs. No modification to the user's state/ folder.
The runner is NOT a substitute for compiling/running a real P2Place cluster.
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures
import csv
import hashlib
import json
import pathlib
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'tests'))
from test_websocket_manual import WebSocketClient  # noqa: E402
from test_lab04_canvas_sync import snapshot  # noqa: E402


class SilentWebSocket(WebSocketClient):
    """Exactly the project's WebSocket helper, minus its handshake test print."""
    def connect(self):
        import base64
        import os
        self.sock = socket.create_connection((self.host, self.port), timeout=3.0)
        self.sock.settimeout(3.0)
        key = base64.b64encode(os.urandom(16)).decode('ascii')
        request = (f'GET / HTTP/1.1\r\nHost: {self.host}:{self.port}\r\n'
                   'Upgrade: websocket\r\nConnection: Upgrade\r\n'
                   'Sec-WebSocket-Version: 13\r\n'
                   f'Sec-WebSocket-Key: {key}\r\n\r\n')
        self.sock.sendall(request.encode('ascii'))
        while b'\r\n\r\n' not in self.buffer:
            data = self.sock.recv(4096)
            if not data:
                raise ConnectionError('WebSocket handshake closed')
            self.buffer += data
            if len(self.buffer) > 8192:
                raise ValueError('WebSocket handshake too large')
        raw, self.buffer = self.buffer.split(b'\r\n\r\n', 1)
        lines = raw.decode('ascii').split('\r\n')
        if lines[0] != 'HTTP/1.1 101 Switching Protocols':
            raise ConnectionError('WebSocket upgrade failed: ' + lines[0])
        headers = dict((k.lower().strip(), v.strip()) for line in lines[1:]
                       for k, sep, v in [line.partition(':')] if sep)
        expected = base64.b64encode(hashlib.sha1(
            (key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode('ascii')
        ).digest()).decode('ascii')
        if headers.get('sec-websocket-accept') != expected:
            raise ConnectionError('Invalid WebSocket handshake accept')


def available_port(excluded):
    for _ in range(200):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            p = sock.getsockname()[1]
        if p not in excluded:
            excluded.add(p)
            return p
    raise RuntimeError('Cannot reserve a distinct port')


def placement(path):
    try:
        fields = path.read_text().split()
        if fields[0] not in ('P2PLACE_BALANCED_V1', 'P2PLACE_BALANCED_V2'):
            return None
        generation, count = int(fields[1]), int(fields[2])
        return generation, tuple(map(int, fields[3:3 + count]))
    except (OSError, ValueError, IndexError):
        return None


def placement_preferences(path, primary_nodes=(8, 9), baseline_n=3):
    """Read actual V1/V2 balanced placement; find regions where both peers
    are in Strategy A's N=3 preference, and the fourth peer is different.
    The layout matches BalancedPlacement::save and preference in the C++ repo.
    """
    fields = path.read_text().split()
    if fields[0] not in ('P2PLACE_BALANCED_V1', 'P2PLACE_BALANCED_V2'):
        raise ValueError('Unsupported placement file')
    count = int(fields[2])
    members = [int(x) for x in fields[3:3 + count]]
    owner_at = 3 + count + (count if fields[0].endswith('V2') else 0)
    owners = [int(x) for x in fields[owner_at:owner_at + 256]]
    if len(owners) != 256 or len(members) < 4:
        raise ValueError('Placement lacks 256 owners or four distinct members')
    targets = []
    for region, owner in enumerate(owners):
        at = members.index(owner)
        pref = [members[(at + j) % count] for j in range(4)]
        if (all(n in pref[:baseline_n] for n in primary_nodes)
                and all(n not in pref[baseline_n:4] for n in primary_nodes)):
            targets.append(region)
    return targets


def phase_name(t):
    if t < 100: return 'baseline'
    if t < 285: return 'training'
    if t < 310: return 'backfill'
    if t < 445: return 'double_failure'
    return 'recovery'


def latest_policy_counts(log_path):
    try:
        content = log_path.read_text(errors='replace')
    except OSError:
        return None
    lines = [line for line in content.splitlines() if '[policy-broadcast]' in line]
    if not lines:
        return None
    last = lines[-1]
    counts = dict((k, int(v)) for k, v in re.findall(r'\b(n[234])=(\d+)', last))
    return {'broadcast': last, 'counts': counts} if counts else None


def error_category(error):
    if not error: return 'none'
    s = error.lower()
    if 'connection refused' in s or 'connect' in s or 'unreachable' in s:
        return 'gateway_connection'
    if 'quorum' in s or 'replica' in s: return 'quorum_or_replica'
    if 'timed out' in s or 'timeout' in s or 'no matching' in s:
        return 'timeout_or_missing_ack'
    if 'closed' in s or 'reset' in s: return 'connection_closed'
    return 'other'


def log_tail(path, lines=30):
    try:
        return '\n'.join(path.read_text(errors='replace').splitlines()[-lines:])
    except OSError:
        return '(no log file)'


@dataclass
class Peer:
    node_id: int
    tcp_port: int
    ws_port: int
    process: subprocess.Popen | None = None
    incarnation: int = 0
    log_handle: object = None


class Cluster:
    def __init__(self, args, result_dir, recorder=None):
        self.args, self.out = args, result_dir
        self.recorder = recorder
        used = set()
        self.peers = {n: Peer(n, available_port(used), available_port(used))
                      for n in range(1, args.nodes + 1)}
        self.faults = []

    def launch(self, n):
        p = self.peers[n]
        if p.process is not None and p.process.poll() is None:
            raise RuntimeError(f'Node {n} already running')
        p.incarnation += 1
        incarnation_dir = self.out / 'metrics' / f'node{n}_boot{p.incarnation}'
        incarnation_dir.mkdir(parents=True, exist_ok=True)
        p.log_handle = (self.out / f'node{n}_boot{p.incarnation}.log').open('w')
        args = [str(self.args.peer), '--id', str(n), '--port', str(p.tcp_port),
                '--ws-port', str(p.ws_port), '--state-file',
                str(self.out / f'node{n}.state'), '--replicas', '3',
                '--read-quorum', '2', '--write-quorum', '2',
                '--strategy', self.args.strategy,
                '--min-replicas', str(self.args.min_replicas),
                '--max-replicas', str(self.args.max_replicas),
                '--reliability-window', str(self.args.reliability_window),
                '--policy-interval', str(self.args.policy_interval),
                '--policy-cooldown', str(self.args.policy_cooldown),
                '--suspect-after', str(self.args.suspect_after),
                '--unavailable-after', str(self.args.unavailable_after),
                '--recovery-stabilization', str(self.args.recovery_stabilization),
                '--metrics-dir', str(incarnation_dir)]
        if n != 1:
            # Always-healthy seed where possible. Node 1 may be dead in the
            # authority-failover phase, so use Node 2 for rejoining Node 1.
            seed = 2 if n == 1 or not self.alive(1) else 1
            args += ['--peers', f'{seed}:127.0.0.1:{self.peers[seed].tcp_port}']
        if n == 1 and p.incarnation > 1:
            args += ['--peers', f'2:127.0.0.1:{self.peers[2].tcp_port}']
        if shutil.which('stdbuf'):
            args = ['stdbuf', '-oL', '-eL'] + args
        p.process = subprocess.Popen(args, stdout=p.log_handle,
                                     stderr=subprocess.STDOUT, cwd=str(ROOT))
        if self.recorder:
            self.recorder.record(n, p.incarnation, 'launch', p.process.pid)
        print(f'[node] start n={n} boot={p.incarnation} pid={p.process.pid} '
              f'tcp={p.tcp_port} ws={p.ws_port}', flush=True)

    def alive(self, n):
        p = self.peers[n].process
        return p is not None and p.poll() is None

    def kill(self, n):
        p = self.peers[n]
        if not self.alive(n):
            return
        if self.recorder:
            self.recorder.record(n, p.incarnation, 'kill', p.process.pid)
        p.process.kill()  # SIGKILL: abrupt crash, not graceful leave.
        try:
            p.process.wait(timeout=4)
        except subprocess.TimeoutExpired:
            pass
        if p.log_handle:
            p.log_handle.close()
            p.log_handle = None
        print(f'[fault] KILL node={n}', flush=True)

    def stop_all(self):
        for p in self.peers.values():
            if p.process and p.process.poll() is None:
                p.process.terminate()
        for p in self.peers.values():
            if p.process:
                try:
                    p.process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.process.kill()
                    p.process.wait()
            if p.log_handle and not p.log_handle.closed:
                p.log_handle.close()

    def wait_members(self, count, timeout):
        want = tuple(range(1, count + 1))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            states = [placement(self.out / f'node{n}.state') for n in range(1, count + 1)]
            if all(s and s[1] == want for s in states):
                # Membership should converge to the same committed generation.
                gens = {s[0] for s in states}
                if len(gens) == 1:
                    print(f'[cluster] {count}/{self.args.nodes} members '
                          f'committed generation={next(iter(gens))}', flush=True)
                    return
            for n in range(1, count + 1):
                if not self.alive(n):
                    raise RuntimeError(f'node {n} exited during startup; '
                                       f'log:\n{log_tail(self.out / f"node{n}_boot1.log")}')
            time.sleep(0.4)
        detail = {n: placement(self.out / f'node{n}.state')
                  for n in range(1, count + 1)}
        raise RuntimeError(f'cluster never reached {count} members; states={detail}; '
                           f'last-started log:\n'
                           + log_tail(self.out / f'node{count}_boot1.log'))


def one_put(ws_port, x, y, color, timeout):
    """Unique pixel coordinate per PUT; pixel event is emitted after W quorum."""
    start = time.monotonic()
    ws = SilentWebSocket('127.0.0.1', ws_port)
    try:
        ws.connect()
        ws.sock.settimeout(min(2.0, timeout))
        ws.send_frame(0x1, json.dumps({'op': 'put', 'x': x, 'y': y,
                                     'color': color}))
        deadline = start + timeout
        while time.monotonic() < deadline:
            ws.sock.settimeout(min(1.5, max(0.05, deadline - time.monotonic())))
            try:
                op, payload = ws.recv_frame()
            except socket.timeout:
                continue
            if op == 0x9:
                ws.send_frame(0xA, payload)
            elif op == 0x8:
                raise ConnectionError('WS closed')
            elif op == 0x1:
                message = json.loads(payload)
                if message.get('op') == 'error':
                    raise RuntimeError(str(message.get('message')))
                if (message.get('op') == 'pixel' and
                        message.get('x') == x and message.get('y') == y and
                        str(message.get('color')).upper() == color):
                    return True, round((time.monotonic()-start)*1000, 2), ''
        raise TimeoutError('no matching quorum-confirmed pixel notification')
    except Exception as exc:
        return False, round((time.monotonic()-start)*1000, 2), str(exc)[:240]
    finally:
        ws.close()


def create_events(profile, duration):
    if profile == 'none':
        return []
    # Intervals scaled to requested duration; long enough for detection and
    # reconfiguration in the recommended >=300-second run.
    e = [(0.16, 'kill', 8), (0.26, 'restart', 8),
         (0.38, 'kill', 9), (0.42, 'kill', 10),
         (0.53, 'restart', 9), (0.57, 'restart', 10),
         (0.69, 'kill', 1), (0.79, 'restart', 1)]
    if profile == 'extreme':
        e += [(0.29, 'kill', 7), (0.34, 'restart', 7),
              (0.59, 'kill', 6), (0.64, 'restart', 6)]
    if profile == 'predictive-churn':
        # 11-second outages below the 15s UNAVAILABLE threshold, but long
        # enough to be observed as SUSPECT after 3s. A rolling 120s window
        # can thus accumulate enough unhealthy time to trigger N=4.
        # The last training crash finishes 261+11=272; policy and backfill
        # have 38 seconds before the first overlapping crash.
        short_starts = (100, 123, 146, 169, 192, 215, 238, 261)
        pairs = ((310, 322), (350, 362), (390, 402), (430, 442))
        timeline = []
        for start_s in short_starts:
            timeline += [(start_s, 'kill', 8), (start_s + 11, 'restart', 8)]
        for start_s, end_s in pairs:
            for node_id in (8, 9):
                timeline += [(start_s, 'kill', node_id),
                             (end_s, 'restart', node_id)]
        return sorted(timeline)
    return sorted([(round(f*duration, 2), action, n)
                   for f, action, n in e])


def verify_samples(cluster, acknowledged, args):
    if not acknowledged or args.verify_nodes == 0:
        return {'sampled_pixels': 0, 'checked_nodes': [], 'missing': 0,
                'read_failures': [], 'examples': []}
    rng = random.Random(args.seed + 10007)
    selected = rng.sample(acknowledged, min(args.verify_pixels, len(acknowledged)))
    live = [n for n in cluster.peers if cluster.alive(n)]
    reader_nodes = live[:args.verify_nodes]
    failures, missing, examples = [], 0, []
    for n in reader_nodes:
        try:
            canvas = snapshot('127.0.0.1', cluster.peers[n].ws_port,
                              args.snapshot_timeout)
            for item in selected:
                actual = canvas.get((item['x'], item['y']))
                if actual != item['color']:
                    missing += 1
                    if len(examples) < 20:
                        examples.append({'reader': n, 'x': item['x'],
                                         'y': item['y'], 'expected': item['color'],
                                         'actual': actual})
            print(f'[verify] node={n} checked={len(selected)} '
                  f'missing_total={missing}', flush=True)
        except Exception as exc:
            failures.append({'node': n, 'error': str(exc)})
            print(f'[verify] node={n} snapshot error: {exc}', flush=True)
    return {'sampled_pixels': len(selected), 'checked_nodes': reader_nodes,
            'missing': missing, 'read_failures': failures, 'examples': examples}


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


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--strategy', choices=['A', 'B'], required=True)
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--seconds', type=int, default=300,
                   help='workload duration, excluding cluster boot and verification')
    p.add_argument('--nodes', type=int, default=10)
    p.add_argument('--out', type=pathlib.Path, required=True)
    p.add_argument('--peer', type=pathlib.Path,
                   default=ROOT / 'build/bin/p2place-peer')
    p.add_argument('--rate', type=float, default=16.0,
                   help='attempted PUTs per second, across all nodes')
    p.add_argument('--workers', type=int, default=12)
    p.add_argument('--put-timeout', type=float, default=4.0)
    p.add_argument('--churn-profile', choices=['aggressive', 'extreme', 'none', 'predictive-churn'],
                   default='aggressive')
    p.add_argument('--targeted-write-percent', type=float, default=None,
                   help='percent of writes to regions where 8 and 9 are both in fixed N3; '
                        'defaults to 60 for predictive-churn and 0 otherwise')
    p.add_argument('--gateway-mode', choices=['round-robin', 'healthy'], default=None,
                   help='healthy uses only stable gateway peers 2..7 in predictive mode')
    p.add_argument('--join-timeout', type=int, default=100,
                   help='per-node sequential membership admission timeout')
    p.add_argument('--reliability-window', type=int, default=120)
    p.add_argument('--policy-interval', type=int, default=10)
    p.add_argument('--policy-cooldown', type=int, default=30)
    p.add_argument('--min-replicas', type=int, default=3)
    p.add_argument('--max-replicas', type=int, default=4)
    p.add_argument('--suspect-after', type=int, default=3)
    p.add_argument('--unavailable-after', type=int, default=15)
    p.add_argument('--recovery-stabilization', type=int, default=2)
    p.add_argument('--verify-nodes', type=int, default=3)
    p.add_argument('--verify-pixels', type=int, default=500)
    p.add_argument('--snapshot-timeout', type=float, default=60)
    p.add_argument('--settle', type=float, default=60,
                   help='seconds from workload end to follow-up verification (default 60)')
    p.add_argument('--replication-timeline', action='store_true', default=True,
                   help='capture the replication-N timeline (default on)')
    p.add_argument('--no-replication-timeline', action='store_true',
                   help='disable timeline capture to quantify overhead')
    p.add_argument('--replication-sample-interval', type=float, default=1.0,
                   help='reconstruction sample step in seconds (0 disables samples)')
    a = p.parse_args()
    if a.nodes != 10:
        p.error('this fault schedule is designed for exactly 10 nodes')
    if a.churn_profile == 'predictive-churn' and a.seconds < 600:
        p.error('predictive-churn requires --seconds >= 600 for training/recovery phases')
    if a.seconds < 180 and a.churn_profile != 'none':
        p.error('use --seconds >= 180 for churn to allow detector and recovery')
    if a.targeted_write_percent is None:
        a.targeted_write_percent = 60.0 if a.churn_profile == 'predictive-churn' else 0.0
    if a.gateway_mode is None:
        a.gateway_mode = 'healthy' if a.churn_profile == 'predictive-churn' else 'round-robin'
    if not 0 <= a.targeted_write_percent <= 100:
        p.error('targeted-write-percent must be between 0 and 100')
    if a.settle < 0:
        p.error('settle must be non-negative')
    if a.rate <= 0 or a.workers < 1 or a.put_timeout <= 0:
        p.error('rate, workers and put-timeout must be positive')
    if a.verify_nodes < 0 or a.verify_nodes > a.nodes:
        p.error('invalid verify-nodes')
    a.peer = a.peer.expanduser().resolve()
    if not a.peer.is_file():
        p.error(f'build executable not found: {a.peer}')
    if a.out.exists() and any(a.out.iterdir()):
        p.error(f'output directory is not empty: {a.out} (choose a fresh path)')
    a.out.mkdir(parents=True, exist_ok=True)
    return a


def main():
    args = parse_args()
    timeline_enabled = args.replication_timeline and not args.no_replication_timeline
    recorder = TimelineRecorder(args.out, timeline_enabled)
    cluster = Cluster(args, args.out, recorder)
    events = create_events(args.churn_profile, args.seconds)
    (args.out / 'config.json').write_text(json.dumps({
        k: str(v) if isinstance(v, pathlib.Path) else v
        for k, v in vars(args).items()}, indent=2) + '\n')
    (args.out / 'fault_plan.json').write_text(json.dumps(
        [{'at_s': t, 'action': op, 'node': n} for t, op, n in events], indent=2) + '\n')
    writer = (args.out / 'runner_operations.csv').open('w', newline='')
    csvw = csv.writer(writer)
    fields = ('attempt', 'time_s', 'phase', 'workload_class', 'via_node', 'region',
              'x', 'y', 'color', 'success', 'latency_ms', 'error', 'error_category')
    csvw.writerow(fields)
    results = []
    acknowledged = []
    summary = {'strategy': args.strategy, 'seed': args.seed,
               'duration_s': args.seconds, 'nodes': args.nodes,
               'churn_profile': args.churn_profile,
               'rate_target': args.rate, 'gateway_mode': args.gateway_mode,
               'targeted_write_percent': args.targeted_write_percent,
               'planned_faults': len(events),
               'executed_faults': [], 'startup_ok': False}
    start = None
    try:
        # Sequential admission: avoid triggering many concurrent join cycles.
        for n in range(1, args.nodes + 1):
            cluster.launch(n)
            cluster.wait_members(n, args.join_timeout)
        summary['startup_ok'] = True
        print('\n[viewer] Open browser tabs if web UI is served on :8080:', flush=True)
        for n, peer in cluster.peers.items():
            print(f'  node {n}: http://localhost:8080/?ws={peer.ws_port}', flush=True)
        print(f'\n[run] strategy={args.strategy} nodes=10 seconds={args.seconds} '
              f'rate={args.rate}/s profile={args.churn_profile}', flush=True)
        rng = random.Random(args.seed)
        # One coordinate per attempted PUT: final snapshot can safely verify
        # acknowledged writes without overwrite races or Lamport ordering ties.
        positions = list(range(256 * 256))
        rng.shuffle(positions)
        targeted_regions = (placement_preferences(args.out / 'node1.state')
                            if args.churn_profile == 'predictive-churn' or
                            args.targeted_write_percent > 0 else [])
        if args.targeted_write_percent and not targeted_regions:
            raise RuntimeError('no regions match baseline replica preference for nodes 8,9')
        targeted_set = set(targeted_regions)
        targeted_positions = [v for v in positions if ((v // 256)//16)*16 + (v%256)//16 in targeted_set]
        uniform_positions = list(positions)
        used_positions = set()
        targeted_idx = uniform_idx = 0
        write_rng = random.Random(args.seed + 991)
        if targeted_regions:
            print(f'[workload] targeted_regions={targeted_regions} '
                  f'count={len(targeted_regions)} '
                  f'percent={args.targeted_write_percent} '
                  f'gateway_mode={args.gateway_mode}', flush=True)
        summary['targeted_regions'] = targeted_regions
        summary['targeted_unique_coordinate_capacity'] = len(targeted_positions)
        expected_targeted = args.rate * args.seconds * args.targeted_write_percent / 100
        if expected_targeted > len(targeted_positions) and args.targeted_write_percent:
            print('[warning] target pool may be exhausted: requested approximately '
                  f'{expected_targeted:.0f} targeted writes, only '
                  f'{len(targeted_positions)} unique coordinates available; '
                  'excess writes fall back to uniform', flush=True)
        scheduled, inflight = {}, {}
        completed = 0
        start = time.monotonic()
        # Timeline clock anchor: absolute monotonic ns shared with the peer
        # processes on the same kernel (CLOCK_MONOTONIC).
        recorder.set_anchor(time.monotonic_ns())
        summary['time_base'] = {'workload_start_monotonic_ns': recorder.anchor_ns,
                                'source': 'time.monotonic_ns() / CLOCK_MONOTONIC'}
        next_dispatch = start
        max_pending = args.workers * 3
        next_fault = 0
        readiness_checked = False
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            while time.monotonic() - start < args.seconds:
                elapsed = time.monotonic() - start
                if (args.churn_profile == 'predictive-churn' and
                        not readiness_checked and elapsed >= 300):
                    readiness_checked = True
                    snapshot_policy = latest_policy_counts(args.out / 'node1_boot1.log')
                    summary['pre_double_failure_policy'] = snapshot_policy
                    if args.strategy == 'B':
                        n4 = (snapshot_policy or {}).get('counts', {}).get('n4', 0)
                        if n4 == 0:
                            print('[warning] B has no logged N=4 policy at 300s; '
                                  'the test may not show an adaptive advantage', flush=True)
                        else:
                            print(f'[policy-check] N=4 regions before double failure: {n4}', flush=True)
                while next_fault < len(events) and elapsed >= events[next_fault][0]:
                    at, action, n = events[next_fault]
                    cluster.kill(n) if action == 'kill' else cluster.launch(n)
                    entry = {'planned_at_s': at, 'actual_at_s': round(elapsed, 2),
                             'action': action, 'node': n}
                    summary['executed_faults'].append(entry)
                    next_fault += 1
                # Drain completed futures while the experiment is in progress.
                for future in list(inflight):
                    if future.done():
                        metadata = inflight.pop(future)
                        success, latency, err = future.result()
                        row = {**metadata, 'success': success,
                               'latency_ms': latency, 'error': err,
                               'error_category': error_category(err)}
                        results.append(row)
                        csvw.writerow([row[k] for k in fields])
                        if success:
                            acknowledged.append(row)
                        completed += 1
                        if completed % 100 == 0:
                            ok = sum(r['success'] for r in results)
                            print(f'[progress] {elapsed:.1f}s completed={completed} '
                                  f'acked={ok} failed={completed-ok} '
                                  f'active={len(inflight)}', flush=True)
                if time.monotonic() >= next_dispatch and len(inflight) < max_pending:
                    index = len(scheduled)
                    if index >= len(positions):
                        break
                    chosen_targeted = (args.targeted_write_percent > 0 and
                                       write_rng.random()*100 < args.targeted_write_percent)
                    pos = None
                    if chosen_targeted:
                        while targeted_idx < len(targeted_positions):
                            candidate = targeted_positions[targeted_idx]
                            targeted_idx += 1
                            if candidate not in used_positions:
                                pos = candidate
                                break
                    if pos is None:
                        chosen_targeted = False  # selected fallback after exhausted targeted pool
                        while uniform_idx < len(uniform_positions):
                            candidate = uniform_positions[uniform_idx]
                            uniform_idx += 1
                            if candidate not in used_positions:
                                pos = candidate
                                break
                    if pos is None:
                        break
                    used_positions.add(pos)
                    x, y = pos % 256, pos // 256
                    color = '#%06X' % rng.randrange(0x1000000)
                    gateways = list(range(2, 8)) if args.gateway_mode == 'healthy' else list(cluster.peers)
                    node = gateways[index % len(gateways)]
                    metadata = {'attempt': index, 'time_s': round(elapsed, 3),
                                'phase': phase_name(elapsed) if args.churn_profile == 'predictive-churn' else 'standard',
                                'workload_class': ('targeted' if chosen_targeted else 'uniform'),
                                'via_node': node, 'region': (y // 16)*16 + x//16,
                                'x': x, 'y': y, 'color': color}
                    future = pool.submit(one_put, cluster.peers[node].ws_port,
                                         x, y, color, args.put_timeout)
                    scheduled[index] = True
                    inflight[future] = metadata
                    next_dispatch += 1.0 / args.rate
                    # No burst catch-up after stalls.
                    if next_dispatch < time.monotonic()-0.5:
                        next_dispatch = time.monotonic()
                time.sleep(0.012)
            # Drain outstanding requests; results still counted after deadline.
            for future, metadata in list(inflight.items()):
                success, latency, err = future.result()
                row = {**metadata, 'success': success,
                       'latency_ms': latency, 'error': err,
                       'error_category': error_category(err)}
                results.append(row)
                csvw.writerow([row[k] for k in fields])
                if success:
                    acknowledged.append(row)
        writer.flush()
        by_group = collections.defaultdict(lambda: {'attempts': 0, 'acked': 0, 'failed': 0, 'error_categories': collections.Counter()})
        for r in results:
            k = r['phase'] + '/' + r['workload_class']
            g = by_group[k]
            g['attempts'] += 1
            g['acked' if r['success'] else 'failed'] += 1
            if not r['success']: g['error_categories'][r['error_category']] += 1
        summary['by_phase_and_workload'] = {k: {**v, 'success_pct': round(100*v['acked']/v['attempts'], 2)}
                                             for k, v in sorted(by_group.items())}
        for k, v in summary['by_phase_and_workload'].items():
            print(f'[phase] {k}: acked={v["acked"]} failed={v["failed"]} '
                  f'success={v["success_pct"]}%', flush=True)
        print(f'[run] finished {len(results)} attempted PUTs; '
              f'{len(acknowledged)} acked', flush=True)
        # Two independent snapshots of the SAME deterministic sample. The
        # second check is scheduled relative to workload completion, not
        # relative to the end of the first (possibly slow) snapshot.
        writes_ended_at = time.monotonic()
        print('[verify] immediate check: starting after PUT workload', flush=True)
        summary['verification_immediate'] = verify_samples(cluster, acknowledged, args)
        summary['verification_immediate']['elapsed_since_writes_s'] = round(
            time.monotonic() - writes_ended_at, 3)
        print('[verify] immediate: missing={missing}, read_failures={failed}'.format(
            missing=summary['verification_immediate']['missing'],
            failed=len(summary['verification_immediate']['read_failures'])), flush=True)

        remaining = max(0.0, args.settle - (time.monotonic() - writes_ended_at))
        if remaining:
            print(f'[verify] synchronization period: {remaining:.1f}s remaining '
                  f'(target {args.settle:g}s after workload)', flush=True)
            time.sleep(remaining)
        print('[verify] follow-up check: starting after recovery period', flush=True)
        summary['verification_after_settle'] = verify_samples(cluster, acknowledged, args)
        summary['verification_after_settle']['elapsed_since_writes_s'] = round(
            time.monotonic() - writes_ended_at, 3)
        # Backwards compatibility: existing dashboards read summary.verification.
        summary['verification'] = summary['verification_after_settle']
        immediate = summary['verification_immediate']
        followup = summary['verification_after_settle']
        summary['verification_comparison'] = {
            'same_sample': True,
            'requested_settle_s': args.settle,
            'immediate_missing_comparisons': immediate['missing'],
            'followup_missing_comparisons': followup['missing'],
            'net_mismatch_reduction': immediate['missing'] - followup['missing'],
            'note': 'Counts are pixel-reader comparisons, not necessarily unique pixels. '
                    'A reduction does not prove every copy converged.'
        }
        print(f'[verify] comparison: immediate={immediate["missing"]} '
              f'follow-up={followup["missing"]} '
              f'net_reduction={immediate["missing"] - followup["missing"]}',
              flush=True)
        return_code = (0 if followup['missing'] == 0 and
                       not followup['read_failures'] else 2)
        return return_code
    except Exception as exc:
        summary['fatal_error'] = str(exc)
        print(f'[FATAL] {exc}', file=sys.stderr, flush=True)
        return 1
    finally:
        summary['attempts'] = len(results)
        summary['acked'] = len(acknowledged)
        summary['failed'] = len(results) - len(acknowledged)
        summary['success_pct'] = round(100*len(acknowledged)/len(results), 3) if results else None
        summary['latency_ms_acked_avg'] = (round(sum(r['latency_ms'] for r in results
                                        if r['success'])/len(acknowledged), 2)
                                        if acknowledged else None)
        summary['final_placement'] = {str(n): placement(args.out / f'node{n}.state')
                                      for n in cluster.peers}
        writer.close()
        cluster.stop_all()
        recorder.close()
        # Post-run timeline reconstruction (never touches the experiment
        # itself: runs after workload, verification and teardown).
        try:
            merge_timeline(args, summary)
        except Exception as exc:  # noqa: BLE001
            summary['replication_timeline'] = summary.get('replication_timeline', {})
            summary['replication_timeline']['complete'] = False
            summary['replication_timeline']['error'] = str(exc)
            print(f'[timeline] WARNING: merge failed: {exc}', file=sys.stderr,
                  flush=True)
        (args.out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(f'[results] {args.out / "summary.json"}', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
