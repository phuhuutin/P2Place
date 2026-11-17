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
    def __init__(self, args, result_dir):
        self.args, self.out = args, result_dir
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
        print(f'[node] start n={n} boot={p.incarnation} pid={p.process.pid} '
              f'tcp={p.tcp_port} ws={p.ws_port}', flush=True)

    def alive(self, n):
        p = self.peers[n].process
        return p is not None and p.poll() is None

    def kill(self, n):
        p = self.peers[n]
        if not self.alive(n):
            return
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
    p.add_argument('--churn-profile', choices=['aggressive', 'extreme', 'none'],
                   default='aggressive')
    p.add_argument('--join-timeout', type=int, default=100,
                   help='per-node sequential membership admission timeout')
    p.add_argument('--reliability-window', type=int, default=120)
    p.add_argument('--policy-interval', type=int, default=10)
    p.add_argument('--policy-cooldown', type=int, default=30)
    p.add_argument('--min-replicas', type=int, default=2)
    p.add_argument('--max-replicas', type=int, default=4)
    p.add_argument('--suspect-after', type=int, default=3)
    p.add_argument('--unavailable-after', type=int, default=15)
    p.add_argument('--recovery-stabilization', type=int, default=2)
    p.add_argument('--verify-nodes', type=int, default=3)
    p.add_argument('--verify-pixels', type=int, default=500)
    p.add_argument('--snapshot-timeout', type=float, default=60)
    p.add_argument('--settle', type=float, default=8,
                   help='seconds after writes stop before verification')
    a = p.parse_args()
    if a.nodes != 10:
        p.error('this fault schedule is designed for exactly 10 nodes')
    if a.seconds < 180 and a.churn_profile != 'none':
        p.error('use --seconds >= 180 for churn to allow detector and recovery')
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
    cluster = Cluster(args, args.out)
    events = create_events(args.churn_profile, args.seconds)
    (args.out / 'config.json').write_text(json.dumps({
        k: str(v) if isinstance(v, pathlib.Path) else v
        for k, v in vars(args).items()}, indent=2) + '\n')
    (args.out / 'fault_plan.json').write_text(json.dumps(
        [{'at_s': t, 'action': op, 'node': n} for t, op, n in events], indent=2) + '\n')
    writer = (args.out / 'runner_operations.csv').open('w', newline='')
    csvw = csv.writer(writer)
    csvw.writerow(['attempt', 'time_s', 'via_node', 'region', 'x', 'y',
                   'color', 'success', 'latency_ms', 'error'])
    results = []
    acknowledged = []
    summary = {'strategy': args.strategy, 'seed': args.seed,
               'duration_s': args.seconds, 'nodes': args.nodes,
               'churn_profile': args.churn_profile,
               'rate_target': args.rate, 'planned_faults': len(events),
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
        scheduled, inflight = {}, {}
        completed = 0
        start = time.monotonic()
        next_dispatch = start
        max_pending = args.workers * 3
        next_fault = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            while time.monotonic() - start < args.seconds:
                elapsed = time.monotonic() - start
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
                               'latency_ms': latency, 'error': err}
                        results.append(row)
                        csvw.writerow([row[k] for k in ('attempt', 'time_s', 'via_node',
                                                         'region', 'x', 'y', 'color',
                                                         'success', 'latency_ms', 'error')])
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
                    pos = positions[index]
                    x, y = pos % 256, pos // 256
                    color = '#%06X' % rng.randrange(0x1000000)
                    node = (index % args.nodes) + 1
                    metadata = {'attempt': index, 'time_s': round(elapsed, 3),
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
                       'latency_ms': latency, 'error': err}
                results.append(row)
                csvw.writerow([row[k] for k in ('attempt', 'time_s', 'via_node',
                                                'region', 'x', 'y', 'color',
                                                'success', 'latency_ms', 'error')])
                if success:
                    acknowledged.append(row)
        writer.flush()
        print(f'[run] finished {len(results)} attempted PUTs; '
              f'{len(acknowledged)} acked', flush=True)
        time.sleep(args.settle)
        summary['verification'] = verify_samples(cluster, acknowledged, args)
        return_code = (0 if summary['verification']['missing'] == 0 and
                       not summary['verification']['read_failures'] else 2)
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
        (args.out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        writer.close()
        cluster.stop_all()
        print(f'[results] {args.out / "summary.json"}', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
