#!/usr/bin/env python3
"""Centered 50% P2Place live churn painter: four nodes, fifth joins, fourth crashes (no painting pause).

- Loads a 256x256 JSON image.
- Scales it to 50% size (128x128) with nearest-neighbor sampling.
- Centers the scaled image on the 256x256 P2Place canvas.
- Skips white pixels by default, so it does not paint the white background.
- Painting never pauses for churn: N5 joins asynchronously at 35%, N4 crashes at 70%.
- Churn is triggered by client-side SENT progress, not by quorum-confirmed writes.

Run from the P2Place project root. Put this file in tests/ beside
"test_websocket_manual.py".
"""
from __future__ import annotations

import argparse
import json
import math
import queue
import random
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from test_websocket_manual import WebSocketClient

SIZE = 256
WHITE = '#FFFFFF'


def load_rows(path: Path):
    with path.open(encoding='utf-8') as fh:
        image = json.load(fh)
    if (image.get('width'), image.get('height')) != (SIZE, SIZE):
        raise ValueError('JSON must describe a 256x256 image')
    if image.get('origin', 'top-left') != 'top-left':
        raise ValueError('Expected top-left origin')
    rows = image.get('pixels')
    if not isinstance(rows, list) or len(rows) != SIZE:
        raise ValueError('JSON must contain 256 rows')
    for y, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != SIZE:
            raise ValueError(f'Row {y} must contain 256 pixels')
        for x, value in enumerate(row):
            if not (isinstance(value, str) and len(value) == 7 and value.startswith('#') and
                    all(ch in '0123456789abcdefABCDEF' for ch in value[1:])):
                raise ValueError(f'Invalid color at ({x},{y}): {value!r}')
            row[x] = value.upper()
    return rows


def build_center_scaled_pixels(rows, scale: float, skip_white: bool, stride: int):
    dest = max(1, min(SIZE, int(round(SIZE * scale))))
    x0 = (SIZE - dest) // 2
    y0 = (SIZE - dest) // 2
    out = []
    for dy in range(dest):
        sy = min(SIZE - 1, int(dy / scale))
        for dx in range(dest):
            if (dy * dest + dx) % stride:
                continue
            sx = min(SIZE - 1, int(dx / scale))
            color = rows[sy][sx]
            if skip_white and color == WHITE:
                continue
            out.append((x0 + dx, y0 + dy, color))
    return out, dest, (x0, y0)


def split_jobs_evenly(pixels, nodes: int, seed: int):
    pixels = list(pixels)
    random.Random(seed).shuffle(pixels)
    jobs = [[] for _ in range(nodes)]
    for i, item in enumerate(pixels):
        jobs[i % nodes].append(item)
    return jobs


def members_in_state(path):
    try:
        data = path.read_text().split()
        if data[0] not in ('P2PLACE_BALANCED_V1', 'P2PLACE_BALANCED_V2'):
            return None
        count = int(data[2])
        return tuple(int(x) for x in data[3:3 + count])
    except (FileNotFoundError, OSError, IndexError, ValueError):
        return None


def launch_nodes(args, state_dir, peer_ports, ws_ports):
    executable = args.peer.resolve()
    if not executable.is_file():
        raise FileNotFoundError(f'Peer binary not found: {executable}')
    state_dir.mkdir(parents=True, exist_ok=True)
    processes, handles = [], []
    try:
        for n in range(1, args.nodes + 1):
            cmd = [str(executable), '--id', str(n),
                   '--port', str(peer_ports[n - 1]), '--ws-port', str(ws_ports[n - 1]),
                   '--state-file', str(state_dir / f'node{n}.state'),
                   '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2',
                   '--strategy', args.strategy]
            if args.strategy in ('B', 'C'):
                cmd += ['--min-replicas', '2', '--max-replicas', '4']
            if n != 1:
                cmd += ['--peers', f'1:{args.host}:{peer_ports[0]}']
            logfile = (state_dir / f'node{n}.log').open('w', encoding='utf-8')
            handles.append(logfile)
            processes.append(subprocess.Popen(cmd, stdout=logfile, stderr=subprocess.STDOUT))
            print(f'[START] node={n} tcp={peer_ports[n - 1]} ws={ws_ports[n - 1]}', flush=True)
            if n == 1:
                end = time.monotonic() + args.ready_timeout
                while time.monotonic() < end:
                    if processes[0].poll() is not None:
                        raise RuntimeError('Node 1 exited during bootstrap; check its log')
                    if members_in_state(state_dir / 'node1.state') == (1,):
                        break
                    time.sleep(.2)
                else:
                    raise TimeoutError('Node 1 bootstrap timed out')
        wanted = tuple(range(1, args.nodes + 1))
        end = time.monotonic() + args.ready_timeout
        while time.monotonic() < end:
            if any(p.poll() is not None for p in processes):
                raise RuntimeError('Peer exited during join; inspect node logs')
            if all(members_in_state(state_dir / f'node{n}.state') == wanted
                   for n in range(1, args.nodes + 1)):
                time.sleep(args.settle)
                print(f'[READY] {args.nodes} committed peers', flush=True)
                return processes, handles
            time.sleep(.3)
        raise TimeoutError('Cluster membership did not commit; inspect node logs')
    except BaseException:
        stop_nodes(processes, handles)
        raise


def stop_nodes(processes, handles):
    for p in processes:
        if p.poll() is None:
            p.terminate()
    for p in processes:
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
    for f in handles:
        f.close()


def drain_broadcasts(ws, stop, errors, rejected, rejected_lock):
    ws.sock.settimeout(1.)
    while not stop.is_set():
        try:
            opcode, payload = ws.recv_frame()
            if opcode == 0x9:
                ws.send_frame(0xA, payload)
            elif opcode == 0x8:
                errors.append('WebSocket closed by peer')
                return
            elif opcode == 0x1:
                try:
                    item = json.loads(payload)
                    if item.get('op') == 'error':
                        message = str(item.get('message', item))
                        # A temporary PUT rejection during membership transition
                        # is expected in a true no-pause churn experiment.
                        # Do NOT terminate all painters for an application error.
                        with rejected_lock:
                            rejected.append((time.time(), message))
                        continue
                except (ValueError, UnicodeDecodeError):
                    pass
        except socket.timeout:
            continue
        except (OSError, ConnectionError, AssertionError) as exc:
            if not stop.is_set():
                errors.append(str(exc))
            return


def file_logger(path, records):
    with path.open('w', encoding='utf-8', buffering=1024 * 1024) as fh:
        fh.write('epoch_seconds,node,ws_port,x,y,color,event\n')
        while True:
            entry = records.get()
            if entry is None:
                return
            fh.write(entry)




def start_node5(args, run_dir, processes, handles):
    """Launch Node 5 via seed Node 1; wait for committed 5-member placement."""
    n = 5
    executable = args.peer.resolve()
    tcp = args.peer_base + (n - 1) * args.port_step
    ws = args.ws_base + (n - 1) * args.port_step
    cmd = [str(executable), '--id', str(n), '--port', str(tcp),
           '--ws-port', str(ws), '--state-file', str(run_dir / 'node5.state'),
           '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2',
           '--strategy', args.strategy,
           '--peers', f'1:{args.host}:{args.peer_base}']
    if args.strategy in ('B', 'C'):
        cmd += ['--min-replicas', '2', '--max-replicas', '4']
    logfile = (run_dir / 'node5.log').open('w', encoding='utf-8')
    handles.append(logfile)
    proc = subprocess.Popen(cmd, stdout=logfile, stderr=subprocess.STDOUT)
    processes.append(proc)
    print(f'[JOIN] Node 5 started: tcp={tcp}, ws={ws}. Waiting for commit...', flush=True)
    deadline = time.monotonic() + args.ready_timeout
    wanted = (1, 2, 3, 4, 5)
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError('Node 5 exited during join; inspect node5.log')
        if all(members_in_state(run_dir / f'node{n}.state') == wanted
               for n in range(1, 6)):
            print('[JOIN] All 5 nodes committed', flush=True)
            return
        time.sleep(.2)
    raise TimeoutError('Node 5 join did not commit; inspect node*.log')


def paint_worker(node_id, args, port, tasks, cancel, done, errors,
                 records, counters, count_lock, schedule, schedule_lock,
                 started, ready, active, paint_enabled, rejected, rejected_lock):
    ws = None
    drain_thread = None
    drain_stop = threading.Event()
    ws_errors = []
    try:
        ws = WebSocketClient(args.host, port)
        ws.connect()
        drain_thread = threading.Thread(target=drain_broadcasts,
                                        args=(ws, drain_stop, ws_errors, rejected, rejected_lock), daemon=True)
        drain_thread.start()
        ready.set()
        started.wait(timeout=args.ready_timeout)
        while not cancel.is_set() and not done.is_set():
            paint_enabled.wait()
            if cancel.is_set() or done.is_set():
                break
            if ws_errors:
                raise RuntimeError(ws_errors[0])
            try:
                x, y, color = tasks.get_nowait()
            except queue.Empty:
                break
            # Globally pace 50%-scale sends across currently active painters.
            with schedule_lock:
                slot = schedule[0]
                schedule[0] += 1
            deadline = schedule[1] + slot * (args.duration / max(1, schedule[2]))
            wait = deadline - time.monotonic()
            if wait > 0:
                # On node4 removal, return the reserved pixel to the shared queue.
                if cancel.wait(wait):
                    tasks.put((x, y, color))
                    break
            if cancel.is_set() or done.is_set():
                tasks.put((x, y, color))
                break
            try:
                ws.send_frame(0x1, json.dumps({'op': 'put', 'x': x, 'y': y, 'color': color},
                                              separators=(',', ':')))
            except Exception:
                tasks.put((x, y, color))
                raise
            with count_lock:
                counters[node_id] += 1
            records.put(f'{time.time():.6f},{node_id},{port},{x},{y},{color},SENT\n')
            if args.console_pixels:
                print(f'[PIXEL] node={node_id} ({x},{y}) {color} SENT')
    except Exception as exc:
        # A disconnected Node 4 is normal after its planned crash.
        if not cancel.is_set() and not done.is_set():
            errors.append(f'Node {node_id}, WS {port}: {exc}')
            done.set()
    finally:
        active[node_id] = False
        drain_stop.set()
        if ws:
            try:
                ws.close()
            except Exception:
                pass
        if drain_thread:
            drain_thread.join(timeout=1)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--image', type=Path, required=True)
    ap.add_argument('--duration', type=float, default=20.0,
                    help='Approximate full-image send window, seconds (default: 20)')
    ap.add_argument('--scale', type=float, default=.5,
                    help='Scale factor, default .5 (128x128 centered)')
    ap.add_argument('--skip-white', action='store_true', default=True)
    ap.add_argument('--no-skip-white', action='store_false', dest='skip_white')
    ap.add_argument('--stride', type=int, default=1)
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--peer', type=Path, default=Path('build/bin/p2place-peer'))
    ap.add_argument('--peer-base', type=int, default=9000)
    ap.add_argument('--ws-base', type=int, default=9001)
    ap.add_argument('--port-step', type=int, default=10)
    ap.add_argument('--strategy', choices=('A', 'B', 'C'), default='A')
    ap.add_argument('--ready-timeout', type=float, default=120.)
    ap.add_argument('--settle', type=float, default=5.)
    ap.add_argument('--console-pixels', action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--stop-after', action='store_true')
    ap.add_argument('--log-dir', type=Path, default=Path('paint_demo_runs'))
    args = ap.parse_args()
    args.nodes = 4  # Always boot four nodes; Node 5 is dynamically admitted.
    if not 0 < args.scale <= 1 or args.duration <= 0 or args.stride < 1:
        ap.error('Require --scale in (0,1], --duration > 0, --stride >= 1')
    rows = load_rows(args.image)
    pixels, side, offset = build_center_scaled_pixels(rows, args.scale, args.skip_white, args.stride)
    random.Random(args.seed).shuffle(pixels)
    total = len(pixels)
    if total < 3:
        ap.error('Too few nonwhite pixels to demonstrate 35%/70% churn')
    join_at = math.ceil(total * .35)
    crash_at = math.ceil(total * .70)
    print(f'[PLAN] {side}x{side} centered at {offset}, {total:,} active pixels, '
          f'{total/args.duration:,.0f} PUT/s total target', flush=True)
    print(f'[CHURN] Start Nodes 1-4; at {join_at:,}/{total:,} SENT (35%) JOIN N5; '
          f'at {crash_at:,}/{total:,} SENT (70%) CRASH N4', flush=True)
    if args.dry_run:
        return 0
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_dir = args.log_dir / stamp
    run_dir.mkdir(parents=True, exist_ok=True)
    processes, handles = [], []
    records = queue.SimpleQueue()
    log_path = run_dir / 'pixels_sent.csv'
    logger = threading.Thread(target=file_logger, args=(log_path, records), daemon=True)
    logger.start()
    events_path = run_dir / 'events.csv'
    events = events_path.open('w', encoding='utf-8')
    events.write('epoch_seconds,elapsed_seconds,pixels_sent,event,details\n')
    events.flush()
    tasks = queue.Queue()
    for p in pixels:
        tasks.put(p)
    count_lock = threading.Lock()
    schedule_lock = threading.Lock()
    counters = {i: 0 for i in range(1, 6)}
    active = {i: False for i in range(1, 6)}
    cancel = {i: threading.Event() for i in range(1, 6)}
    done = threading.Event()
    start_gate = threading.Event()
    paint_enabled = threading.Event()
    ready = {i: threading.Event() for i in range(1, 6)}
    errors = []
    rejected = []  # Application-level PUT rejections, not necessarily correlated to sent pixels.
    rejected_lock = threading.Lock()
    schedule = [0, 0.0, total]
    workers = {}
    event_lock = threading.Lock()
    def event(name, details=''):
        elapsed = max(0.0, time.monotonic() - schedule[1]) if schedule[1] else 0.0
        count = sum(counters.values())
        with event_lock:
            print(f'[EVENT {elapsed:5.1f}s] {name}, progress={count:,}/{total:,} ({count/total:.1%}) {details}', flush=True)
            events.write(f'{time.time():.6f},{elapsed:.3f},{count},{name},{details}\n')
            events.flush()
    def add_worker(n):
        active[n] = True
        port = args.ws_base + (n - 1) * args.port_step
        t = threading.Thread(target=paint_worker,
                             args=(n, args, port, tasks, cancel[n], done, errors,
                                   records, counters, count_lock, schedule, schedule_lock,
                                   start_gate, ready[n], active, paint_enabled,
                                   rejected, rejected_lock), daemon=True)
        workers[n] = t
        t.start()
        if not ready[n].wait(timeout=10):
            raise TimeoutError(f'Node {n} WebSocket client failed to connect')
    try:
        peer_ports = [args.peer_base + i * args.port_step for i in range(4)]
        ws_ports = [args.ws_base + i * args.port_step for i in range(4)]
        processes, handles = launch_nodes(args, run_dir, peer_ports, ws_ports)
        for i in range(1, 5):
            add_worker(i)
        schedule[1] = time.monotonic() + .05
        start_gate.set()
        paint_enabled.set()
        event('PAINT_START', 'four_gateways')
        joined = crashed = False
        churn_threads = []

        def join_node5_in_background():
            try:
                start_node5(args, run_dir, processes, handles)
                event('JOIN_COMMITTED', 'node5')
                # Only attach a painter if work is still ongoing. The cluster
                # admission is never delayed or accelerated by the workload.
                if not done.is_set() and not tasks.empty():
                    add_worker(5)
                    event('NODE5_PAINTING', 'node5_gateway_connected')
            except Exception as exc:
                event('JOIN_FAILED', str(exc).replace(',', ';'))
                errors.append(f'Node 5 join: {exc}')

        def crash_node4_in_background():
            try:
                cancel[4].set()
                proc = processes[3]
                if proc.poll() is None:
                    proc.kill()  # Fail-stop, without graceful leave.
                # Do not block ongoing painters while OS reaps this process.
                proc.wait(timeout=5)
                event('NODE_CRASHED', 'node4_SIGKILL')
            except Exception as exc:
                event('CRASH_FAILED', str(exc).replace(',', ';'))
                errors.append(f'Node 4 crash: {exc}')

        last_report = -1
        previous_rejections = 0
        while True:
            count = sum(counters.values())
            elapsed = time.monotonic() - schedule[1]
            if errors:
                break
            if not joined and count >= join_at:
                joined = True
                event('JOIN_START', 'node5_async_no_pause')
                t = threading.Thread(target=join_node5_in_background,
                                     name='churn-join-node5', daemon=True)
                churn_threads.append(t)
                t.start()
            if not crashed and count >= crash_at:
                crashed = True
                event('CRASH_START', 'node4_async_no_pause')
                t = threading.Thread(target=crash_node4_in_background,
                                     name='churn-crash-node4', daemon=True)
                churn_threads.append(t)
                t.start()
            if int(elapsed) > last_report:
                last_report = int(elapsed)
                with rejected_lock:
                    rejection_count = len(rejected)
                    sample = rejected[-1][1] if rejected else ''
                print(f'[LIVE {elapsed:5.1f}s] {count:,}/{total:,} SENT | '
                      + ' '.join(f'N{i}={counters[i]:,}' for i in range(1, 6))
                      + f' | server_rejections={rejection_count}', flush=True)
                if rejection_count > previous_rejections:
                    print(f'[REJECT] +{rejection_count-previous_rejections} server error message(s), latest={sample}', flush=True)
                previous_rejections = rejection_count
            if tasks.empty() and (sum(counters.values()) == total):
                break
            if all(not t.is_alive() for t in workers.values()) and not tasks.empty():
                errors.append(f'No workers alive but {tasks.qsize()} pixels remain')
                break
            time.sleep(.03)
        # If painting finishes before Node 5 completes admission, the main
        # sender still never pauses; wait only AFTER painting finishes so logs
        # and joining processes are not torn down mid-transition.
        for t in churn_threads:
            if t.name == 'churn-join-node5' and t.is_alive():
                event('PAINT_END_JOIN_PENDING', 'waiting_for_join_after_paint')
                t.join(timeout=args.ready_timeout + 2)
        done.set()
        for t in list(workers.values()):
            t.join(timeout=3)
        for t in churn_threads:
            t.join(timeout=6)
        count = sum(counters.values())
        elapsed = time.monotonic() - schedule[1]
        event('PAINT_FINISHED' if not errors and count == total else 'PAINT_STOPPED')
        print(f'[RESULT] {count:,}/{total:,} pixels SENT in {elapsed:.1f}s; '
              f'average {count/max(.001,elapsed):,.0f} PUT/s', flush=True)
        rejection_path = run_dir / 'server_rejections.csv'
        with rejected_lock:
            rejection_snapshot = list(rejected)
        with rejection_path.open('w', encoding='utf-8') as f:
            f.write('epoch_seconds,message\n')
            for ts, message in rejection_snapshot:
                f.write(f'{ts:.6f},{json.dumps(message)}\n')
        print(f'[LOG] {log_path} | {events_path} | {rejection_path}', flush=True)
        print(f'[SERVER] {len(rejection_snapshot)} application-level rejection(s) observed', flush=True)
        print('[NOTE] SENT is not quorum-ACKed. PUTs rejected during transitions are NOT retried or counted as successful writes.', flush=True)
        print('[NOTE] 20 seconds is a SEND pacing target; join and crash do not pause it.', flush=True)
        if errors:
            for err in errors:
                print('[ERROR] ' + err, file=sys.stderr)
            return 2
        if not args.stop_after:
            print('[CLUSTER] Live nodes 1,2,3,5 remain running; Ctrl+C to stop.', flush=True)
            while all(p.poll() is None for p in (processes[0], processes[1], processes[2], processes[4])):
                time.sleep(.5)
        return 0
    except KeyboardInterrupt:
        print('\n[STOP] Interrupted', flush=True)
        return 130
    finally:
        done.set()
        start_gate.set()
        paint_enabled.set()
        for v in cancel.values():
            v.set()
        for t in workers.values():
            t.join(timeout=2)
        records.put(None)
        logger.join(timeout=5)
        events.close()
        if processes:
            stop_nodes(processes, handles)
            print(f'[NODE LOGS] {run_dir}', flush=True)


if __name__ == '__main__':
    sys.exit(main())
