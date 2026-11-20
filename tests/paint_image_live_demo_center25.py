#!/usr/bin/env python3
"""Centered 25% painter for P2Place.

- Loads a 256x256 JSON image.
- Scales it to 25% size (64x64) with nearest-neighbor sampling.
- Centers the scaled image on the 256x256 P2Place canvas.
- Skips white pixels by default, so it does not paint the white background.
- Distributes the remaining active pixels evenly across all nodes so every node paints at the same time.

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


def drain_broadcasts(ws, stop, errors):
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
                        errors.append(f"Server error: {item.get('message', item)}")
                        return
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


def paint_worker(idx, args, port, jobs, barrier, records, sent, problems, begin):
    ws = None
    drain_thread = None
    stop = threading.Event()
    errors = []
    try:
        ws = WebSocketClient(args.host, port)
        ws.connect()
        drain_thread = threading.Thread(target=drain_broadcasts,
                                        args=(ws, stop, errors), daemon=True)
        drain_thread.start()
        barrier.wait(timeout=args.ready_timeout)
        interval = args.duration / max(1, len(jobs))
        t0 = begin[0]
        for x, y, color in jobs:
            if errors:
                raise RuntimeError(errors[0])
            payload = json.dumps({'op':'put','x':x,'y':y,'color':color}, separators=(',',':'))
            ws.send_frame(0x1, payload)
            records.put(f'{time.time():.6f},{idx+1},{port},{x},{y},{color},SENT\n')
            sent[idx] += 1
            if args.console_pixels:
                print(f'[PIXEL] node={idx+1} ({x},{y}) {color} SENT', flush=False)
            deadline = t0 + sent[idx] * interval
            pause = deadline - time.monotonic()
            if pause > 0:
                time.sleep(pause)
        if errors:
            raise RuntimeError(errors[0])
    except Exception as exc:
        problems.append(f'Node {idx+1}, WS {port}: {exc}')
        try:
            barrier.abort()
        except Exception:
            pass
    finally:
        stop.set()
        if ws:
            ws.close()
        if drain_thread:
            drain_thread.join(timeout=1)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--image', type=Path, required=True)
    ap.add_argument('--nodes', type=int, default=4)
    ap.add_argument('--duration', type=float, default=20.)
    ap.add_argument('--scale', type=float, default=0.25,
                    help='Scale factor for the source image, default 0.25 (25%%)')
    ap.add_argument('--skip-white', action='store_true', default=True,
                    help='Skip pure white pixels (#FFFFFF); enabled by default')
    ap.add_argument('--no-skip-white', action='store_false', dest='skip_white')
    ap.add_argument('--attach', action='store_true')
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--peer', type=Path, default=Path('build/bin/p2place-peer'))
    ap.add_argument('--peer-base', type=int, default=9000)
    ap.add_argument('--ws-base', type=int, default=9001)
    ap.add_argument('--port-step', type=int, default=10)
    ap.add_argument('--ws-ports', nargs='+', type=int)
    ap.add_argument('--strategy', choices=['A', 'B', 'C'], default='A')
    ap.add_argument('--stride', type=int, default=1)
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--ready-timeout', type=float, default=120.)
    ap.add_argument('--settle', type=float, default=5.)
    ap.add_argument('--console-pixels', action='store_true')
    ap.add_argument('--stop-after', action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--log-dir', type=Path, default=Path('paint_demo_runs'))
    args = ap.parse_args()

    if not 1 <= args.nodes <= 32 or args.duration <= 0 or args.stride < 1 or args.port_step < 1:
        ap.error('--nodes 1..32; --duration > 0; --stride >= 1; --port-step >= 1')
    if not 0 < args.scale <= 1:
        ap.error('--scale must be in (0,1]')

    ws_ports = args.ws_ports or [args.ws_base + i * args.port_step for i in range(args.nodes)]
    peer_ports = [args.peer_base + i * args.port_step for i in range(args.nodes)]
    if len(ws_ports) != args.nodes or (not args.attach and len(set(peer_ports + ws_ports)) != args.nodes * 2):
        ap.error('WebSocket port count must match --nodes; launch ports must be distinct')

    rows = load_rows(args.image)
    pixels, dest, origin = build_center_scaled_pixels(rows, args.scale, args.skip_white, args.stride)
    jobs = split_jobs_evenly(pixels, args.nodes, args.seed)

    print(f'[PLAN] centered {dest}x{dest} image at origin={origin}, active pixels={len(pixels):,}', flush=True)
    print(f'[PLAN] {len(pixels):,} pixels / {args.nodes} nodes / {args.duration:g}s = '
          f'{len(pixels)/args.duration:,.0f} PUT/s target in total', flush=True)
    for i, batch in enumerate(jobs):
        print(f'  Node {i+1} (WS {ws_ports[i]}): {len(batch):,} randomized active pixels', flush=True)
    if args.dry_run:
        return 0

    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_dir = args.log_dir / stamp
    run_dir.mkdir(parents=True, exist_ok=True)
    records = queue.SimpleQueue()
    log_path = run_dir / 'pixels_sent.csv'
    logger = threading.Thread(target=file_logger, args=(log_path, records), daemon=True)
    logger.start()

    processes, files = [], []
    sent = [0] * args.nodes
    problems = []
    try:
        if not args.attach:
            processes, files = launch_nodes(args, run_dir, peer_ports, ws_ports)
        else:
            print('[ATTACH] This will overwrite pixels on the existing canvas.', flush=True)

        barrier = threading.Barrier(args.nodes + 1)
        begin = [0.0]
        threads = [threading.Thread(target=paint_worker,
                                    args=(i, args, ws_ports[i], jobs[i], barrier, records, sent, problems, begin),
                                    daemon=True)
                   for i in range(args.nodes)]
        for t in threads:
            t.start()
        begin[0] = time.monotonic() + 0.1
        try:
            barrier.wait(timeout=args.ready_timeout)
        except threading.BrokenBarrierError:
            raise RuntimeError('At least one gateway failed to connect')

        last = 0
        while any(t.is_alive() for t in threads):
            for t in threads:
                t.join(0.05)
            elapsed = time.monotonic() - begin[0]
            if elapsed >= last + 1.0:
                last = int(elapsed)
                print(f'[LIVE {elapsed:5.1f}s] {sum(sent):,}/{len(pixels):,} SENT | ' +
                      ' '.join(f'N{i+1}={v:,}' for i, v in enumerate(sent)), flush=True)

        elapsed = time.monotonic() - begin[0]
        print(f'[RESULT] {sum(sent):,}/{len(pixels):,} sent in {elapsed:.1f}s '
              f'({sum(sent)/max(.001, elapsed):,.0f} PUT/s)', flush=True)
        print(f'[LOG] One line per sent pixel: {log_path}', flush=True)
        if problems:
            for p in problems:
                print('[ERROR] ' + p, file=sys.stderr)
            return 2
        print('[NOTE] SENT means the client transmitted the PUT, not that quorum ACK succeeded.', flush=True)
        if processes and not args.stop_after:
            print('[CLUSTER] Nodes remain running; Ctrl+C to stop.', flush=True)
            while all(p.poll() is None for p in processes):
                time.sleep(.5)
        return 0
    except KeyboardInterrupt:
        print('\n[STOP] Interrupted', flush=True)
        return 130
    finally:
        records.put(None)
        logger.join(timeout=5)
        if processes:
            stop_nodes(processes, files)
            print(f'[NODE LOGS] {run_dir}', flush=True)


if __name__ == '__main__':
    sys.exit(main())
