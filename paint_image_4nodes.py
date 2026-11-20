#!/usr/bin/env python3
"""Paint a 256x256 JSON image through four P2Place browser WebSocket gateways.

Place this script in <P2Place>/tests/ next to test_websocket_manual.py.
Launches a clean four-node cluster by default; --attach uses existing nodes.
Uses only Python standard library and the project's test_websocket_manual.py.
WARNING: Destructively writes up to 65,536 pixels on the connected canvas.
"""
from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

try:
    from test_websocket_manual import WebSocketClient
except ImportError as exc:
    raise SystemExit(
        "Place this script in the P2Place tests/ directory beside "
        "test_websocket_manual.py (the project's WebSocketClient)."
    ) from exc

SIDE = 256


def read_pixels(path: Path, stride: int) -> list[tuple[int, int, str]]:
    with path.open(encoding="utf-8") as f:
        doc = json.load(f)
    if doc.get("width") != SIDE or doc.get("height") != SIDE:
        raise ValueError("Image must be 256 x 256")
    if doc.get("origin", "top-left") != "top-left":
        raise ValueError("Expected top-left origin")
    rows = doc.get("pixels")
    if not isinstance(rows, list) or len(rows) != SIDE:
        raise ValueError("Expected 256 rows in 'pixels'")
    pixels = []
    for y, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != SIDE:
            raise ValueError(f"Row {y} must contain exactly 256 pixels")
        for x, color in enumerate(row):
            if (y * SIDE + x) % stride:
                continue
            if not (isinstance(color, str) and len(color) == 7 and
                    color.startswith('#') and
                    all(c in '0123456789abcdefABCDEF' for c in color[1:])):
                raise ValueError(f"Invalid RGB color at ({x}, {y}): {color!r}")
            pixels.append((x, y, color.upper()))
    return pixels


def committed_members(path: Path):
    try:
        a = path.read_text().split()
        if a[0] not in ("P2PLACE_BALANCED_V1", "P2PLACE_BALANCED_V2"):
            return None
        count = int(a[2])
        return tuple(int(v) for v in a[3:3 + count])
    except (FileNotFoundError, OSError, IndexError, ValueError):
        return None


def start_cluster(args, state_dir: Path):
    binary = args.peer.resolve()
    if not binary.is_file():
        raise FileNotFoundError(f"Missing peer executable: {binary}; build P2Place first")
    state_dir.mkdir(parents=True, exist_ok=True)
    procs = []
    logs = []
    for i in range(1, 5):
        peer_port = args.peer_ports[i - 1]
        ws_port = args.ws_ports[i - 1]
        cmd = [str(binary), '--id', str(i), '--port', str(peer_port),
               '--ws-port', str(ws_port),
               '--state-file', str(state_dir / f'node{i}.state'),
               '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2',
               '--strategy', args.strategy]
        if args.strategy in ('B', 'C'):
            cmd += ['--min-replicas', '2', '--max-replicas', '4']
        if i != 1:
            cmd += ['--peers', f'1:{args.host}:{args.peer_ports[0]}']
        logfile = (state_dir / f'node{i}.log').open('w', encoding='utf-8')
        logs.append(logfile)
        procs.append(subprocess.Popen(cmd, stdout=logfile,
                                      stderr=subprocess.STDOUT))
        print(f"Node {i} started: TCP {peer_port}, WS {ws_port} | log {logfile.name}", flush=True)
        if i == 1:
            # Wait for bootstrap placement before admitting other peers.
            deadline = time.monotonic() + args.ready_timeout
            while time.monotonic() < deadline:
                if procs[0].poll() is not None:
                    raise RuntimeError(f"Node 1 exited; inspect {state_dir/'node1.log'}")
                if committed_members(state_dir / 'node1.state') == (1,):
                    break
                time.sleep(0.2)
            else:
                raise TimeoutError("Node 1 bootstrap did not complete")

    print("Waiting for all nodes to commit (1,2,3,4)...", flush=True)
    deadline = time.monotonic() + args.ready_timeout
    while time.monotonic() < deadline:
        for i, p in enumerate(procs, start=1):
            if p.poll() is not None:
                raise RuntimeError(f"Node {i} exited; inspect {state_dir/f'node{i}.log'}")
        if all(committed_members(state_dir/f'node{i}.state') == (1,2,3,4)
               for i in range(1, 5)):
            # Let connection setup and post-commit backfill settle.
            time.sleep(args.settle)
            return procs, logs
        time.sleep(0.3)
    raise TimeoutError(f"Four-node membership did not commit; inspect logs in {state_dir}")


def stop_cluster(procs, logs):
    for p in procs:
        if p.poll() is None:
            p.terminate()
    for p in procs:
        try:
            p.wait(timeout=4)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
    for f in logs:
        f.close()


def read_broadcasts(ws: WebSocketClient, stop: threading.Event,
                    errors: list[str], received: list[int]):
    """Drain real-time broadcasts, or WS send queues can overflow."""
    ws.sock.settimeout(1.0)
    while not stop.is_set():
        try:
            opcode, payload = ws.recv_frame()
        except socket.timeout:
            continue
        except (OSError, ConnectionError) as e:
            if not stop.is_set():
                errors.append(f"WebSocket disconnected: {e}")
            return
        if opcode == 0x9:
            try:
                ws.send_frame(0xA, payload)
            except (OSError, ConnectionError) as e:
                errors.append(f"PONG send failed: {e}")
                return
        elif opcode == 0x8:
            errors.append('WebSocket closed by P2Place server')
            return
        elif opcode == 0x1:
            try:
                msg = json.loads(payload)
            except (ValueError, UnicodeDecodeError):
                continue
            if msg.get('op') == 'error':
                errors.append(f"Server error: {msg.get('message', msg)}")
                return
            received[0] += 1


def paint_gateway(idx, args, pixels, counters, problems):
    port = args.ws_ports[idx]
    ws = None
    reader = None
    stop = threading.Event()
    errors = []
    received = [0]
    sent = 0
    try:
        ws = WebSocketClient(args.host, port)
        ws.connect()
        ws.sock.settimeout(args.timeout)
        reader = threading.Thread(target=read_broadcasts,
                                  args=(ws, stop, errors, received), daemon=True)
        reader.start()
        start = time.monotonic()
        # Each pixel is sent to just ONE browser gateway. P2Place handles
        # routing/quorum replication; don't duplicate PUTs to all four nodes.
        for k in range(idx, len(pixels), 4):
            if errors:
                raise RuntimeError(errors[0])
            x, y, color = pixels[k]
            ws.send_frame(0x1, json.dumps(
                {'op': 'put', 'x': x, 'y': y, 'color': color},
                separators=(',', ':')))
            sent += 1
            if sent % 1000 == 0:
                print(f"Node {idx+1} (WS {port}): {sent:,} pixels sent", flush=True)
            if args.rate > 0:
                delay = start + sent / args.rate - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
        time.sleep(args.drain)
        if errors:
            raise RuntimeError(errors[0])
    except Exception as exc:
        problems.append(f"Node {idx+1}, WS {port}: {exc}")
    finally:
        counters[idx] = sent
        stop.set()
        if ws:
            ws.close()
        if reader:
            reader.join(timeout=2)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--image', type=Path, required=True,
                    help='256x256 JSON image path')
    ap.add_argument('--attach', action='store_true',
                    help='Use an already-running 4-node cluster; do not launch peers')
    ap.add_argument('--peer', type=Path, default=Path('build/bin/p2place-peer'))
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--peer-ports', nargs=4, type=int, default=[9000,9010,9020,9030])
    ap.add_argument('--ws-ports', nargs=4, type=int, default=[9001,9011,9021,9031])
    ap.add_argument('--strategy', choices=['A','B','C'], default='A')
    ap.add_argument('--rate', type=float, default=40.0,
                    help='Approximate PUTs/second PER node (default 40)')
    ap.add_argument('--stride', type=int, default=1,
                    help='Paint every Nth pixel (use 64 for a fast test, 1 for full image)')
    ap.add_argument('--timeout', type=float, default=30.0)
    ap.add_argument('--ready-timeout', type=float, default=120.0)
    ap.add_argument('--settle', type=float, default=5.0)
    ap.add_argument('--drain', type=float, default=4.0)
    ap.add_argument('--stop-after', action='store_true',
                    help='Stop launched nodes after painting (default: keep canvas running)')
    ap.add_argument('--dry-run', action='store_true',
                    help='Validate JSON and display plan without contacting servers')
    args = ap.parse_args()
    if args.rate < 0 or args.stride < 1 or args.timeout <= 0:
        ap.error('--rate >= 0, --stride >= 1, --timeout > 0 required')
    if len(set(args.peer_ports + args.ws_ports)) != 8:
        ap.error('All 8 ports must be distinct')
    pixels = read_pixels(args.image, args.stride)
    print(f"Image: {args.image} | {len(pixels):,}/65,536 pixels", flush=True)
    for idx, port in enumerate(args.ws_ports):
        print(f"Node {idx+1}: {len(range(idx,len(pixels),4)):,} PUTs via ws://{args.host}:{port}")
    if args.dry_run:
        return 0
    if args.attach:
        print('ATTACH MODE: modifying existing cluster canvas!', flush=True)
    else:
        print('START MODE: creating a 4-node cluster; existing ports must be free.', flush=True)
    procs, logs = [], []
    state_dir = Path('paint4_runs') / datetime.now().strftime('%Y%m%d_%H%M%S')
    try:
        if not args.attach:
            # Independent state files; do not alter production state/*.state.
            procs, logs = start_cluster(args, state_dir)
        counters = [0,0,0,0]
        problems = []
        writers = [threading.Thread(target=paint_gateway,
                                    args=(i,args,pixels,counters,problems),daemon=True)
                   for i in range(4)]
        print('Painting image across four WebSocket gateways...', flush=True)
        t0 = time.monotonic()
        for writer in writers: writer.start()
        for writer in writers: writer.join()
        print(f"Done sending: {sum(counters):,}/{len(pixels):,} PUTs in {time.monotonic()-t0:.1f}s")
        if problems:
            for p in problems: print('ERROR:', p, file=sys.stderr)
            print('Some writes may not have succeeded; inspect node logs.', file=sys.stderr)
            return 2
        print('No WebSocket errors observed. This does NOT verify every write ACK.')
        print('View the canvas in a browser connected to a node WS port.')
        if not args.attach and not args.stop_after:
            print('Nodes remain running. Press Ctrl+C to stop all four.', flush=True)
            while all(p.poll() is None for p in procs):
                time.sleep(1)
        return 0
    except KeyboardInterrupt:
        print('\nStopping...', flush=True)
        return 130
    finally:
        if procs:
            stop_cluster(procs, logs)
            print(f'Node logs and state files: {state_dir}', flush=True)

if __name__ == '__main__':
    sys.exit(main())
