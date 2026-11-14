#!/usr/bin/env python3
"""Write a deterministic 256x256 canvas through three P2Place WebSocket nodes,
then fetch a fresh full snapshot from each node and compare all pixels.

WARNING: Destructively overwrites the specified pixel positions.
Requires tests/test_websocket_manual.py from the P2Place project; stdlib only.
"""
import argparse
import json
import socket
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_websocket_manual import WebSocketClient

SIDE = 256


def color_for(x, y):
    # Stable, nontrivial RGB pattern. Never uses the transparency channel.
    return f"#{(x * 37 + y * 11 + 29) & 255:02X}{(x * 13 + y * 47 + 71) & 255:02X}{(x * 53 + y * 19 + 113) & 255:02X}"


def connect(host, port, timeout):
    ws = WebSocketClient(host, port)
    ws.connect()
    ws.sock.settimeout(timeout)
    return ws


def receive_background(ws, done, errors, stop):
    """Drain WebSocket broadcasts, preventing server->client queue overflow."""
    try:
        ws.sock.settimeout(1.0)
        while not stop.is_set():
            try:
                opcode, payload = ws.recv_frame()
            except socket.timeout:
                continue
            except (ConnectionError, OSError) as e:
                if not stop.is_set():
                    errors.append(f"writer connection closed: {e}")
                break
            if opcode == 0x9:
                ws.send_frame(0xA, payload)
            elif opcode == 0x8:
                errors.append("writer WebSocket closed by peer")
                break
            elif opcode == 0x1:
                try:
                    message = json.loads(payload)
                except (ValueError, UnicodeDecodeError):
                    continue
                if message.get("op") == "error":
                    errors.append(str(message.get("message", message)))
    finally:
        done.set()


def write_pixels(host, port, index, count, pixels, rate, timeout, counters, problems):
    ws = None
    stop = threading.Event()
    done = threading.Event()
    errors = []
    drain = None
    sent = 0
    try:
        ws = connect(host, port, timeout)
        drain = threading.Thread(target=receive_background,
                                 args=(ws, done, errors, stop), daemon=True)
        drain.start()
        start = time.monotonic()
        for k, (x, y) in enumerate(pixels):
            if k % count != index:
                continue
            if errors:
                raise RuntimeError(f"Node WS {port}: {errors[0]}")
            ws.send_frame(0x1, json.dumps({"op": "put", "x": x, "y": y,
                                           "color": color_for(x, y)}, separators=(",", ":")))
            sent += 1
            if sent % 1000 == 0:
                print(f"  WS {port}: {sent} PUTs sent", flush=True)
            if rate > 0:
                due = start + sent / rate
                wait = due - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
        # Allow asynchronous PUT_OK / broadcast / error messages to arrive.
        time.sleep(2.0)
        if errors:
            raise RuntimeError(f"Node WS {port}: {errors[0]}")
    except Exception as exc:
        problems.append(f"WS {port}: {type(exc).__name__}: {exc}")
    finally:
        counters[port] = sent
        stop.set()
        if ws:
            ws.close()
        if drain:
            drain.join(timeout=2.0)


def read_snapshot(host, port, timeout):
    ws = connect(host, port, timeout)
    found = {}
    started = False
    try:
        ws.send_frame(0x1, '{"op":"get_canvas"}')
        # Server requests one region at a time. Timeout is for inactivity,
        # not the entire large snapshot, so slow full canvases are supported.
        while True:
            ws.sock.settimeout(timeout)
            opcode, payload = ws.recv_frame()
            if opcode == 0x9:
                ws.send_frame(0xA, payload)
                continue
            if opcode == 0x8:
                raise ConnectionError("server closed snapshot WebSocket")
            if opcode != 0x1:
                continue
            msg = json.loads(payload)
            op = msg.get("op")
            if op == "error":
                raise RuntimeError(str(msg.get("message", msg)))
            if op == "canvas_begin":
                found.clear()
                started = True
            elif op == "canvas_chunk" and started:
                for p in msg.get("pixels", []):
                    found[(int(p["x"]), int(p["y"]))] = str(p["color"]).upper()
            elif op == "canvas_end" and started:
                return found
    finally:
        ws.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--ports", nargs=3, type=int, default=[9001, 9011, 9021])
    ap.add_argument("--rate", type=float, default=100.0,
                    help="max PUTs/second PER node (default 100; 0=unlimited)")
    ap.add_argument("--stride", type=int, default=1,
                    help="draw every Nth pixel in row-major order; 1=all 65536")
    ap.add_argument("--timeout", type=float, default=30.0,
                    help="inactivity timeout in seconds for a socket/snapshot")
    ap.add_argument("--settle", type=float, default=3.0)
    ap.add_argument("--show-failures", type=int, default=15)
    args = ap.parse_args()
    if args.stride < 1 or args.rate < 0 or args.timeout <= 0 or args.settle < 0:
        ap.error("stride>=1, rate>=0, timeout>0, settle>=0 required")
    pixels = [(k % SIDE, k // SIDE) for k in range(0, SIDE * SIDE, args.stride)]
    print(f"WARNING: overwrites {len(pixels):,} canvas pixels via {args.ports}")
    print("Stop membership transitions; all three peers should be connected and ready.", flush=True)
    counters, problems, threads = {}, [], []
    for idx, port in enumerate(args.ports):
        t = threading.Thread(target=write_pixels, args=(args.host, port, idx, 3,
                            pixels, args.rate, args.timeout, counters, problems))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    print("Writes sent:", counters, flush=True)
    if problems:
        for p in problems:
            print("ERROR:", p)
        return 2
    time.sleep(args.settle)
    total_mismatches = 0
    for port in args.ports:
        try:
            actual = read_snapshot(args.host, port, args.timeout)
        except Exception as exc:
            print(f"FAIL WS {port}: snapshot: {exc}")
            total_mismatches += len(pixels)
            continue
        mismatches = [(x, y, color_for(x, y), actual.get((x, y)))
                      for x, y in pixels if actual.get((x, y)) != color_for(x, y)]
        total_mismatches += len(mismatches)
        print(f"{'PASS' if not mismatches else 'FAIL'} WS {port}: "
              f"{len(pixels)-len(mismatches):,}/{len(pixels):,} matching pixels; "
              f"snapshot contains {len(actual):,} nonempty pixels")
        for x, y, expected, got in mismatches[:args.show_failures]:
            print(f"    ({x},{y}): expected {expected}, got {got or 'MISSING'}")
    print(f"RESULT: {len(pixels)*3-total_mismatches:,}/{len(pixels)*3:,} comparisons passed")
    return 0 if total_mismatches == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
