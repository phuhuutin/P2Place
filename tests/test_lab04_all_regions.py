#!/usr/bin/env python3
"""P2Place Lab 04: write one pixel in every region and read from every node.

Copy into tests/ next to test_websocket_manual.py. Requires running peer
WebSocket listeners; no HTTP server and no third-party Python packages.
WARNING: overwrites 256 canvas pixels (one at each region center).
"""
import argparse
import json
import socket
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_websocket_manual import WebSocketClient


def make_pixels(region_size, side):
    pixels = {}
    for key in range(side * side):
        rx, ry = key % side, key // side
        # Deterministic distinct colors; avoid the default blank pixel color.
        color = f"#{(key * 103 + 31) % 256:02X}{(key * 67 + 83) % 256:02X}{(key * 191 + 41) % 256:02X}"
        x = rx * region_size + region_size // 2
        y = ry * region_size + region_size // 2
        pixels[(x, y)] = (key, color)
    return pixels


def connect(host, port, timeout):
    ws = WebSocketClient(host=host, port=port)
    ws.connect()
    ws.sock.settimeout(timeout)
    return ws


def get_snapshot(host, port, timeout):
    """Read one complete snapshot, never count live broadcasts as the snapshot."""
    ws = connect(host, port, timeout)
    try:
        ws.send_frame(0x1, json.dumps({"op": "get_canvas"}))
        deadline = time.monotonic() + timeout
        begun = False
        received = {}
        while time.monotonic() < deadline:
            ws.sock.settimeout(max(0.1, deadline - time.monotonic()))
            opcode, raw = ws.recv_frame()
            if opcode == 0x9:
                ws.send_frame(0xA, raw)
                continue
            if opcode == 0x8:
                raise ConnectionError("WebSocket closed during snapshot")
            if opcode != 0x1:
                continue
            msg = json.loads(raw.decode("utf-8"))
            op = msg.get("op")
            if op == "error":
                raise RuntimeError(f"WebSocket error: {msg}")
            if op == "canvas_begin":
                begun = True
                received.clear()
            elif op == "canvas_chunk" and begun:
                for p in msg.get("pixels", []):
                    received[(int(p["x"]), int(p["y"]))] = str(p["color"]).upper()
            elif op == "canvas_end" and begun:
                return received
            elif op == "canvas":
                return {(int(p["x"]), int(p["y"])): str(p["color"]).upper()
                        for p in msg.get("pixels", [])}
            # Ignore unrelated live updates and other messages.
        raise TimeoutError("Timed out before canvas_end")
    finally:
        ws.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--ports", type=int, nargs="+", default=[9001, 9011, 9021])
    ap.add_argument("--region-size", type=int, default=16)
    ap.add_argument("--regions-per-side", type=int, default=16)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--write-delay", type=float, default=0.025)
    ap.add_argument("--settle", type=float, default=2.0)
    ap.add_argument("--attempts", type=int, default=3)
    ap.add_argument("--show-failures", type=int, default=20)
    args = ap.parse_args()
    if args.region_size <= 0 or args.regions_per_side <= 0 or args.attempts <= 0:
        ap.error("Region sizes and attempts must be positive")
    if args.timeout <= 0 or args.write_delay < 0 or args.settle < 0:
        ap.error("Timeout must be positive; delays cannot be negative")
    pixels = make_pixels(args.region_size, args.regions_per_side)
    ports = args.ports
    print(f"P2Place all-region synchronization test: {len(pixels)} regions, {len(ports)} nodes", flush=True)
    print("WARNING: overwrites one pixel per region; no peer restarts required", flush=True)
    print(f"WebSocket ports: {ports}", flush=True)

    # A separate fresh connection for each writer; each node originates its own
    # share of writes to exercise remote and local routing paths.
    for writer_idx, port in enumerate(ports):
        items = [(xy, detail) for i, (xy, detail) in enumerate(pixels.items())
                 if i % len(ports) == writer_idx]
        try:
            ws = connect(args.host, port, args.timeout)
            try:
                for (x, y), (key, color) in items:
                    ws.send_frame(0x1, json.dumps(
                        {"op": "put", "x": x, "y": y, "color": color}))
                    if args.write_delay:
                        time.sleep(args.write_delay)
            finally:
                ws.close()
            print(f"  Sent {len(items):3} PUTs via WS {port}", flush=True)
        except Exception as exc:
            print(f"ERROR: write via WS {port}: {type(exc).__name__}: {exc}")
            return 2

    if args.settle:
        print(f"Waiting {args.settle:g}s for forwarding...", flush=True)
        time.sleep(args.settle)

    total = len(pixels) * len(ports)
    failed = 0
    for reader in ports:
        missing = []
        error = None
        for attempt in range(1, args.attempts + 1):
            try:
                actual = get_snapshot(args.host, reader, args.timeout)
                error = None
                missing = [(key, xy, color, actual.get(xy),
                            ports[key % len(ports)])
                           for xy, (key, color) in pixels.items()
                           if actual.get(xy) != color]
                if not missing:
                    break
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                missing = []
            if attempt < args.attempts:
                time.sleep(args.settle)
        if error:
            print(f"FAIL reader WS {reader}: could not complete snapshot: {error}")
            failed += len(pixels)
            continue
        passed = len(pixels) - len(missing)
        failed += len(missing)
        print(f"{'PASS' if not missing else 'FAIL'} reader WS {reader}: "
              f"{passed}/{len(pixels)} correct snapshot pixels "
              f"(attempt {attempt}/{args.attempts})", flush=True)
        for key, (x, y), expected, observed, writer in missing[:args.show_failures]:
            print(f"    region={key:3} xy=({x:3},{y:3}) writer={writer} "
                  f"expected={expected} actual={observed or 'MISSING'}")
        if len(missing) > args.show_failures:
            print(f"    ... {len(missing) - args.show_failures} more failed regions")

    print(f"\nRESULT: {total - failed}/{total} region-reader checks passed")
    if failed:
        print("FAIL: Investigate forwarding/storage, GET snapshot assembly, or ring membership.")
        return 1
    print("PASS: Every region's test pixel was returned by fresh snapshots from every node.")
    print("NOTE: This does not prove disk durability or recovery from a failed owner.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
