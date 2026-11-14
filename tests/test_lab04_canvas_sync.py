#!/usr/bin/env python3
"""Integration test for P2Place Lab 04 cross-node canvas persistence.

Run with all three peers online. Uses the existing Lab 03 WebSocketClient
(tests/test_websocket_manual.py), which implements the P2Place framing protocol.

Writes three diagnostic pixels, one through each WebSocket endpoint, then
requests a fresh, authoritative canvas snapshot from every endpoint.
WARNING: This test modifies three pixels in the active canvas.
"""
import argparse
import json
import socket
import sys
import time
from pathlib import Path

# Works after copying this file to the project's tests/ directory.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_websocket_manual import WebSocketClient

PIXELS = [
    (41, 38, "#E12B34"),    # Key 34 (owner 2 in the supplied 2-peer log)
    (40, 87, "#29C76B"),    # Key 82 (owner 1 in the supplied 2-peer log)
    (216, 203, "#3459EF"),  # Key 205 (owner 1 in the supplied 2-peer log)
]


def open_ws(host, port, timeout):
    ws = WebSocketClient(host=host, port=port)
    ws.connect()
    ws.sock.settimeout(timeout)
    return ws


def receive_json(ws):
    while True:
        opcode, raw = ws.recv_frame()
        if opcode == 0x1:
            return json.loads(raw.decode("utf-8"))
        if opcode == 0x9:  # Ping
            ws.send_frame(0xA, raw)
            continue
        if opcode == 0x8:
            raise ConnectionError("WebSocket closed before snapshot finished")
        raise RuntimeError(f"Unexpected WebSocket opcode {opcode}")


def snapshot(host, port, timeout):
    ws = open_ws(host, port, timeout)
    try:
        ws.send_frame(0x1, json.dumps({"op": "get_canvas"}))
        pixels = {}
        begun = False
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ws.sock.settimeout(max(0.1, deadline - time.monotonic()))
            msg = receive_json(ws)
            op = msg.get("op")
            if op == "canvas_begin":
                begun = True
                pixels.clear()
            elif op in ("canvas_chunk", "canvas"):
                if not begun and op != "canvas":
                    raise AssertionError("canvas_chunk before canvas_begin")
                for px in msg.get("pixels", []):
                    pixels[(int(px["x"]), int(px["y"]))] = str(px["color"]).upper()
                if op == "canvas":
                    return pixels
            elif op == "canvas_end":
                if not begun:
                    raise AssertionError("canvas_end before canvas_begin")
                return pixels
            elif op == "error":
                raise RuntimeError(f"Node {port} returned: {msg}")
            # Ignore asynchronous live pixel notifications while snapshotting.
        raise TimeoutError(f"No canvas_end from Node WS {port} within {timeout}s")
    finally:
        ws.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--ports", nargs=3, type=int, default=[9001, 9011, 9021])
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--settle", type=float, default=1.5,
                        help="Seconds to allow PUT forwarding before fresh GETs")
    args = parser.parse_args()

    print("P2Place Lab 04 cross-node snapshot test")
    print("WARNING: will overwrite pixels:", PIXELS)
    print("WebSocket ports:", args.ports)

    # Baseline: each node responds to a complete snapshot request.
    for port in args.ports:
        try:
            old = snapshot(args.host, port, args.timeout)
            print(f"  WS {port} baseline snapshot: {len(old)} written pixels")
        except Exception as exc:
            print(f"ERROR: cannot retrieve initial canvas from WS {port}: {exc}")
            return 2

    # Send a distinct PUT through each node. Connection remains open briefly
    # to receive possible error messages, but a live broadcast is not a durable
    # write acknowledgment. Fresh GET below is the actual assertion.
    for port, (x, y, color) in zip(args.ports, PIXELS):
        ws = None
        try:
            ws = open_ws(args.host, port, args.timeout)
            ws.send_frame(0x1, json.dumps({
                "op": "put", "x": x, "y": y, "color": color
            }))
            print(f"  PUT via WS {port}: ({x},{y}) {color}")
            time.sleep(0.15)
        except Exception as exc:
            print(f"ERROR: PUT failed on WS {port}: {exc}")
            return 2
        finally:
            if ws:
                ws.close()

    time.sleep(args.settle)

    failures = 0
    for port in args.ports:
        try:
            fresh = snapshot(args.host, port, args.timeout)
        except (Exception,) as exc:
            print(f"FAIL WS {port}: snapshot error: {exc}")
            failures += len(PIXELS)
            continue
        for writer_port, (x, y, color) in zip(args.ports, PIXELS):
            found = fresh.get((x, y))
            ok = found == color.upper()
            status = "PASS" if ok else "FAIL"
            print(f"{status} reader WS {port} | writer WS {writer_port} | "
                  f"({x},{y}) expected={color.upper()} actual={found or 'MISSING'}")
            if not ok:
                failures += 1

    print(f"\\nResult: {9-failures}/9 checks passed")
    if failures:
        print("Pixels disappear or differ in at least one fresh canvas snapshot.")
        print("Check remote PUT storage, forwarded GET/snapshot aggregation, and membership.")
    else:
        print("All diagnostic pixels persist in fresh snapshots across all 3 nodes.")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
