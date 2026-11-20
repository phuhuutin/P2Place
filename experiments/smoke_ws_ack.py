#!/usr/bin/env python3
"""Live smoke test for the browser PUT acknowledgement path.

Validates, against a real 2-node cluster (the minimum cluster that can satisfy
W=2), that:

  1. a successful PUT produces one client-visible ACK (the quorum-gated
     ``{"op":"pixel"}`` echo used by the web UI), and
  2. a PUT that cannot reach its write quorum produces NO client ACK.

The second case is a genuine quorum failure: node 2 is killed and the next PUT
still targets both original members until failure detection reconfigures
placement.  This is the exact distinction the targeted B/C experiment relies
on; the same ``ws_client.put_pixel`` implementation is used unchanged there.

Usage:
    python3 experiments/smoke_ws_ack.py [--peer build/bin/p2place-peer]
                                        [--keep] [--ack-timeout 4.0]
Exit: 0 = PASS, 1 = FAIL.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ws_client import put_pixel  # noqa: E402


def free_ports(count: int) -> list[tuple[int, int]]:
    """Allocate count (peer_port, ws_port) pairs while holding every bind."""
    socks = []
    pairs = []
    try:
        for _ in range(count):
            pair = []
            for _ in range(2):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                socks.append(sock)
                pair.append(sock.getsockname()[1])
            pairs.append((pair[0], pair[1]))
        return pairs
    finally:
        for sock in socks:
            sock.close()


def wait_ws(port: int, proc: subprocess.Popen, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"peer exited early with {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"ws port {port} never opened")


def kill_group(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--peer", type=Path, default=Path("build/bin/p2place-peer"))
    parser.add_argument("--ack-timeout", type=float, default=4.0)
    parser.add_argument("--keep", action="store_true", help="keep the temporary run directory")
    args = parser.parse_args()
    args.peer = args.peer.resolve()
    if not args.peer.is_file():
        print(f"FAIL: peer binary missing: {args.peer}")
        return 1

    root = Path(tempfile.mkdtemp(prefix="p2place_ws_smoke_"))
    ports = free_ports(2)
    procs: list[subprocess.Popen] = []
    logs = []
    prefix = [shutil.which("stdbuf"), "-oL", "-eL"] if shutil.which("stdbuf") else []

    def launch(node_id: int) -> subprocess.Popen:
        node_dir = root / f"node{node_id}"
        node_dir.mkdir(parents=True)
        cmd = [
            *prefix, str(args.peer),
            "--id", str(node_id),
            "--port", str(ports[node_id - 1][0]),
            "--ws-port", str(ports[node_id - 1][1]),
            "--replicas", "3", "--read-quorum", "2", "--write-quorum", "2",
            "--strategy", "B", "--min-replicas", "2", "--max-replicas", "4",
            "--metrics-dir", str(node_dir),
        ]
        if node_id != 1:
            cmd += ["--peers", f"1:127.0.0.1:{ports[0][0]}"]
        handle = (node_dir / "node.log").open("a", buffering=1)
        logs.append(handle)
        proc = subprocess.Popen(
            cmd, cwd=node_dir, stdout=handle, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        procs.append(proc)
        return proc

    failures = []

    def check(name: str, condition: bool, detail: str = "") -> None:
        if condition:
            print(f"PASS: {name}")
        else:
            failures.append(name)
            print(f"FAIL: {name}" + (f" — {detail}" if detail else ""))

    try:
        p1 = launch(1)
        wait_ws(ports[0][1], p1)
        time.sleep(0.4)
        p2 = launch(2)
        wait_ws(ports[1][1], p2)

        log1 = root / "node1" / "node.log"
        pattern = re.compile(r"\[placement\] committed generation=(\d+)[^\n]* owners=(\d+)")
        deadline = time.monotonic() + 60
        gen = 0
        while time.monotonic() < deadline:
            text = log1.read_text(errors="replace")
            matches = list(pattern.finditer(text))
            if matches and int(matches[-1].group(2)) == 2:
                gen = int(matches[-1].group(1))
                break
            time.sleep(0.2)
        check("two-node cluster committed", gen >= 2, f"last generation={gen}")

        # A successful W=2 PUT through node 1.  Any region targets both members.
        first = put_pixel(ports[0][1], 5, 7, "#12AB34", timeout=args.ack_timeout)
        print("      first PUT raw:", json.dumps(first["messages"]))
        check("successful PUT returns client ACK", first["result"] == "ack", json.dumps(first))
        check("ACK is the matching pixel echo",
              first["result"] == "ack"
              and first["response"].get("x") == 5
              and first["response"].get("y") == 7
              and str(first["response"].get("color", "")).upper() == "#12AB34",
              json.dumps(first.get("response")))
        text = log1.read_text(errors="replace")
        check("server logged a 2/2 PUT quorum",
              bool(re.search(r"\[quorum-done\] node=1 op=PUT request=\d+ ok=1 ack=2/2", text)))

        # An application error is a NAK, never an ACK.
        bad = put_pixel(ports[0][1], 300, -1, "#FFFFFF", timeout=min(2.0, args.ack_timeout))
        print("      invalid PUT raw:", json.dumps(bad["messages"]))
        check("forced invalid PUT does not ACK", bad["result"] != "ack", json.dumps(bad))
        check("forced invalid PUT is a NAK", bad["result"] == "nak", json.dumps(bad))

        # Genuine quorum failure: kill node 2, then PUT before failure detection.
        kill_group(p2)
        time.sleep(0.2)
        failed = put_pixel(ports[0][1], 9, 11, "#A1B2C3", timeout=args.ack_timeout)
        print("      post-kill PUT raw:", json.dumps(failed["messages"]))
        check("quorum-failing PUT never returns ACK", failed["result"] != "ack", json.dumps(failed))
        text = log1.read_text(errors="replace")
        check("server-side PUT quorum failed (corroboration, not the ACK)",
              bool(re.search(r"\[quorum-done\] node=1 op=PUT request=\d+ ok=0", text)),
              "no quorum-done ok=0 line")
        if failed["result"] == "timeout":
            print("      note: client deadline expired before the server's 3 s quorum deadline")
        else:
            print(f"      client saw {failed['result']}: {failed['reason']}")

        print("-" * 60)
        if failures:
            print(f"RESULT: FAIL ({len(failures)} check(s))")
            print(f"artifacts kept at {root}")
            return 1
        print("RESULT: PASS — one ACK on success, no ACK on quorum failure")
        return 0
    except Exception as exc:  # noqa: BLE001 - smoke test reports the failure
        print(f"RESULT: FAIL — {type(exc).__name__}: {exc}")
        print(f"artifacts kept at {root}")
        return 1
    finally:
        for proc in procs:
            kill_group(proc)
        for handle in logs:
            try:
                handle.close()
            except OSError:
                pass
        if not args.keep and not failures and root.exists():
            shutil.rmtree(root, ignore_errors=True)
        elif root.exists():
            print(f"artifacts: {root}")


if __name__ == "__main__":
    sys.exit(main())
