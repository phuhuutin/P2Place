#!/usr/bin/env python3
"""Live integration for the bounded browser PUT buffer.

Modes:
  join45   four committed Strategy-A peers (R=2/W=2), continuous browser PUT
           traffic, then admit Node 5 and verify queued writes replay after
           RELEASE without loss or false success.
  crash54  five committed peers, continuous painting, SIGKILL Node 4, then
           verify authority-led removal and queue drain on surviving members.

The script times freeze/RELEASE/drain by tailing peer logs, then reconciles
put-buffer accounting and verifies a fresh canvas snapshot against the exact
last color each painter sent. Rejections received by browsers are counted
separately. Stdlib only; uses tests/test_websocket_manual.py's client.
"""
import argparse
import json
import os
import pathlib
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from test_websocket_manual import WebSocketClient

ENQUEUE = re.compile(r"\[put-buffer\] node=(\d+) event=enqueue depth=(\d+) x=(\d+) y=(\d+)")
REPLAY = re.compile(r"\[put-buffer\] node=(\d+) event=replay depth=(\d+)")
SUBMITTED = re.compile(r"\[put-buffer\] node=(\d+) event=submitted")
QUORUM_OK = re.compile(r"\[put-buffer\] node=(\d+) event=quorum_ok")
QUORUM_FAILED = re.compile(r"\[put-buffer\] node=(\d+) event=quorum_failed")
TICK = re.compile(r"\[put-buffer\] node=(\d+) event=tick submitted=(\d+) discarded=(\d+) depth=(\d+) oldest_age_ms=(-?\d+)")
DISCARD = re.compile(r"\[put-buffer\] node=(\d+) event=discard reason=(\w+) count=(\d+)")


def free_ports(count):
    sockets = []
    ports = {}
    try:
        for node in range(1, count + 1):
            pair = []
            for _ in range(2):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                sockets.append(sock)
                pair.append(sock.getsockname()[1])
            ports[node] = pair
        return ports
    finally:
        for sock in sockets:
            sock.close()


def manifest(path):
    parts = path.read_text().split()
    assert parts[0] in ("P2PLACE_BALANCED_V1", "P2PLACE_BALANCED_V2"), parts[0]
    generation, count = int(parts[1]), int(parts[2])
    members = list(map(int, parts[3:3 + count]))
    offset = 3 + count
    if parts[0] == "P2PLACE_BALANCED_V2":
        offset += count
    owners = list(map(int, parts[offset:offset + 256]))
    assert len(owners) == 256
    return generation, members, owners


def wait_for(fn, label, timeout=60, interval=0.1):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = fn()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError(f"Timed out waiting for {label}")


class LogTail(threading.Thread):
    """Timestamped live tail of one peer log."""

    def __init__(self, path, stop):
        super().__init__(daemon=True)
        self.path = path
        self.stop_event = stop
        self.lines = []
        self.lock = threading.Lock()
        self.ready = threading.Event()

    def run(self):
        end = time.monotonic() + 20
        while not self.path.exists() and time.monotonic() < end:
            time.sleep(0.02)
        if not self.path.exists():
            self.ready.set()
            return
        with open(self.path, "r", errors="replace") as handle:
            self.ready.set()
            while not self.stop_event.is_set():
                line = handle.readline()
                if line:
                    with self.lock:
                        self.lines.append((time.monotonic(), line.rstrip("\n")))
                else:
                    time.sleep(0.02)
            # Final drain after stop.
            for line in handle:
                with self.lock:
                    self.lines.append((time.monotonic(), line.rstrip("\n")))

    def snapshot(self):
        with self.lock:
            return list(self.lines)


class SharedPaint:
    def __init__(self):
        self.lock = threading.Lock()
        self.last_color = {}
        self.last_color_by_node = defaultdict(dict)
        self.sent = Counter()
        self.errors = []
        self.closed = []

    def record(self, node, x, y, color):
        with self.lock:
            self.last_color[(x, y)] = color
            self.last_color_by_node[node][(x, y)] = color
            self.sent[node] += 1

    def error(self, node, text):
        with self.lock:
            self.errors.append((node, text))

    def closed_node(self, node):
        with self.lock:
            self.closed.append(node)


class Painter(threading.Thread):
    """One persistent browser session per peer, fixed-rate PUT stream."""

    def __init__(self, node, port, sequence_start, sequence_step, rate,
                 shared, stop_event, close_event):
        super().__init__(daemon=True)
        self.node = node
        self.port = port
        self.sequence = sequence_start
        self.step = sequence_step
        self.rate = rate
        self.shared = shared
        self.stop_event = stop_event
        self.close_event = close_event
        self.ws = None
        self.reader_stop = threading.Event()
        self.reader = None
        self.failure = None

    def _read_loop(self):
        try:
            self.ws.sock.settimeout(0.5)
            while not self.reader_stop.is_set():
                try:
                    opcode, payload = self.ws.recv_frame()
                except socket.timeout:
                    continue
                except (ConnectionError, OSError) as exc:
                    if not self.reader_stop.is_set():
                        self.shared.closed_node(self.node)
                        self.failure = f"{type(exc).__name__}:{exc}"
                    return
                if opcode == 0x9:
                    self.ws.send_frame(0xA, payload)
                elif opcode == 0x8:
                    self.shared.closed_node(self.node)
                    return
                elif opcode == 0x1:
                    try:
                        message = json.loads(payload)
                    except (ValueError, UnicodeDecodeError):
                        continue
                    if isinstance(message, dict) and message.get("op") == "error":
                        self.shared.error(self.node,
                                          str(message.get("message", message)))
        except Exception as exc:  # never silence reader failures
            self.failure = f"{type(exc).__name__}:{exc}"

    def run(self):
        try:
            self.ws = WebSocketClient("127.0.0.1", self.port)
            self.ws.connect()
            self.reader = threading.Thread(target=self._read_loop, daemon=True)
            self.reader.start()
            next_due = time.monotonic()
            while not self.stop_event.is_set():
                x = self.sequence % 256
                y = (self.sequence // 256) % 256
                color = "#{:02X}{:02X}{:02X}".format(
                    (self.sequence * 37 + self.node * 53) & 0xFF,
                    (self.sequence * 11 + self.node * 97) & 0xFF,
                    (self.sequence * 19 + self.node * 29) & 0xFF)
                self.ws.send_frame(
                    0x1,
                    json.dumps({"op": "put", "x": x, "y": y, "color": color},
                               separators=(",", ":")))
                self.shared.record(self.node, x, y, color)
                self.sequence += self.step
                next_due += 1.0 / self.rate
                delay = next_due - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                elif delay < -0.5:
                    next_due = time.monotonic()
            # Keep the browser SESSION alive across replay: closing it would
            # (correctly) discard its queued PUTs instead of draining them.
            while not self.close_event.is_set():
                time.sleep(0.05)
        except Exception as exc:
            self.failure = f"{type(exc).__name__}:{exc}"
            self.shared.closed_node(self.node)
        finally:
            self.reader_stop.set()
            if self.reader:
                self.reader.join(timeout=1)
            if self.ws:
                self.ws.close()


def prefill(ports, count=256, rate=250, nodes=None):
    """One pixel in every region so a join/removal has real handoff work."""
    clients = {}
    nodes = list(nodes) if nodes is not None else list(range(1, len(ports) + 1))
    try:
        for node in nodes:
            ws = WebSocketClient("127.0.0.1", ports[node][1])
            ws.connect()
            clients[node] = ws
        expected = {}
        for key in range(count):
            x = (key % 16) * 16
            y = (key // 16) * 16
            color = "#{:02X}{:02X}{:02X}".format(
                (key * 7 + 3) & 0xFF, (key * 13 + 11) & 0xFF,
                (key * 31 + 17) & 0xFF)
            node = nodes[key % len(nodes)]
            clients[node].send_frame(
                0x1, json.dumps({"op": "put", "x": x, "y": y, "color": color},
                                separators=(",", ":")))
            expected[(x, y)] = color
            time.sleep(1.0 / rate)
        time.sleep(1.5)  # let every prefill quorum settle
        return expected
    finally:
        for ws in clients.values():
            ws.close()


def read_snapshot(port, timeout=20):
    ws = WebSocketClient("127.0.0.1", port)
    ws.connect()
    found = {}
    started = False
    try:
        ws.send_frame(0x1, '{"op":"get_canvas"}')
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            ws.sock.settimeout(timeout)
            opcode, payload = ws.recv_frame()
            if opcode == 0x9:
                ws.send_frame(0xA, payload)
                continue
            if opcode == 0x8:
                raise ConnectionError("snapshot socket closed")
            if opcode != 0x1:
                continue
            message = json.loads(payload)
            op = message.get("op")
            if op == "error":
                raise RuntimeError(str(message.get("message", message)))
            if op == "canvas_begin":
                found.clear()
                started = True
            elif op == "canvas_chunk" and started:
                for pixel in message.get("pixels", []):
                    found[(int(pixel["x"]), int(pixel["y"]))] = \
                        str(pixel["color"]).upper()
            elif op == "canvas_end" and started:
                return found
        raise TimeoutError("snapshot deadline")
    finally:
        ws.close()


class Cluster:
    def __init__(self, exe, root, node_count, suspect=2, unavailable=6,
                 strategy="A"):
        self.exe = exe
        self.root = root
        self.ports = free_ports(node_count)
        self.procs = {}
        self.logs = {}
        self.stop_tail = threading.Event()
        self.tails = {}
        self.node_count = node_count
        self.suspect = suspect
        self.unavailable = unavailable
        self.strategy = strategy

    def launch(self, node, wait=True):
        log_path = self.root / f"node{node}.log"
        log = open(log_path, "a", buffering=1)
        cmd = [str(self.exe), "--id", str(node),
               "--port", str(self.ports[node][0]),
               "--ws-port", str(self.ports[node][1]),
               "--state-file", str(self.root / f"node{node}.state"),
               "--replicas", "3", "--read-quorum", "2",
               "--write-quorum", "2", "--strategy", self.strategy,
               "--suspect-after", str(self.suspect),
               "--unavailable-after", str(self.unavailable),
               "--recovery-stabilization", "1"]
        if node != 1:
            cmd += ["--peers", f"1:127.0.0.1:{self.ports[1][0]}"]
        if shutil.which("stdbuf"):
            cmd = ["stdbuf", "-oL", "-eL"] + cmd
        proc = subprocess.Popen(cmd, cwd=self.root, stdout=log,
                                stderr=subprocess.STDOUT,
                                start_new_session=True)
        self.procs[node] = proc
        self.logs[node] = log
        tail = LogTail(log_path, self.stop_tail)
        tail.start()
        self.tails[node] = tail
        if wait:
            self.wait_port(node)

    def wait_port(self, node, timeout=25):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            proc = self.procs[node]
            if proc.poll() is not None:
                raise RuntimeError(f"node {node} exited: {proc.returncode}")
            try:
                with socket.create_connection(
                        ("127.0.0.1", self.ports[node][1]), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError(f"node {node} WebSocket never opened")

    def state(self, node):
        path = self.root / f"node{node}.state"
        return manifest(path) if path.exists() else None

    def lines(self, node):
        return self.tails[node].snapshot()

    def wait_generation(self, nodes, generation, members, timeout=90):
        def ready():
            for node in nodes:
                state = self.state(node)
                if state is None or state[0] < generation:
                    return False
                if state[0] == generation and state[1] != members:
                    return False
            return True
        wait_for(ready, f"generation {generation} members {members}",
                 timeout=timeout)

    def stop(self):
        self.stop_tail.set()
        for proc in self.procs.values():
            if proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
        for proc in self.procs.values():
            try:
                proc.wait(timeout=4)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        for log in self.logs.values():
            log.close()
        for tail in self.tails.values():
            tail.join(timeout=2)


def parse_lines(lines):
    events = []
    for timestamp, line in lines:
        for regex, kind in ((ENQUEUE, "enqueue"), (REPLAY, "replay"),
                            (SUBMITTED, "submitted"),
                            (QUORUM_OK, "quorum_ok"),
                            (QUORUM_FAILED, "quorum_failed"),
                            (TICK, "tick"), (DISCARD, "discard")):
            match = regex.search(line)
            if match:
                events.append((timestamp, kind, line, match))
                break
    return events


def first_time(lines, needle):
    for timestamp, line in lines:
        if needle in line:
            return timestamp
    return None


def buffered_coordinates(lines):
    coordinates = set()
    for _, line in lines:
        match = ENQUEUE.search(line)
        if match:
            coordinates.add((int(match.group(3)), int(match.group(4))))
    return coordinates


def transition_start(lines, generation):
    for timestamp, line in lines:
        if f"gen={generation}" in line and (
                "[transition-start]" in line or
                "[prepare-received]" in line):
            return timestamp
    return None


def transition_release(lines, generation, start):
    for timestamp, line in lines:
        if start is not None and timestamp < start:
            continue
        if (f"RELEASE generation {generation}" in line or
                ("[release-received]" in line and
                 f"gen={generation} " in line)):
            return timestamp
    return None


def summarize_node(node, lines, generation):
    start = transition_start(lines, generation)
    scoped = [(ts, line) for ts, line in lines
              if start is None or ts >= start]
    events = parse_lines(scoped)
    counts = Counter(kind for _, kind, _, _ in events)
    enqueue_times = [ts for ts, kind, _, _ in events if kind == "enqueue"]
    submitted_times = [ts for ts, kind, _, _ in events if kind == "submitted"]
    tick_zero = [ts for ts, kind, line, _ in events
                 if kind == "tick" and " depth=0 " in line]
    release = transition_release(scoped, generation, start)
    after_release = [ts for ts in tick_zero if release is None or ts >= release]
    return {
        "node": node,
        "transition_found": start is not None,
        "enqueued": counts["enqueue"],
        "replayed": counts["replay"],
        "submitted": counts["submitted"],
        "quorum_ok": counts["quorum_ok"],
        "quorum_failed": counts["quorum_failed"],
        "discarded_disconnected": sum(
            int(m.group(3)) for _, kind, _, m in events
            if kind == "discard" and m.group(2) == "disconnected"),
        "discarded_membership": sum(
            int(m.group(3)) for _, kind, _, m in events
            if kind == "discard" and m.group(2) == "membership_reset"),
        "first_enqueue": enqueue_times[0] if enqueue_times else None,
        "last_enqueue": enqueue_times[-1] if enqueue_times else None,
        "first_submitted": submitted_times[0] if submitted_times else None,
        "last_submitted": submitted_times[-1] if submitted_times else None,
        "drain_complete": after_release[-1] if after_release else None,
        "freeze": start,
        "release": release,
        "prepare_received": first_time(scoped, "[prepare-received]"),
        "commit_ack": first_time(scoped, "[commit-ack]"),
        "release_received": first_time(scoped, "[release-received]"),
        "removed": first_time(scoped, "[membership] REMOVED"),
        "data_loss": first_time(scoped, "[data-loss]"),
        "quorum_put_ok": sum(1 for _, line in scoped
                             if "[quorum] op=PUT ok=1" in line),
        "pixel_event_accepted": sum(
            1 for _, line in scoped
            if "[pixel-event]" in line and "accepted=W quorum" in line),
        "ws_put_quorum_success": sum(
            1 for _, line in scoped
            if "[ws-put]" in line and "quorum=success" in line),
    }


def check_accounting(summary, label):
    problems = []
    for node, info in summary.items():
        if info["submitted"] != info["enqueued"] - info["discarded_disconnected"] - \
                info["discarded_membership"]:
            problems.append(
                f"{label} node {node}: enqueued={info['enqueued']} "
                f"submitted={info['submitted']} "
                f"discarded={info['discarded_disconnected'] + info['discarded_membership']}")
        if info["quorum_ok"] + info["quorum_failed"] != info["submitted"]:
            problems.append(
                f"{label} node {node}: submitted={info['submitted']} "
                f"ok={info['quorum_ok']} failed={info['quorum_failed']}")
    return problems


def verify_canvas(ports, expected, nodes, sample_step=37,
                  extra_coordinates=()):
    mismatches = []
    missing = []
    coordinates = sorted(expected)
    sample = set(coordinates[::sample_step] + coordinates[-40:])
    sample.update(coordinate for coordinate in extra_coordinates
                  if coordinate in expected)
    sample = sorted(sample)
    for node in nodes:
        snapshot = read_snapshot(ports[node][1])
        for coordinate in sample:
            want = expected[coordinate].upper()
            got = snapshot.get(coordinate)
            if got is None:
                missing.append((node, coordinate))
            elif got != want:
                mismatches.append((node, coordinate, want, got))
    return sample, mismatches, missing


def run_join(args):
    root = args.out
    root.mkdir(parents=True, exist_ok=True)
    cluster = Cluster(args.peer, root, 5, args.suspect, args.unavailable)
    shared = SharedPaint()
    painters = []
    stop = threading.Event()
    close_sessions = threading.Event()
    try:
        for node in range(1, 5):
            cluster.launch(node)
            cluster.wait_generation(list(range(1, node + 1)), node,
                                    list(range(1, node + 1)), timeout=40)
        print("PASS join45: four peers committed", flush=True)
        prefill_colors = prefill(cluster.ports, count=256, nodes=(1, 2, 3, 4))
        expected = dict(prefill_colors)

        for index, node in enumerate((1, 2, 3, 4)):
            painter = Painter(node, cluster.ports[node][1], index, 4,
                              args.rate, shared, stop, close_sessions)
            painter.start()
            painters.append(painter)
        time.sleep(1.0)
        print(f"join45: painting at ~{args.rate}/node before join",
              flush=True)

        cluster.launch(5, wait=True)  # wait_port only; admission follows
        join_started = time.monotonic()
        cluster.wait_generation([1, 2, 3, 4, 5], 5, [1, 2, 3, 4, 5],
                                timeout=120)
        join_committed = time.monotonic()
        print(f"PASS join45: Node 5 admitted generation 5 "
              f"({join_committed - join_started:.2f}s)", flush=True)

        # Stop sending once every survivor has processed RELEASE; queued
        # entries then replay without newer direct writes to the same pixels.
        def released():
            for node in (1, 2, 3, 4):
                lines = cluster.lines(node)
                start = transition_start(lines, 5)
                if start is None or \
                        transition_release(lines, 5, start) is None:
                    return False
            return True
        wait_for(released, "generation-5 RELEASE on every committed node",
                 timeout=90)
        release_time = time.monotonic()
        stop.set()  # stop issuing new PUTs; keep sessions open for replay
        print(f"join45: painters stopped sending at release "
              f"({release_time - join_started:.2f}s after join launch)",
              flush=True)

        # Queue drain completion: every node reports a depth=0 tick after its
        # release, with no later enqueue.
        def drained():
            for node in (1, 2, 3, 4):
                lines = cluster.lines(node)
                start = transition_start(lines, 5)
                release_at = transition_release(lines, 5, start)
                zero_at = None
                for timestamp, line in lines:
                    if release_at is not None and timestamp < release_at:
                        continue
                    if ("[put-buffer] node=" in line and "event=tick" in line and
                            " depth=0 " in line):
                        zero_at = timestamp
                if release_at is None or zero_at is None:
                    return False
                # Any enqueue after the zero tick means the queue refilled.
                if any(timestamp > zero_at and "event=enqueue" in line
                       for timestamp, line in lines):
                    return False
            return True
        wait_for(drained, "queue drain on all committed nodes", timeout=60)
        drain_time = time.monotonic()
        close_sessions.set()
        for painter in painters:
            painter.join(timeout=5)
        time.sleep(1.5)  # allow the final quorum callbacks/broadcasts
        print(f"PASS join45: queues drained ({drain_time - release_time:.2f}s "
              f"after release)", flush=True)

        summary = {}
        for node in (1, 2, 3, 4, 5):
            info = summarize_node(node, cluster.lines(node), 5)
            summary[node] = info
        accounting = check_accounting(
            {n: summary[n] for n in (1, 2, 3, 4)}, "join45")

        with shared.lock:
            errors = list(shared.errors)
            last_color = dict(shared.last_color)
            sent = dict(shared.sent)
        expected_all = dict(expected)
        expected_all.update(last_color)
        buffered = set()
        for node in (1, 2, 3, 4):
            buffered |= buffered_coordinates(cluster.lines(node))
        buffered = {c for c in buffered if c in expected_all}
        sample, mismatches, missing = verify_canvas(
            cluster.ports, expected_all, (1, 2, 3, 4, 5),
            extra_coordinates=buffered)

        result = {
            "mode": "join45",
            "nodes": summary,
            "accounting_problems": accounting,
            "browser_sent": sent,
            "browser_errors": errors,
            "browser_closed_nodes": shared.closed,
            "buffered_unique_coordinates": len(buffered),
            "canvas_sample": len(sample),
            "canvas_mismatches": mismatches[:20],
            "canvas_mismatch_count": len(mismatches),
            "canvas_missing_count": len(missing),
            "state": {n: cluster.state(n)[:2] for n in (1, 2, 3, 4, 5)},
            "timing": {
                "join_started": join_started,
                "join_committed": join_committed,
                "release": release_time,
                "drained": drain_time,
            },
        }
        (root / "result.json").write_text(json.dumps(result, indent=2))
        print(json.dumps({k: v for k, v in result.items()
                          if k not in ("nodes", "canvas_mismatches")},
                         indent=2), flush=True)
        for node, info in summary.items():
            print(f"node {node}: enqueued={info['enqueued']} "
                  f"replayed={info['replayed']} submitted={info['submitted']} "
                  f"ok={info['quorum_ok']} failed={info['quorum_failed']} "
                  f"disc_disc={info['discarded_disconnected']} "
                  f"disc_mem={info['discarded_membership']}", flush=True)
        assert not accounting, accounting
        assert not mismatches, mismatches[:5]
        assert not any("placement transition" in text or "buffer full" in text
                       for _, text in errors), errors[:5]
        assert len(errors) == 0, errors[:5]
        assert len(mismatches) == 0 and len(missing) == 0
        # At least one committed node must actually have buffered writes.
        assert sum(info["enqueued"] for n, info in summary.items()
                   if n != 5) > 0, "join45 exercised no buffering"
        assert buffered, "join45 did not verify any buffered coordinate"
        print("PASS join45: bounded PUT buffer integrated with a live 4->5 join",
              flush=True)
    finally:
        close_sessions.set()
        for painter in painters:
            painter.join(timeout=2)
        cluster.stop()


def run_crash(args):
    root = args.out
    root.mkdir(parents=True, exist_ok=True)
    cluster = Cluster(args.peer, root, 5, args.suspect, args.unavailable)
    shared = SharedPaint()
    painters = {}
    stop = threading.Event()
    close_sessions = threading.Event()
    try:
        for node in range(1, 6):
            cluster.launch(node)
            cluster.wait_generation(list(range(1, node + 1)), node,
                                    list(range(1, node + 1)), timeout=45)
        print("PASS crash54: five peers committed", flush=True)
        prefill_colors = prefill(cluster.ports, count=256)
        expected = dict(prefill_colors)

        for index, node in enumerate((1, 2, 3, 4, 5)):
            painter = Painter(node, cluster.ports[node][1], index, 5,
                              args.rate, shared, stop, close_sessions)
            painter.start()
            painters[node] = painter
        time.sleep(1.0)

        crash_time = time.monotonic()
        os.killpg(cluster.procs[4].pid, signal.SIGKILL)
        print("crash54: SIGKILL Node 4 during painting", flush=True)

        cluster.wait_generation([1, 2, 3, 5], 6, [1, 2, 3, 5],
                                timeout=120)
        removal_committed = time.monotonic()
        print(f"PASS crash54: Node 4 removed, generation 6 "
              f"({removal_committed - crash_time:.2f}s after kill)",
              flush=True)

        def removed_seen():
            # The authority owns the removal commit log line.
            return any("[membership] REMOVED peer=4" in line
                       for _, line in cluster.lines(1))
        wait_for(removed_seen, "REMOVED accounting on the authority",
                 timeout=60)

        def released():
            for node in (1, 2, 3, 5):
                lines = cluster.lines(node)
                start = transition_start(lines, 6)
                if start is None or \
                        transition_release(lines, 6, start) is None:
                    return False
            return True
        wait_for(released, "generation-6 RELEASE on survivors", timeout=60)
        release_time = time.monotonic()
        stop.set()  # keep every surviving session open through replay

        def drained():
            for node in (1, 2, 3, 5):
                lines = cluster.lines(node)
                start = transition_start(lines, 6)
                release_at = transition_release(lines, 6, start)
                zero_at = None
                for timestamp, line in lines:
                    if release_at is not None and timestamp < release_at:
                        continue
                    if ("[put-buffer] node=" in line and "event=tick" in line
                            and " depth=0 " in line):
                        zero_at = timestamp
                if release_at is None or zero_at is None:
                    return False
                if any(timestamp > zero_at and "event=enqueue" in line
                       for timestamp, line in lines):
                    return False
            return True
        wait_for(drained, "queue drain on survivors", timeout=60)
        drain_time = time.monotonic()
        close_sessions.set()
        for painter in painters.values():
            painter.join(timeout=5)
        time.sleep(1.5)

        summary = {}
        for node in (1, 2, 3, 5):
            summary[node] = summarize_node(node, cluster.lines(node), 6)
        summary[4] = summarize_node(4, cluster.lines(4), 6)
        accounting = check_accounting(summary, "crash54")

        with shared.lock:
            errors = list(shared.errors)
            last_color = dict(shared.last_color)
            node4_color = dict(shared.last_color_by_node[4])
            sent = dict(shared.sent)
            closed = list(shared.closed)
        expected_all = dict(expected)
        expected_all.update(last_color)
        # Node 4's final sends may never have reached a replica (SIGKILL).
        # They are not required for correctness; drop them from the expected
        # canvas instead of pretending every socket write was durable.
        for coordinate in node4_color:
            expected_all.pop(coordinate, None)
        buffered = set()
        for node in (1, 2, 3, 5):
            buffered |= buffered_coordinates(cluster.lines(node))
        buffered = {c for c in buffered if c in expected_all}
        sample, mismatches, missing = verify_canvas(
            cluster.ports, expected_all, (1, 2, 3, 5),
            extra_coordinates=buffered)

        result = {
            "mode": "crash54",
            "nodes": summary,
            "accounting_problems": accounting,
            "browser_sent": sent,
            "browser_errors": errors,
            "browser_closed_nodes": closed,
            "buffered_unique_coordinates": len(buffered),
            "canvas_sample": len(sample),
            "canvas_mismatches": mismatches[:20],
            "canvas_mismatch_count": len(mismatches),
            "canvas_missing_count": len(missing),
            "state": {n: cluster.state(n)[:2] for n in (1, 2, 3, 4, 5)},
            "timing": {
                "crash": crash_time,
                "removal_committed": removal_committed,
                "release": release_time,
                "drained": drain_time,
            },
        }
        (root / "result.json").write_text(json.dumps(result, indent=2))
        print(json.dumps({k: v for k, v in result.items()
                          if k not in ("nodes", "canvas_mismatches")},
                         indent=2), flush=True)
        for node, info in summary.items():
            print(f"node {node}: enqueued={info['enqueued']} "
                  f"replayed={info['replayed']} submitted={info['submitted']} "
                  f"ok={info['quorum_ok']} failed={info['quorum_failed']} "
                  f"disc_disc={info['discarded_disconnected']} "
                  f"disc_mem={info['discarded_membership']}", flush=True)
        assert not accounting, accounting
        assert not mismatches, mismatches[:5]
        assert not missing, missing[:5]
        assert len(mismatches) == 0 and len(missing) == 0
        # Node 4 crashed; its painter reports a closed session. That is the
        # expected client-visible consequence of losing the RAM-only queue.
        assert 4 in closed, "Node 4's browser session should close"
        # No false success: survivors' replay attempts must reconcile.
        assert all(info["quorum_ok"] + info["quorum_failed"] ==
                   info["submitted"] for n, info in summary.items()
                   if n in (1, 2, 3, 5))
        assert sum(info["enqueued"] for n, info in summary.items()
                   if n in (1, 2, 3, 5)) > 0, "crash54 exercised no buffering"
        assert buffered, "crash54 did not verify any buffered coordinate"
        print("PASS crash54: bounded PUT buffer integrated with a live 5->4 "
              "SIGKILL removal", flush=True)
    finally:
        stop.set()
        close_sessions.set()
        for painter in painters.values():
            painter.join(timeout=2)
        cluster.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("join45", "crash54"))
    parser.add_argument("--peer", default="./build/bin/p2place-peer")
    parser.add_argument("--out", required=True)
    parser.add_argument("--rate", type=float, default=40.0,
                        help="PUTs/second per painter")
    parser.add_argument("--suspect", type=int, default=2)
    parser.add_argument("--unavailable", type=int, default=6)
    args = parser.parse_args()
    args.peer = pathlib.Path(args.peer).resolve()
    assert args.peer.is_file(), f"build the peer first: {args.peer}"
    args.out = pathlib.Path(args.out).resolve()
    if args.mode == "join45":
        run_join(args)
    else:
        run_crash(args)


if __name__ == "__main__":
    main()
