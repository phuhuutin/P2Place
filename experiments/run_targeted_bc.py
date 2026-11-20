#!/usr/bin/env python3
"""P2Place targeted B/C dual-original-owner crash experiment (one strategy per run).

A deliberately adversarial demonstration of *write-aware* replication, not a
random-churn benchmark and not proof that Strategy C is generally superior.

Timeline (defaults, seconds after the workload starts):

    0 .. 48    concentrated valid PUTs to one hot region (default 6/s)
    ~48        strict readiness gate, then simultaneous SIGKILL of the region's
               two *original* N=2 owners (gateway node 1 is never one of them)
    immediately 8 concurrent browser PUT probes on unique pixels in the region,
               issued before failure detection can reconfigure placement
    48 .. 78   continue the hot-region workload during the failure/reconfiguration
    78         restart the two original owners from their isolated directories
    78 .. 180  continue writes and perform periodic full-canvas browser GETs
    180 .. 300 no writes: verify C's write window decays back to N=2

A client-visible ACK is a matching ``{"op":"pixel"}`` echo on the issuing
WebSocket connection (the protocol used by ``web/app.js``).  Server-side
``[quorum-done]`` lines are parsed only as corroboration, never as ACKs.

Run B first; it writes ``scenario.json``.  C validates and reuses the same
region, original owners, placement generation, binary and timeline.  Each run
writes its own complete result set under ``<out-root>/seed<seed>/<STRATEGY>/``.
When both strategy summaries exist, ``comparison.md`` is (re)generated in the
seed directory.

Usage:
    python3 -u experiments/run_targeted_bc.py --strategy B --seed 712367 \
        --out-root experiments/results/bc_targeted_v4
    python3 -u experiments/run_targeted_bc.py --strategy C --seed 712367 \
        --out-root experiments/results/bc_targeted_v4

Exit codes: 0 PASS, 2 FAIL, 3 INCONCLUSIVE.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ws_client import get_canvas, normalize_color, put_pixel  # noqa: E402

# --------------------------------------------------------------------------- #
# Log formats (verified against build/bin/p2place-peer, Nov 2026)
# --------------------------------------------------------------------------- #
PLACEMENT_RE = re.compile(
    r"\[placement\] committed generation=(\d+) fingerprint=(\d+) owners=(\d+)"
)
QUORUM_PUT_RE = re.compile(
    r"\[quorum\] node=(\d+) op=PUT request=(\d+) region=(\d+) index=(\d+) "
    r"N=(\d+) assigned=(\d+) W=(\d+) targets=([\d,]*)"
)
QUORUM_DONE_RE = re.compile(
    r"\[quorum-done\] node=(\d+) op=(GET|PUT) request=(\d+) ok=([01]) "
    r"ack=(\d+)/(\d+) error=(.*)"
)
WS_PUT_RE = re.compile(
    r"\[ws-put\] node=(\d+) request=(\d+) x=(\d+) y=(\d+) region=(\d+) "
    r"index=(\d+) color=(#[0-9A-Fa-f]{6}) version=(\d+)"
)
POLICY_BCAST_RE = re.compile(
    r"\[policy-broadcast\] node=(\d+) gen=(\d+) epoch=(\d+) "
    r"n2=(\d+) n3=(\d+) n4=(\d+) other=(\d+)"
)

PHASES = ("heat", "immediate_fault_probe", "fault_window", "recovery", "idle")
PROBE_BASE_SEQUENCE = 10000


class ExperimentError(RuntimeError):
    """Base class for controlled experiment aborts."""


class Inconclusive(ExperimentError):
    """Evidence is unusable; never reinterpret this as a policy failure."""


class HypothesisFailure(ExperimentError):
    """The run is valid, but the strategy's required property did not hold."""


# --------------------------------------------------------------------------- #
# Small utilities
# --------------------------------------------------------------------------- #
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sleep_until(deadline: float, stop: threading.Event | None = None) -> bool:
    """Sleep until a monotonic deadline; return False if ``stop`` was set."""
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return True
        if stop is not None and stop.is_set():
            return False
        time.sleep(min(0.05, remaining))


def free_ports(nodes: int) -> dict[int, tuple[int, int]]:
    """Allocate all peer/WS ports while every socket is still bound."""
    sockets = []
    ports: dict[int, tuple[int, int]] = {}
    try:
        for node in range(1, nodes + 1):
            pair = []
            for _ in range(2):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                sockets.append(sock)
                pair.append(sock.getsockname()[1])
            ports[node] = (pair[0], pair[1])
        return ports
    finally:
        for sock in sockets:
            sock.close()


def wait_ws_port(port: int, proc: subprocess.Popen, timeout: float = 25.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise ExperimentError(f"peer exited early with code {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise ExperimentError(f"websocket port {port} never opened")


def kill_group(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def pixel(region: int, sequence: int, seed: int) -> tuple[int, int, str]:
    """Deterministic per-PUT pixel; identical for B and C for the same sequence."""
    x = (region % 16) * 16 + (sequence % 16)
    y = (region // 16) * 16 + ((sequence // 16) % 16)
    digest = hashlib.sha256(f"{seed}:{sequence}".encode()).digest()
    return x, y, f"#{digest[0]:02X}{digest[1]:02X}{digest[2]:02X}"


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * q))))
    return round(ordered[index], 3)


# --------------------------------------------------------------------------- #
# Incremental log follower (node 1)
# --------------------------------------------------------------------------- #
class LogFollower:
    """Incrementally parse node1's log into placement/quorum/ws-put records."""

    def __init__(self, path: Path):
        self.path = path
        self.offset = 0
        self.partial = b""
        self.lock = threading.RLock()
        self.placements: list[dict] = []
        self.quorum_puts: list[dict] = []
        self.quorum_done: dict[int, dict] = {}
        self.ws_puts: dict[int, dict] = {}
        self.policy_broadcasts: list[dict] = []
        self.errors = 0

    def poll(self) -> None:
        with self.lock:
            if not self.path.exists():
                return
            try:
                size = self.path.stat().st_size
                if size <= self.offset:
                    return
                with self.path.open("rb") as handle:
                    handle.seek(self.offset)
                    data = handle.read()
                    self.offset = handle.tell()
            except OSError:
                return
            data = self.partial + data
            lines = data.split(b"\n")
            self.partial = lines.pop() if lines else b""
            for raw in lines:
                self._parse(raw.decode("utf-8", errors="replace"))

    def _parse(self, line: str) -> None:
        match = PLACEMENT_RE.search(line)
        if match:
            self.placements.append(
                {"gen": int(match.group(1)), "fp": match.group(2), "owners": int(match.group(3))}
            )
            return
        match = QUORUM_PUT_RE.search(line)
        if match:
            targets = [int(v) for v in match.group(8).split(",") if v]
            self.quorum_puts.append(
                {
                    "request": int(match.group(2)),
                    "region": int(match.group(3)),
                    "index": int(match.group(4)),
                    "n": int(match.group(5)),
                    "assigned": int(match.group(6)),
                    "w": int(match.group(7)),
                    "targets": targets,
                }
            )
            return
        match = QUORUM_DONE_RE.search(line)
        if match:
            if match.group(2) == "PUT":
                self.quorum_done[int(match.group(3))] = {
                    "ok": match.group(4) == "1",
                    "ack": int(match.group(5)),
                    "required": int(match.group(6)),
                    "error": match.group(7).strip(),
                }
            return
        match = WS_PUT_RE.search(line)
        if match:
            self.ws_puts[int(match.group(2))] = {
                "x": int(match.group(3)),
                "y": int(match.group(4)),
                "region": int(match.group(5)),
                "index": int(match.group(6)),
                "color": normalize_color(match.group(7)),
            }
            return
        match = POLICY_BCAST_RE.search(line)
        if match:
            self.policy_broadcasts.append(
                {
                    "gen": int(match.group(2)),
                    "epoch": int(match.group(3)),
                    "n2": int(match.group(4)),
                    "n3": int(match.group(5)),
                    "n4": int(match.group(6)),
                    "other": int(match.group(7)),
                }
            )

    # -- thread-safe views -------------------------------------------------- #
    def latest_placement(self) -> dict | None:
        self.poll()
        with self.lock:
            return dict(self.placements[-1]) if self.placements else None

    def latest_gen(self) -> int | None:
        placement = self.latest_placement()
        return placement["gen"] if placement else None

    def quorum_for_region(self, region: int, op: str = "PUT") -> list[dict]:
        self.poll()
        with self.lock:
            return [dict(r) for r in self.quorum_puts if r["region"] == region]

    def wsput_by_pixel(self, x: int, y: int, color: str) -> dict | None:
        self.poll()
        with self.lock:
            match = None
            for request, record in self.ws_puts.items():
                if (
                    record["x"] == int(x)
                    and record["y"] == int(y)
                    and record["color"] == normalize_color(color)
                ):
                    match = {"request": request, **record}
            return match

    def put_evidence(self, request: int) -> dict:
        self.poll()
        with self.lock:
            return {
                "quorum": dict(next((r for r in self.quorum_puts if r["request"] == request), {})),
                "done": dict(self.quorum_done.get(request, {})),
            }

    def latest_policy_broadcast(self) -> dict | None:
        self.poll()
        with self.lock:
            return dict(self.policy_broadcasts[-1]) if self.policy_broadcasts else None


class PolicyFollower:
    """Incrementally read node1/policy_node1.csv (byte offsets, binary safe)."""

    def __init__(self, path: Path):
        self.path = path
        self.offset = 0
        self.lock = threading.RLock()
        self.header: list[str] | None = None
        self.rows: list[dict] = []
        self.by_region: dict[int, list[dict]] = {}
        self.partial = ""

    def poll(self) -> None:
        with self.lock:
            if not self.path.exists():
                return
            try:
                size = self.path.stat().st_size
                if size <= self.offset:
                    return
                with self.path.open("rb") as handle:
                    handle.seek(self.offset)
                    data = handle.read()
                    self.offset = handle.tell()
            except OSError:
                return
            data = self.partial + data.decode("utf-8", errors="replace")
            lines = data.split("\n")
            self.partial = lines.pop() if lines else ""
            for line in lines:
                if not line.strip():
                    continue
                values = next(csv.reader([line]))
                if self.header is None:
                    self.header = values
                    continue
                if len(values) != len(self.header):
                    continue
                row = dict(zip(self.header, values))
                try:
                    parsed = {
                        "time_s": float(row["time_s"]),
                        "gen": int(row["placement_gen"]),
                        "epoch": int(row["policy_epoch"]),
                        "region": int(row["region"]),
                        "strategy": row.get("strategy", ""),
                        "risk": float(row.get("risk_score") or 0.0),
                        "desired_n": int(row.get("desired_n") or 0),
                        "assigned_n": int(row.get("assigned_n") or 0),
                        "confirmed": int(row.get("confirmed_live_copies") or 0),
                    }
                except (KeyError, ValueError):
                    continue
                self.rows.append(parsed)
                self.by_region.setdefault(parsed["region"], []).append(parsed)

    def history(self, region: int) -> list[dict]:
        self.poll()
        with self.lock:
            return [dict(r) for r in self.by_region.get(region, [])]

    def latest(self, region: int) -> dict | None:
        history = self.history(region)
        return history[-1] if history else None


def read_repair_events(out: Path, region: int) -> list[dict]:
    """Read live per-node replica_repair_events.csv rows for a region."""
    events: list[dict] = []
    for path in sorted(out.glob("node*/replica_repair_events.csv")):
        try:
            with path.open(newline="") as handle:
                for row in csv.DictReader(handle):
                    try:
                        if int(row["region"]) != region:
                            continue
                        events.append(
                            {
                                "node": int(row["node_id"]),
                                "region": int(row["region"]),
                                "gen": int(row["placement_gen"]),
                                "epoch": int(row["policy_epoch"]),
                                "event": row["event"],
                                "classification": row["classification"],
                                "attempts": int(row["attempts"] or 0),
                                "source": path.parent.name,
                            }
                        )
                    except (KeyError, ValueError):
                        continue
        except OSError:
            continue
    return events


# --------------------------------------------------------------------------- #
# Process management
# --------------------------------------------------------------------------- #
class ProcessRecorder:
    """Write the A/B-compatible node_process_events.csv during the run.

    Launch/kill/restart rows use the real PID and CLOCK_MONOTONIC timestamp.
    Events recorded before the workload anchor are written with negative
    time_s once the anchor is known (startup launches are genuinely before
    t=0), never back-dated to zero.
    """

    def __init__(self, out: Path):
        self.path = out / "node_process_events.csv"
        self.handle = self.path.open("w", newline="", buffering=1)
        self.writer = csv.writer(self.handle)
        self.writer.writerow(["time_s", "node_id", "boot", "action", "pid"])
        self.lock = threading.RLock()
        self.anchor_ns: int | None = None
        self.pending: list[tuple[int, int, int, str, int | str]] = []
        self.rows: list[dict] = []
        self.closed = False

    def set_anchor(self, monotonic_ns: int) -> None:
        with self.lock:
            self.anchor_ns = int(monotonic_ns)
            for record in self.pending:
                self._write(*record)
            self.pending.clear()

    def record(self, node_id: int, boot: int, action: str, pid: int | str) -> None:
        with self.lock:
            ns = time.monotonic_ns()
            self.rows.append(
                {"monotonic_ns": ns, "node_id": int(node_id), "boot": int(boot),
                 "action": action, "pid": pid}
            )
            if self.anchor_ns is None:
                self.pending.append((ns, int(node_id), int(boot), action, pid))
            else:
                self._write(ns, int(node_id), int(boot), action, pid)

    def _write(self, ns: int, node_id: int, boot: int, action: str, pid: int | str) -> None:
        if self.closed:
            return
        seconds = (ns - self.anchor_ns) / 1e9 if self.anchor_ns is not None else 0.0
        self.writer.writerow([f"{seconds:.3f}", node_id, boot, action, pid])
        self.handle.flush()

    def close(self) -> None:
        with self.lock:
            if self.closed:
                return
            if self.pending:
                # Startup aborted before a workload anchor: relative time is
                # undefined. Keep the real rows but mark the provenance in the
                # summary; do not invent a launch time.
                for ns, node_id, boot, action, pid in self.pending:
                    self.writer.writerow(["0.000", node_id, boot, action, pid])
                self.pending.clear()
            self.handle.close()
            self.closed = True


class Cluster:
    def __init__(self, args, strategy: str, out: Path, ports: dict[int, tuple[int, int]],
                 recorder: ProcessRecorder | None = None):
        self.args = args
        self.strategy = strategy
        self.out = out
        self.ports = ports
        self.nodes: dict[int, dict] = {}
        self.recorder = recorder
        self.boot: dict[int, int] = {}
        self.archived: set[tuple[int, int]] = set()
        self.prefix = [shutil.which("stdbuf"), "-oL", "-eL"] if shutil.which("stdbuf") else []

    def command(self, node_id: int) -> list[str]:
        args = self.args
        cmd = [
            *self.prefix,
            str(args.peer),
            "--id", str(node_id),
            "--port", str(self.ports[node_id][0]),
            "--ws-port", str(self.ports[node_id][1]),
            "--replicas", "3",
            "--read-quorum", "2",
            "--write-quorum", "2",
            "--strategy", self.strategy,
            "--min-replicas", "2",
            "--max-replicas", "4",
            "--policy-interval", str(args.policy_interval),
            "--policy-cooldown", str(args.policy_cooldown),
            "--write-window", str(args.write_window),
            "--write-report-interval", str(args.write_report_interval),
            "--write-threshold-mid", str(args.write_threshold_mid),
            "--write-threshold-high", str(args.write_threshold_high),
            "--metrics-dir", str(self.nodes_dir(node_id)),
            "--state-file", str(self.out / f"node{node_id}.state"),
        ]
        if node_id != 1:
            cmd += ["--peers", f"1:127.0.0.1:{self.ports[1][0]}"]
        cmd += args.peer_arg
        return cmd

    def nodes_dir(self, node_id: int) -> Path:
        return self.out / f"node{node_id}"

    def metrics_dir(self, node_id: int, boot: int) -> Path:
        return self.out / "metrics" / f"node{node_id}_boot{boot}"

    def _launch_node(self, node_id: int, spacing: bool) -> dict:
        """Launch one incarnation, with a per-boot log and a live node.log link.

        A/B logging parity uses node<N>_boot<K>.log.  The targeted harness keeps
        node<N>/node.log as a hard link to the CURRENT boot so existing readers
        (node1 readiness/timeline followers) keep working; previous boots remain
        at their per-boot paths.
        """
        boot = self.boot.get(node_id, 0) + 1
        self.boot[node_id] = boot
        directory = self.nodes_dir(node_id)
        directory.mkdir(parents=True, exist_ok=True)
        boot_log = self.out / f"node{node_id}_boot{boot}.log"
        log_handle = boot_log.open("w", buffering=1)
        link = directory / "node.log"
        try:
            if link.exists() or link.is_symlink():
                link.unlink()
            os.link(boot_log, link)
        except OSError as exc:
            # Fallback (e.g. filesystem without hard links): the current boot
            # writes node.log; the per-boot split is unavailable for this boot.
            log_handle.close()
            boot_log = link
            log_handle = boot_log.open("w", buffering=1)
            print(f"[log] WARNING: per-boot link failed for node {node_id}: {exc}",
                  flush=True)
        proc = subprocess.Popen(
            self.command(node_id),
            cwd=directory,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        info = {
            "proc": proc, "log": log_handle, "cmd": self.command(node_id),
            "dir": directory, "boot": boot, "log_path": boot_log, "pid": proc.pid,
        }
        self.nodes[node_id] = info
        if self.recorder is not None:
            self.recorder.record(node_id, boot, "launch", proc.pid)
        print(f"[node] start n={node_id} boot={boot} pid={proc.pid} "
              f"tcp={self.ports[node_id][0]} ws={self.ports[node_id][1]}",
              flush=True)
        wait_ws_port(self.ports[node_id][1], proc)
        if spacing:
            time.sleep(self.args.join_spacing)
        return info

    def launch(self) -> None:
        try:
            for node_id in range(1, self.args.nodes + 1):
                self._launch_node(node_id, spacing=True)
        except Exception:
            self.teardown()
            raise

    def teardown(self) -> None:
        for node in self.nodes.values():
            try:
                kill_group(node["proc"])
            except OSError:
                pass
        for node in self.nodes.values():
            try:
                node["log"].close()
            except OSError:
                pass
        # Per-incarnation metrics dirs for the dashboard, including the final
        # boot of every node.  Live node*/ CSVs remain for targeted tooling.
        for node_id, node in self.nodes.items():
            try:
                self.archive_metrics(node_id, int(node.get("boot", 1)))
            except OSError:
                pass
        self.nodes.clear()

    def kill_owners(self, owners: list[int]) -> dict[int, float]:
        """SIGKILL every owner in one burst before waiting for any of them."""
        killed = {}
        for owner in owners:
            info = self.nodes[owner]
            proc = info["proc"]
            if proc.poll() is None:
                if self.recorder is not None:
                    self.recorder.record(owner, int(info.get("boot", 1)), "kill", proc.pid)
                os.killpg(proc.pid, signal.SIGKILL)
                killed[owner] = time.monotonic()
                print(f"[fault] SIGKILL node={owner} boot={info.get('boot', 1)} "
                      f"pid={proc.pid}", flush=True)
        for owner in owners:
            try:
                self.nodes[owner]["proc"].wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        return killed

    def archive_metrics(self, node_id: int, boot: int) -> None:
        source = self.nodes_dir(node_id)
        target = self.metrics_dir(node_id, boot)
        target.mkdir(parents=True, exist_ok=True)
        for path in source.glob("*"):
            if path.is_file() and path.suffix in (".csv", ".jsonl"):
                try:
                    shutil.copy2(path, target / path.name)
                except OSError:
                    pass
        self.archived.add((node_id, boot))

    def restart(self, node_id: int) -> float:
        node = self.nodes[node_id]
        self.archive_metrics(node_id, int(node.get("boot", 1)))
        try:
            node["log"].close()
        except OSError:
            pass
        self._launch_node(node_id, spacing=False)
        print(f"[node] restart n={node_id} boot={self.boot[node_id]}", flush=True)
        return time.monotonic()


def wait_full_cluster(follower: LogFollower, nodes: int, timeout: float = 90.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        placement = follower.latest_placement()
        if placement and placement["owners"] == nodes:
            return placement["gen"]
        time.sleep(0.1)
    raise ExperimentError(f"cluster did not commit {nodes} members within {timeout}s")


def existing_peer_processes(peer: Path) -> list[str]:
    try:
        result = subprocess.run(
            ["pgrep", "-af", str(peer)], capture_output=True, text=True, timeout=5
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []
    return [line for line in result.stdout.splitlines() if line.strip()]


# --------------------------------------------------------------------------- #
# Scenario selection / validation
# --------------------------------------------------------------------------- #
def find_selection(
    args,
    strategy: str,
    out: Path,
    cluster: Cluster,
    follower: LogFollower,
    scenario: dict | None,
) -> tuple[int, list[int]]:
    if scenario is not None:
        region = int(scenario["region"])
        candidates = [region]
        owners_constraint = [int(v) for v in scenario["owners"]]
    else:
        candidates = random.Random(args.seed).sample(range(256), 256)
        owners_constraint = None
    last_detail = None
    for region in candidates:
        x, y, color = pixel(region, -1, args.seed)
        result = put_pixel(cluster.ports[1][1], x, y, color, timeout=args.ack_timeout)
        time.sleep(0.15)
        records = follower.quorum_for_region(region)
        baseline = None
        for record in reversed(records):
            targets = record["targets"]
            if record["n"] == 2 and len(targets) >= 2 and len(set(targets)) == 2:
                baseline = targets[:2]
                break
        last_detail = {
            "region": region,
            "selection_put": result,
            "quorum_put": records[-1] if records else None,
        }
        if not baseline or 1 in baseline:
            continue
        if owners_constraint is None or list(baseline) == list(owners_constraint):
            return region, list(baseline)
        raise Inconclusive(
            "scenario baseline owners changed: expected "
            f"{owners_constraint}, observed {baseline} for region {region}"
        )
    raise Inconclusive(
        "no eligible N=2 region found (needs two owners, excluding gateway node 1); "
        f"last probe: {json.dumps(last_detail)}"
    )


def scenario_config(args) -> dict:
    return {
        "nodes": args.nodes,
        "seconds": args.seconds,
        "rate": args.rate,
        "kill_at": args.kill_at,
        "rejoin_at": args.rejoin_at,
        "heat_until": args.heat_until,
        "probes": args.probes,
        "ack_timeout": args.ack_timeout,
        "join_spacing": args.join_spacing,
        "stabilize": args.stabilize,
        "policy_interval": args.policy_interval,
        "policy_cooldown": args.policy_cooldown,
        "write_window": args.write_window,
        "write_report_interval": args.write_report_interval,
        "write_threshold_mid": args.write_threshold_mid,
        "write_threshold_high": args.write_threshold_high,
        "concurrency": args.concurrency,
        "readback_timeout": args.readback_timeout,
        "get_timeout": args.get_timeout,
    }


def validate_scenario(scenario: dict, args, peer_sha: str) -> None:
    if int(scenario.get("seed", -1)) != args.seed:
        raise Inconclusive(f"scenario seed mismatch: {scenario.get('seed')} != {args.seed}")
    if int(scenario.get("nodes", -1)) != args.nodes:
        raise Inconclusive("scenario node count mismatch")
    if scenario.get("peer_sha256") and scenario["peer_sha256"] != peer_sha:
        raise Inconclusive("scenario binary sha256 mismatch (rebuild between B and C?)")
    stored = scenario.get("config")
    if isinstance(stored, dict):
        current = scenario_config(args)
        differences = {
            key: {"scenario": stored.get(key), "current": value}
            for key, value in current.items()
            if key in stored and stored[key] != value
        }
        if differences:
            raise Inconclusive(f"scenario configuration mismatch: {json.dumps(differences)}")


# --------------------------------------------------------------------------- #
# Readiness gates
# --------------------------------------------------------------------------- #
def activation_epochs(rows: list[dict], region: int, gen: int) -> dict[int, int | None]:
    """Most recent policy epoch at which preference position 2/3 became active."""
    act: dict[int, int | None] = {2: None, 3: None}
    previous = 0
    for row in rows:
        if row["region"] != region or row["gen"] != gen:
            continue
        assigned = row["assigned_n"]
        if assigned >= 3 and previous < 3:
            act[2] = row["epoch"]
        if assigned >= 4 and previous < 4:
            act[3] = row["epoch"]
        previous = assigned
    return act


def latest_scheduled_region_put(workload_rows: list[dict], region: int) -> dict | None:
    candidates = [
        row
        for row in workload_rows
        if row["region"] == region and row["phase"] != "immediate_fault_probe"
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda row: row["time_s"])


def evaluate_readiness(
    args,
    strategy: str,
    out: Path,
    region: int,
    owners: list[int],
    placement_gen: int,
    follower: LogFollower,
    policies: PolicyFollower,
    workload_rows: list[dict],
) -> dict:
    """Strict pre-fault certification.  Never infers a client ACK from node logs."""
    now = time.monotonic()
    detail: dict = {
        "strategy": strategy,
        "region": region,
        "original_owners": owners,
        "placement_gen": placement_gen,
        "ready": False,
        "proceed": False,
        "hypothesis_applicable": None,
        "reason": "",
        "checked_at_utc": utc_now(),
    }

    placement = follower.latest_placement()
    detail["latest_placement"] = placement
    records = follower.quorum_for_region(region)
    detail["recent_quorum_puts"] = records[-5:]
    if not records:
        detail["reason"] = "no_put_quorum_records"
        return detail
    latest = records[-1]
    request = latest["request"]
    done = follower.put_evidence(request)["done"]
    detail["latest_put"] = latest
    detail["latest_put_done"] = done

    row = latest_scheduled_region_put(workload_rows, region)
    detail["latest_client_put"] = row
    client_ack = bool(row and row["ack"] == 1 and row.get("result") == "ack")
    client_fresh = bool(row and (now - row["_monotonic"]) <= 3.0)
    detail["client_ack"] = client_ack
    detail["client_ack_fresh"] = client_fresh

    placement_stable = bool(
        placement
        and placement["owners"] == args.nodes
        and placement["gen"] == placement_gen
    )
    detail["placement_stable"] = placement_stable

    policy = policies.latest(region)
    detail["latest_policy"] = policy
    policy_gen_ok = bool(policy and policy["gen"] == placement_gen)
    detail["policy_gen_ok"] = policy_gen_ok

    broadcast = follower.latest_policy_broadcast()
    detail["latest_policy_broadcast"] = broadcast

    common_quorum = (
        done.get("ok") is True
        and done.get("ack", 0) >= 2
        and latest["w"] == 2
        and len(latest["targets"]) >= 2
    )

    if strategy == "B":
        raised = bool(
            (policy and policy["assigned_n"] > 2)
            or latest["n"] > 2
            or len(latest["targets"]) > 2
        )
        detail["b_raised_n_before_fault"] = raised
        if raised:
            detail.update(
                proceed=True,
                hypothesis_applicable=False,
                reason="B_increased_N_before_fault",
                policy_n=policy["assigned_n"] if policy else None,
            )
            return detail
        n2_broadcast = bool(
            broadcast
            and placement
            and broadcast["gen"] == placement["gen"]
            and broadcast["n2"] == 256
            and broadcast["n3"] == 0
            and broadcast["n4"] == 0
        )
        detail["n2_policy_broadcast"] = n2_broadcast
        genuine_n2 = (
            latest["n"] == 2
            and latest["w"] == 2
            and list(latest["targets"]) == list(owners)
        )
        detail["genuine_n2_original_owner_put"] = genuine_n2
        ready = bool(
            genuine_n2
            and common_quorum
            and placement_stable
            and client_ack
            and client_fresh
            and n2_broadcast
            and (policy is None or (policy_gen_ok and policy["assigned_n"] == 2))
        )
        detail.update(ready=ready, proceed=ready, hypothesis_applicable=True)
        if not ready:
            reasons = []
            if not genuine_n2:
                reasons.append("latest_PUT_not_N2_on_original_owners")
            if not common_quorum:
                reasons.append("no_successful_W2_quorum")
            if not placement_stable:
                reasons.append("placement_not_stable")
            if not client_ack or not client_fresh:
                reasons.append("no_fresh_client_ACK")
            if not n2_broadcast:
                reasons.append("no_N2_policy_broadcast")
            detail["reason"] = ",".join(reasons) or "B_readiness_failed"
        return detail

    # ---------------------------- Strategy C ---------------------------- #
    if not policy or policy["assigned_n"] != 4 or not policy_gen_ok:
        detail["reason"] = "C_not_N4_in_current_placement_generation"
        detail["policy_history_tail"] = policies.history(region)[-8:]
        return detail
    if latest["n"] != 4 or len(latest["targets"]) < 4:
        detail["reason"] = "latest_routed_PUT_not_N4"
        return detail
    if list(latest["targets"][:2]) != list(owners):
        detail["reason"] = "original_owner_order_changed"
        return detail
    if not common_quorum:
        detail["reason"] = "no_successful_W2_quorum_on_N4_targets"
        return detail
    if not placement_stable:
        detail["reason"] = "placement_not_stable"
        return detail
    if not (client_ack and client_fresh):
        detail["reason"] = "no_fresh_client_ACK_from_N4_put"
        return detail

    extras = list(latest["targets"][2:4])
    detail["additional_replicas"] = extras
    rows = policies.history(region)
    act = activation_epochs(rows, region, placement_gen)
    detail["activation_epochs"] = {"position2": act[2], "position3": act[3]}
    events = read_repair_events(out, region)
    detail["repair_events"] = [
        event
        for event in events
        if event["gen"] == placement_gen and event["event"] == "backfill_completed"
    ]
    checks = []
    for position, node_id in ((2, extras[0]), (3, extras[1])):
        join_epoch = act[position]
        completed = [
            event
            for event in events
            if event["node"] == node_id
            and event["gen"] == placement_gen
            and event["event"] == "backfill_completed"
            and event["classification"] == "complete"
            and join_epoch is not None
            and event["epoch"] >= join_epoch
        ]
        checks.append(
            {
                "position": position,
                "node": node_id,
                "join_policy_epoch": join_epoch,
                "completed_events": completed,
                "ready": bool(completed),
            }
        )
    detail["additional_replica_checks"] = checks

    # live-copy corroboration: self is never in fd_by_node_, so discount it.
    expected_confirmed = latest["assigned"] - (1 if 1 in latest["targets"] else 0)
    live_ok = policy["confirmed"] >= expected_confirmed
    detail["confirmed_live_copies"] = policy["confirmed"]
    detail["expected_confirmed_live_copies"] = expected_confirmed
    detail["live_copies_ok"] = live_ok

    ready = all(check["ready"] for check in checks) and live_ok
    detail.update(ready=ready, proceed=ready)
    if not ready:
        reasons = [f"missing_complete_backfill_node{check['node']}" for check in checks if not check["ready"]]
        if not live_ok:
            reasons.append("confirmed_live_copies_below_expected")
        detail["reason"] = ",".join(reasons)
        detail["policy_history_tail"] = rows[-8:]
    return detail


# --------------------------------------------------------------------------- #
# Workload
# --------------------------------------------------------------------------- #
class Workload(threading.Thread):
    """Paced, bounded-concurrency browser PUT generator (one connection per PUT)."""

    def __init__(self, args, cluster: Cluster, region: int, start: float,
                 skip_pixels: set[tuple[int, int]], rows: list[dict], writer,
                 lock: threading.Lock, flush):
        super().__init__(daemon=True)
        self.args = args
        self.cluster = cluster
        self.region = region
        self.start_time = start
        self.skip_pixels = skip_pixels
        self.rows = rows
        self.writer = writer
        self.lock = lock
        self.flush = flush
        self.pause_event = threading.Event()
        self.stop_event = threading.Event()
        self.semaphore = threading.BoundedSemaphore(args.concurrency)
        self.schedule_lock = threading.Lock()
        self.drained = threading.Event()
        self.inflight = 0
        self.inflight_lock = threading.Lock()
        self.skipped_collisions = 0
        self.sequence = 0
        self.last_quorum: dict | None = None
        self.last_quorum_lock = threading.Lock()
        self.executor_threads: list[threading.Thread] = []
        self.executor_stop = threading.Event()
        self.task_queue: list[tuple[int, int, float]] = []
        self.queue_lock = threading.Lock()
        self.queue_event = threading.Event()
        self.raw_samples: list[dict] = []
        self.raw_samples_lock = threading.Lock()

    # -- counters used by the timeline ------------------------------------- #
    def snapshot(self) -> dict:
        with self.inflight_lock:
            inflight = self.inflight
        with self.queue_lock:
            queued = len(self.task_queue)
        return {"inflight": inflight, "queued": queued, "skipped_collisions": self.skipped_collisions}

    def _worker(self) -> None:
        while True:
            if self.executor_stop.is_set() and not self.task_queue:
                return
            self.queue_event.wait(timeout=0.2)
            item = None
            with self.queue_lock:
                if self.task_queue:
                    item = self.task_queue.pop(0)
                elif self.executor_stop.is_set():
                    return
                if not self.task_queue:
                    self.queue_event.clear()
            if item is None:
                continue
            sequence, phase, scheduled = item
            self._do_put(sequence, phase, scheduled)
            self.semaphore.release()
            with self.inflight_lock:
                self.inflight -= 1
                if self.inflight == 0:
                    self.drained.set()

    def _do_put(self, sequence: int, phase: str, scheduled: float) -> None:
        x, y, color = pixel(self.region, sequence, self.args.seed)
        sent = time.monotonic()
        result = put_pixel(
            self.cluster.ports[1][1], x, y, color, timeout=self.args.ack_timeout
        )
        row = {
            "time_s": round(sent - self.start_time, 3),
            "phase": phase,
            "sequence": sequence,
            "region": self.region,
            "x": x,
            "y": y,
            "color": color,
            "ack": int(result["result"] == "ack"),
            "result": result["result"],
            "reason": result["reason"],
            "latency_ms": result["latency_ms"],
            "total_ms": result["total_ms"],
            "offered_offset_s": round(scheduled, 3),
            "_monotonic": sent,
        }
        with self.lock:
            self.rows.append(row)
            self.writer.writerow({key: value for key, value in row.items() if not key.startswith("_")})
            self.flush()
        with self.raw_samples_lock:
            if len(self.raw_samples) < 5:
                self.raw_samples.append(
                    {
                        "time_s": row["time_s"],
                        "sequence": sequence,
                        "request": {"op": "put", "x": x, "y": y, "color": color},
                        "result": result["result"],
                        "reason": result["reason"],
                        "latency_ms": result["latency_ms"],
                        "messages": result.get("messages", []),
                    }
                )
        if result["result"] == "ack":
            with self.last_quorum_lock:
                self.last_quorum = {"x": x, "y": y, "color": color, "time_s": row["time_s"]}

    def run(self) -> None:
        workers = [
            threading.Thread(target=self._worker, daemon=True, name=f"put-worker-{i}")
            for i in range(self.args.concurrency)
        ]
        for worker in workers:
            worker.start()
        self.executor_threads = workers
        next_scheduled = 0.0
        # The nominal schedule is the experiment's offered load.
        while not self.stop_event.is_set():
            now = time.monotonic() - self.start_time
            if now >= self.args.heat_until:
                break
            if self.pause_event.is_set():
                next_scheduled = now
                time.sleep(0.02)
                continue
            wait = next_scheduled - now
            if wait > 0:
                if self.stop_event.wait(min(wait, 0.05)):
                    break
                continue
            self.sequence += 1
            sequence = self.sequence
            x, y, _ = pixel(self.region, sequence, self.args.seed)
            if (x, y) in self.skip_pixels:
                self.skipped_collisions += 1
                next_scheduled += 1.0 / self.args.rate
                continue
            phase = (
                "heat"
                if now < self.args.kill_at
                else "fault_window"
                if now < self.args.rejoin_at
                else "recovery"
            )
            self.semaphore.acquire()
            with self.schedule_lock:
                if self.pause_event.is_set() or self.stop_event.is_set():
                    self.semaphore.release()
                    next_scheduled = time.monotonic() - self.start_time
                    continue
                with self.inflight_lock:
                    self.inflight += 1
                    self.drained.clear()
                with self.queue_lock:
                    self.task_queue.append((sequence, phase, next_scheduled))
                    self.queue_event.set()
            next_scheduled += 1.0 / self.args.rate
            if next_scheduled < now - 1.0:
                next_scheduled = now
        # Let queued/in-flight work finish.
        deadline = time.monotonic() + self.args.ack_timeout + 3.0
        while time.monotonic() < deadline:
            with self.inflight_lock:
                if self.inflight == 0:
                    break
            time.sleep(0.05)
        self.executor_stop.set()
        self.queue_event.set()

    def pause_and_drain(self, timeout: float = 5.0) -> bool:
        # Holding schedule_lock while setting pause guarantees that no new
        # task can slip between the pause check and the enqueue.
        with self.schedule_lock:
            self.pause_event.set()
        if not self.wait_drained(timeout):
            return False
        return True

    def wait_drained(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.inflight_lock:
                if self.inflight == 0:
                    return True
            time.sleep(0.02)
        return False

    def resume(self) -> None:
        self.pause_event.clear()

    def stop(self) -> None:
        self.stop_event.set()
        self.pause_event.clear()


# --------------------------------------------------------------------------- #
# Probes
# --------------------------------------------------------------------------- #
def run_probe_burst(
    args,
    cluster: Cluster,
    region: int,
    start: float,
    follower: LogFollower,
) -> list[dict]:
    """Issue all probes concurrently, immediately after the crash.

    All workers record the placement generation immediately before sending.
    Their requests are correlated to ``[quorum]`` lines later; a probe only
    counts as fixed-generation evidence if its routed targets match the
    certified pre-fault target set and the generation was unchanged.
    """
    count = args.probes
    results: list[dict | None] = [None] * count
    barrier = threading.Barrier(count)

    def worker(index: int) -> None:
        sequence = PROBE_BASE_SEQUENCE + index
        x, y, color = pixel(region, sequence, args.seed)
        try:
            barrier.wait(timeout=10)
        except threading.BrokenBarrierError:
            pass
        gen_before = follower.latest_gen()
        issued = time.monotonic()
        result = put_pixel(cluster.ports[1][1], x, y, color, timeout=args.ack_timeout)
        results[index] = {
            "sequence": sequence,
            "x": x,
            "y": y,
            "color": color,
            "issue_time_s": round(issued - start, 4),
            "gen_before": gen_before,
            "ack": int(result["result"] == "ack"),
            "result": result["result"],
            "reason": result["reason"],
            "latency_ms": result["latency_ms"],
            "total_ms": result["total_ms"],
            "raw_first_messages": result.get("messages", []),
            "_monotonic": issued,
        }

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=args.ack_timeout + 5)
    return [result for result in results if result is not None]


def correlate_probe_evidence(
    probes: list[dict],
    follower: LogFollower,
    pre_fault_gen: int,
    expected_targets: list[int],
    strategy: str,
) -> None:
    """Attach routed-target and server-quorum evidence to each probe."""
    time.sleep(0.3)
    follower.poll()
    gen_after = follower.latest_gen()
    for probe in probes:
        probe["gen_after_burst"] = gen_after
        wsput = follower.wsput_by_pixel(probe["x"], probe["y"], probe["color"])
        if not wsput:
            probe.update(
                routed=False,
                targets=[],
                quorum_n=None,
                quorum_ok=None,
                fixed_generation=False,
                window_label="not_routed_no_quorum",
            )
            continue
        request = wsput["request"]
        evidence = follower.put_evidence(request)
        quorum = evidence["quorum"]
        done = evidence["done"]
        targets = list(quorum.get("targets", []))
        probe.update(
            routed=True,
            request=request,
            quorum_n=quorum.get("n"),
            quorum_w=quorum.get("w"),
            targets=targets,
            server_quorum_ok=done.get("ok"),
            server_quorum_ack=f"{done.get('ack')}/{done.get('required')}" if done else None,
            server_quorum_error=done.get("error", ""),
        )
        # The full routed target set must match the certified pre-fault set:
        # for B the original N=2 owners (or B's adapted set), for C the four
        # certified targets including both completed-backfill extras.
        target_match = list(targets) == list(expected_targets)
        probe["target_match"] = target_match
        # The probe is fixed-generation evidence if the PUT was *routed* under
        # the certified pre-fault generation.  The reply may legitimately
        # arrive after failure detection reconfigures placement (the B timeout
        # is ~3 s), so the post-burst generation is diagnostic, not a gate.
        # Targets matching the dead original owners cannot occur after a
        # reconfiguration removed those owners from the membership.
        probe["fixed_generation"] = bool(
            target_match and probe["gen_before"] == pre_fault_gen
        )
        if probe["fixed_generation"]:
            probe["window_label"] = "fixed_generation"
        elif probe["gen_before"] != pre_fault_gen:
            probe["window_label"] = "after_placement_reconfiguration"
        else:
            probe["window_label"] = "target_set_mismatch"


# --------------------------------------------------------------------------- #
# GETs and read-back
# --------------------------------------------------------------------------- #
class Getter(threading.Thread):
    def __init__(self, args, cluster: Cluster, start: float, rejoin_time: float,
                 stop_event: threading.Event, probe_pixels: list[dict],
                 fallback_nodes: list[int], rows: list[dict], lock: threading.Lock,
                 writer, events: list[dict]):
        super().__init__(daemon=True)
        self.args = args
        self.cluster = cluster
        self.start_time = start
        self.rejoin_time = rejoin_time
        self.stop_event = stop_event
        self.probe_pixels = probe_pixels
        self.fallback_nodes = fallback_nodes
        self.rows = rows
        self.lock = lock
        self.writer = writer
        self.events = events
        self.readback_state = {
            "acked_probes": len(probe_pixels),
            "visible": {},
            "first_visible_s": None,
            "complete_gets": 0,
            "attempts": 0,
        }
        self.error: str | None = None

    def _get_once(self, port: int, purpose: str) -> dict:
        sent = time.monotonic()
        result = get_canvas(port, timeout=self.args.get_timeout)
        row = {
            "time_s": round(sent - self.start_time, 3),
            "purpose": purpose,
            "reader_port": port,
            "complete": int(result["complete"]),
            "pixel_count": result["pixel_count"],
            "latency_ms": result["latency_ms"],
            "reason": result["reason"],
        }
        with self.lock:
            self.rows.append(row)
            self.writer.writerow(row)
        return result

    def _check_visibility(self, result: dict, elapsed: float) -> None:
        if not result["complete"]:
            return
        self.readback_state["complete_gets"] += 1
        pixels = result["pixels"]
        newly = []
        for probe in self.probe_pixels:
            key = (probe["x"], probe["y"])
            get_key = f"{probe['x']},{probe['y']}"
            if get_key in self.readback_state["visible"]:
                continue
            if pixels.get(key) == normalize_color(probe["color"]):
                self.readback_state["visible"][get_key] = round(elapsed, 3)
                newly.append(get_key)
        if newly and self.readback_state["first_visible_s"] is None:
            self.readback_state["first_visible_s"] = round(elapsed, 3)

    def run(self) -> None:
        try:
            self._run()
        except Exception as exc:  # noqa: BLE001 - surfaced as INCONCLUSIVE
            self.error = f"{type(exc).__name__}: {exc}"

    def _run(self) -> None:
        # Poll for visibility for --readback-timeout seconds after rejoin,
        # then keep taking periodic snapshots through the recovery interval.
        deadline = self.rejoin_time + self.args.readback_timeout
        schedule = []
        t = self.rejoin_time + 5.0
        while t <= deadline:
            schedule.append(t)
            t += 4.0
        periodic = [self.rejoin_time + 60.0, self.rejoin_time + 80.0, self.rejoin_time + 100.0]
        schedule += [value for value in periodic if value <= self.args.seconds]
        for target in schedule:
            if self.stop_event.is_set():
                return
            if not sleep_until(self.start_time + target, self.stop_event):
                return
            within_readback = target <= deadline
            already_complete = (
                self.probe_pixels
                and len(self.readback_state["visible"]) == len(self.probe_pixels)
            )
            if within_readback and already_complete:
                continue  # still take the periodic recovery snapshots below
            result = self._get_once(
                self.cluster.ports[1][1],
                "readback" if within_readback else "periodic",
            )
            self.readback_state["attempts"] += 1
            self._check_visibility(result, target)
        # If node1 never showed every ACKed probe, fall back to the certified
        # extra replicas (still real browser GETs).  This avoids calling a
        # first-R read race permanent data loss.
        if self.probe_pixels and len(self.readback_state["visible"]) < len(self.probe_pixels):
            for node_id in self.fallback_nodes:
                if node_id not in self.cluster.nodes:
                    continue
                if self.stop_event.is_set():
                    return
                result = self._get_once(self.cluster.ports[node_id][1], "readback_fallback")
                self.readback_state["attempts"] += 1
                self._check_visibility(result, self.args.readback_timeout)
                if len(self.readback_state["visible"]) == len(self.probe_pixels):
                    return


# --------------------------------------------------------------------------- #
# Timeline
# --------------------------------------------------------------------------- #
class Timeline(threading.Thread):
    def __init__(self, args, start: float, follower: LogFollower, policies: PolicyFollower,
                 workload: Workload, stop_event: threading.Event, out: Path,
                 state: dict, events: list[dict], lock: threading.Lock):
        super().__init__(daemon=True)
        self.args = args
        self.start_time = start
        self.follower = follower
        self.policies = policies
        self.workload = workload
        self.stop_event = stop_event
        self.state = state
        self.events = events
        self.lock = lock
        self.path = out / "replication_timeline.csv"
        self.handle = self.path.open("w", newline="", buffering=1)
        self.writer = csv.DictWriter(
            self.handle,
            fieldnames=[
                "time_s", "phase", "placement_gen", "policy_epoch", "policy_n",
                "confirmed_live_copies", "last_put_n", "last_put_targets",
                "inflight", "queued", "attempts_total", "acks_total",
                "reconfig_detected_s", "event",
            ],
        )
        self.writer.writeheader()

    def run(self) -> None:
        try:
            self._run()
        except Exception as exc:  # noqa: BLE001 - surfaced as INCONCLUSIVE
            self.state["timeline_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            try:
                self.handle.close()
            except OSError:
                pass

    def _run(self) -> None:
        next_sample = self.start_time
        while not self.stop_event.is_set():
            if not sleep_until(next_sample, self.stop_event):
                break
            next_sample += 0.5
            now = time.monotonic() - self.start_time
            placement = self.follower.latest_placement()
            policy = self.policies.latest(self.args.region) if self.args.region is not None else None
            records = self.follower.quorum_for_region(self.args.region) if self.args.region is not None else []
            last_put = records[-1] if records else None
            with self.workload.lock:
                attempts = len(self.workload.rows)
                acks = sum(row["ack"] for row in self.workload.rows)
            counters = self.workload.snapshot()
            pre_fault = self.state.get("pre_fault_gen")
            reconfig = self.state.get("reconfig_detected_s")
            kill_time = self.state.get("kill_time_s") or self.args.kill_at
            rejoin_time = self.state.get("rejoin_time_s") or self.args.rejoin_at
            if (
                reconfig is None
                and pre_fault is not None
                and placement
                and placement["gen"] != pre_fault
                and now >= kill_time
            ):
                reconfig = round(now, 3)
                self.state["reconfig_detected_s"] = reconfig
                with self.lock:
                    self.events.append({"time_s": reconfig, "event": "placement_reconfigured",
                                        "detail": f"generation {placement['gen']}"})
            phase = (
                "idle"
                if now >= self.args.heat_until
                else "heat"
                if now < kill_time
                else "fault_window"
                if now < rejoin_time
                else "recovery"
            )
            self.writer.writerow(
                {
                    "time_s": round(now, 3),
                    "phase": phase,
                    "placement_gen": placement["gen"] if placement else None,
                    "policy_epoch": policy["epoch"] if policy else None,
                    "policy_n": policy["assigned_n"] if policy else None,
                    "confirmed_live_copies": policy["confirmed"] if policy else None,
                    "last_put_n": last_put["n"] if last_put else None,
                    "last_put_targets": ",".join(map(str, last_put["targets"])) if last_put else None,
                    "inflight": counters["inflight"],
                    "queued": counters["queued"],
                    "attempts_total": attempts,
                    "acks_total": acks,
                    "reconfig_detected_s": reconfig,
                    "event": "",
                }
            )


# --------------------------------------------------------------------------- #
# Aggregate helpers
# --------------------------------------------------------------------------- #
def summarize_subset(subset: list[dict], duration: float | None) -> dict:
    summary = {
        "attempts": len(subset),
        "acks": sum(row["ack"] for row in subset),
        "naks": sum(1 for row in subset if row["result"] == "nak"),
        "timeouts": sum(1 for row in subset if row["result"] == "timeout"),
        "closed": sum(1 for row in subset if row["result"] == "closed"),
        "connect_errors": sum(1 for row in subset if row["result"] == "connect_error"),
        "duration_s": round(duration, 3) if duration is not None else None,
        "offered_rate": None,
        "issued_rate": None,
        "latency_ms": {},
    }
    if duration and duration > 0:
        summary["issued_rate"] = round(len(subset) / duration, 3)
    ack_latencies = [row["latency_ms"] for row in subset if row["ack"] and row["latency_ms"] is not None]
    fail_latencies = [row["latency_ms"] for row in subset if not row["ack"] and row["latency_ms"] is not None]
    summary["latency_ms"] = {
        "ack_p50": percentile(ack_latencies, 0.50),
        "ack_p95": percentile(ack_latencies, 0.95),
        "ack_max": round(max(ack_latencies), 3) if ack_latencies else None,
        "fail_p50": percentile(fail_latencies, 0.50),
        "fail_p95": percentile(fail_latencies, 0.95),
    }
    summary["success_pct"] = (
        round(100.0 * summary["acks"] / summary["attempts"], 2) if summary["attempts"] else None
    )
    return summary


def bucket_rows(rows: list[dict]) -> list[dict]:
    groups: dict[tuple[int, str], list[dict]] = {}
    for row in rows:
        bin_s = int(row["time_s"])
        groups.setdefault((bin_s, row["phase"]), []).append(row)
    output = []
    for (bin_s, phase), subset in sorted(groups.items()):
        latencies = [row["latency_ms"] for row in subset if row["latency_ms"] is not None]
        ack_lat = [row["latency_ms"] for row in subset if row["ack"] and row["latency_ms"] is not None]
        output.append(
            {
                "bin_s": bin_s,
                "phase": phase,
                "attempts": len(subset),
                "acks": sum(row["ack"] for row in subset),
                "naks": sum(1 for row in subset if row["result"] == "nak"),
                "timeouts": sum(1 for row in subset if row["result"] == "timeout"),
                "closed": sum(1 for row in subset if row["result"] == "closed"),
                "connect_errors": sum(1 for row in subset if row["result"] == "connect_error"),
                "success_pct": round(100.0 * sum(row["ack"] for row in subset) / len(subset), 2),
                "ack_p50_ms": percentile(ack_lat, 0.50),
                "fail_p50_ms": percentile(latencies, 0.50),
                "max_ms": round(max(latencies), 3) if latencies else None,
            }
        )
    return output


# --------------------------------------------------------------------------- #
# Comparison
# --------------------------------------------------------------------------- #
def load_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def read_puts(path: Path) -> list[dict]:
    rows = []
    try:
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                try:
                    rows.append(
                        {
                            "time_s": float(row["time_s"]),
                            "phase": row["phase"],
                            "sequence": int(row["sequence"]),
                            "x": int(row["x"]),
                            "y": int(row["y"]),
                            "color": row["color"],
                            "ack": int(row["ack"]),
                            "result": row["result"],
                            "reason": row["reason"],
                            "latency_ms": float(row["latency_ms"]) if row["latency_ms"] else None,
                        }
                    )
                except (KeyError, ValueError):
                    continue
    except OSError:
        return []
    return rows


def read_timeline_policy(path: Path) -> list[tuple[float, int]]:
    series = []
    try:
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                if not row.get("policy_n"):
                    continue
                try:
                    series.append((float(row["time_s"]), int(row["policy_n"])))
                except ValueError:
                    continue
    except OSError:
        pass
    return series


def fmt_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}%"


def interval_table(summary: dict, rows: list[dict]) -> list[dict]:
    timing = summary.get("timing", {})
    kill = timing.get("kill_time_s") or summary.get("kill_time_s")
    rejoin = timing.get("rejoin_time_s") or summary.get("rejoin_time_s")
    heat_until = timing.get("heat_until_s") or 180.0
    scheduled = [row for row in rows if row["phase"] != "immediate_fault_probe"]
    intervals = [
        ("0-48 s (heat)", 0.0, kill, "heat"),
        ("48-78 s (fault window)", kill, rejoin, "fault_window"),
        ("78-180 s (recovery)", rejoin, heat_until, "recovery"),
    ]
    output = []
    for label, begin, end, phase in intervals:
        if begin is None or end is None:
            output.append({"label": label, "attempts": 0, "acks": 0, "success_pct": None,
                           "p50": None, "p95": None, "issues": "timing unavailable"})
            continue
        subset = [row for row in scheduled if begin <= row["time_s"] < end]
        ack_lat = [row["latency_ms"] for row in subset if row["ack"] and row["latency_ms"] is not None]
        all_lat = [row["latency_ms"] for row in subset if row["latency_ms"] is not None]
        output.append(
            {
                "label": label,
                "attempts": len(subset),
                "acks": sum(row["ack"] for row in subset),
                "success_pct": round(100.0 * sum(row["ack"] for row in subset) / len(subset), 1) if subset else None,
                "p50": percentile(ack_lat, 0.50),
                "p95": percentile(ack_lat, 0.95),
                "fail_p50": percentile(all_lat, 0.50),
                "issues": "",
            }
        )
    return output


def write_comparison(root: Path) -> Path | None:
    b_summary = load_json(root / "B" / "summary.json")
    c_summary = load_json(root / "C" / "summary.json")
    if not b_summary or not c_summary:
        return None
    b_rows = read_puts(root / "B" / "puts.csv")
    c_rows = read_puts(root / "C" / "puts.csv")
    lines: list[str] = []
    lines.append("# P2Place targeted B vs C dual-owner crash — comparison")
    lines.append("")
    lines.append(
        "**Research framing.** This is an intentionally adversarial, favorable-to-C "
        "case study: one heavily written region, its two original N=2 owners killed "
        "together, and availability measured before membership reconfiguration. It is "
        "not an unbiased measurement of overall strategy quality, and a PASS below says "
        "nothing about general workloads."
    )
    lines.append("")

    # ---- validity ----------------------------------------------------- #
    scenario = load_json(root / "scenario.json") or {}
    validity_issues = []
    if b_summary.get("strategy") != "B" or c_summary.get("strategy") != "C":
        validity_issues.append("summary strategy labels are not B/C")
    for key, label in (("seed", "seed"), ("region", "region"), ("original_owners", "original owners")):
        b_value = b_summary.get("scenario", {}).get(key, b_summary.get(key))
        c_value = c_summary.get("scenario", {}).get(key, c_summary.get(key))
        if b_value != c_value:
            validity_issues.append(f"{label} mismatch: B={b_value!r} C={c_value!r}")
    if b_summary.get("initial_placement_gen") != c_summary.get("initial_placement_gen"):
        validity_issues.append(
            "initial placement generation mismatch: "
            f"B={b_summary.get('initial_placement_gen')} "
            f"C={c_summary.get('initial_placement_gen')}"
        )
    if b_summary.get("binary_sha256") != c_summary.get("binary_sha256"):
        validity_issues.append("binary sha256 mismatch between runs")
    if b_summary.get("gateway_alive") is False or c_summary.get("gateway_alive") is False:
        validity_issues.append("gateway node 1 died during a run")

    # ---- verdict ------------------------------------------------------ #
    b_status = b_summary.get("status")
    c_status = c_summary.get("status")
    b_probe = b_summary.get("probe_summary", {})
    c_probe = c_summary.get("probe_summary", {})
    b_fixed = b_probe.get("fixed_generation", 0)
    c_fixed = c_probe.get("fixed_generation", 0)
    b_acks = b_probe.get("fixed_generation_acks", 0)
    c_acks = c_probe.get("fixed_generation_acks", 0)
    c_readback = c_summary.get("readback", {})
    b_valid = b_status == "PASS"
    c_valid = c_status == "PASS"
    verdict = "INCONCLUSIVE"
    verdict_basis = ""
    if validity_issues:
        verdict = "INCONCLUSIVE"
        verdict_basis = "; ".join(validity_issues)
    elif b_status == "INCONCLUSIVE" or c_status == "INCONCLUSIVE":
        verdict = "INCONCLUSIVE"
        verdict_basis = (
            f"B={b_status} ({b_summary.get('status_basis', '')}); "
            f"C={c_status} ({c_summary.get('status_basis', '')})"
        )
    elif b_status == "FAIL" or c_status == "FAIL":
        verdict = "FAIL"
        verdict_basis = (
            f"B={b_status} ({b_summary.get('status_basis', '')}); "
            f"C={c_status} ({c_summary.get('status_basis', '')})"
        )
    elif b_valid and c_valid and b_acks == 0 and c_acks > 0 and c_readback.get("all_visible"):
        verdict = "PASS"
        verdict_basis = (
            f"fixed-generation probes: B {b_acks}/{b_fixed} ACK vs C {c_acks}/{c_fixed}; "
            f"C's {c_readback.get('acked_probes', 0)} ACKed probe pixel(s) read back"
        )
    else:
        verdict = "FAIL"
        verdict_basis = (
            f"valid runs but the predicted contrast was not observed: "
            f"B {b_acks}/{b_fixed} ACK, C {c_acks}/{c_fixed} ACK, "
            f"C read-back all_visible={c_readback.get('all_visible')}"
        )

    lines.append(f"## Verdict: **{verdict}**")
    lines.append("")
    lines.append(verdict_basis)
    lines.append("")

    # ---- scenario ----------------------------------------------------- #
    lines.append("## Shared scenario")
    lines.append("")
    lines.append("| item | value |")
    lines.append("|---|---|")
    lines.append(f"| seed | {scenario.get('seed', b_summary.get('seed'))} |")
    lines.append(f"| target region | {scenario.get('region', b_summary.get('region'))} |")
    lines.append(f"| original owners (killed) | {scenario.get('owners', b_summary.get('original_owners'))} |")
    lines.append(f"| gateway | node 1 (never an original owner) |")
    lines.append(f"| nodes | {scenario.get('nodes', b_summary.get('config', {}).get('nodes'))} |")
    lines.append(f"| initial placement generation | {b_summary.get('initial_placement_gen')} |")
    lines.append(f"| binary sha256 | `{b_summary.get('binary_sha256')}` |")
    b_timing = b_summary.get("timing", {})
    c_timing = c_summary.get("timing", {})
    def event_time(summary: dict, name: str) -> float | None:
        for event in summary.get("events", []):
            if event.get("event") == name and event.get("time_s") is not None:
                return event.get("time_s")
        return None

    lines.append(f"| kill time | B {b_timing.get('kill_time_s')} s / C {c_timing.get('kill_time_s')} s |")
    lines.append(
        f"| rejoin initiated | B {b_timing.get('rejoin_time_s')} s / "
        f"C {c_timing.get('rejoin_time_s')} s |"
    )
    lines.append(
        f"| both owners listening again | B {event_time(b_summary, 'owners_restarted')} s / "
        f"C {event_time(c_summary, 'owners_restarted')} s |"
    )
    lines.append(f"| offered write rate | {b_summary.get('config', {}).get('rate')} PUT/s |")
    lines.append(f"| client ACK definition | matching `pixel` echo after W=2 quorum |")
    lines.append("| N bounds / quorums | min N=2, max N=4; R=2, W=2 |")
    lines.append(
        "| B churn mapping | min reliability >=0.95 -> N2; 0.80-0.95 -> N3; "
        "<0.80 -> N4 (120 s reliability window) |"
    )
    lines.append(
        "| C write mapping | N = max(N_churn, N_write); attempted-write rate "
        ">=0.2/s -> N3, >=1.0/s -> N4 over a 60 s window |"
    )
    lines.append("| policy cadence | evaluate every 2 s; per-region cooldown 3 s |")
    lines.append("")

    # ---- readiness ---------------------------------------------------- #
    lines.append("## Pre-fault certification")
    lines.append("")
    b_ready = b_summary.get("readiness", {})
    c_ready = c_summary.get("readiness", {})
    lines.append("| strategy | certified | target N | routed targets | added complete copies | policy epoch |")
    lines.append("|---|---|---|---|---|---|")
    b_policy_n = b_ready.get("policy_n") or (b_ready.get("latest_policy") or {}).get("assigned_n")
    c_policy = c_ready.get("latest_policy") or {}
    b_added = 0
    c_extra = c_ready.get("additional_replicas") or []
    c_checks = c_ready.get("additional_replica_checks") or []
    c_added = ", ".join(
        f"node {check['node']} (epoch {check.get('join_policy_epoch')})"
        for check in c_checks if check.get("ready")
    ) or "none"
    lines.append(
        f"| B | {b_ready.get('ready')} | {b_policy_n if b_policy_n is not None else 'n/a'} | "
        f"{(b_ready.get('latest_put') or {}).get('targets')} | {b_added} | "
        f"{(b_ready.get('latest_policy') or {}).get('epoch')} |"
    )
    lines.append(
        f"| C | {c_ready.get('ready')} | {c_policy.get('assigned_n', 'n/a')} | "
        f"{(c_ready.get('latest_put') or {}).get('targets')} | {c_added} | "
        f"{c_policy.get('epoch', 'n/a')} |"
    )
    lines.append("")
    if b_ready.get("b_raised_n_before_fault"):
        lines.append(
            "> B had already increased N before the fault; the experiment no longer "
            "isolates the anticipated C advantage (see status_basis)."
        )
    else:
        lines.append(
            "B had **not** increased N before the fault: its routed target set at the gate "
            f"was {(b_ready.get('latest_put') or {}).get('targets')} with policy N="
            f"{b_policy_n}. It first increased N only after the crash "
            "(see fault-window dynamics)."
        )
    c_series = read_timeline_policy(root / "C" / "replication_timeline.csv")
    c_n4_at = next((t for t, n in c_series if n == 4), None)
    if c_n4_at is not None:
        lines.append(
            f"C reached policy N=4 at {c_n4_at} s after the workload started, leaving "
            f"{max(0.0, (c_timing.get('kill_time_s') or 0.0) - c_n4_at):.1f} s of certified "
            "pre-fault margin (including the completed backfills below)."
        )
    if c_checks:
        evidence = "; ".join(
            f"node {check['node']}: join epoch {check.get('join_policy_epoch')}, "
            f"{len(check.get('completed_events') or [])} complete backfill event(s)"
            for check in c_checks
        )
        lines.append(f"Backfill evidence — {evidence}.")
    lines.append("")

    # ---- central probes ----------------------------------------------- #
    lines.append("## Central test: immediate fixed-generation fault window")
    lines.append("")
    lines.append(
        "All probes are issued concurrently immediately after the two original owners "
        "are SIGKILLed. A probe counts only if its routed target set matches the "
        "certified pre-fault set and the placement generation did not change."
    )
    b_skipped = b_summary.get("skipped_collisions")
    c_skipped = c_summary.get("skipped_collisions")
    lines.append("")
    lines.append(
        f"Measurement note: the scheduled heat sequence skips the probe coordinates "
        f"({b_skipped} withheld PUT slots for B, {c_skipped} for C; same deterministic "
        "set) so later heat writes cannot overwrite the ACKed probe pixels before "
        "read-back. The same skip set is applied to both strategies."
    )
    lines.append("")
    def quorum_label(probe_row: dict) -> str:
        if not probe_row.get("routed"):
            return "not routed"
        return (
            f"{'ok' if probe_row.get('server_quorum_ok') else 'fail'}"
            f" {probe_row.get('server_quorum_ack') or ''}".strip()
        )

    lines.append(
        "| probe | B client result | B targets | B server quorum | "
        "C client result | C targets | C server quorum |"
    )
    lines.append("|---|---|---|---|---|---|---|")
    b_probes = b_summary.get("probes", [])
    c_probes = c_summary.get("probes", [])
    for index in range(max(len(b_probes), len(c_probes))):
        b_probe_row = b_probes[index] if index < len(b_probes) else {}
        c_probe_row = c_probes[index] if index < len(c_probes) else {}
        b_label = f"{b_probe_row.get('result')} ({b_probe_row.get('reason', '')})"
        c_label = f"{c_probe_row.get('result')} ({c_probe_row.get('reason', '')})"
        if not b_probe_row.get("fixed_generation"):
            b_label += " *(outside window)*"
        if not c_probe_row.get("fixed_generation"):
            c_label += " *(outside window)*"
        lines.append(
            f"| {index + 1} | {b_label} | {b_probe_row.get('targets')} | "
            f"{quorum_label(b_probe_row)} | {c_label} | {c_probe_row.get('targets')} | "
            f"{quorum_label(c_probe_row)} |"
        )
    lines.append("")
    lines.append(
        f"**Fixed-generation ACK fraction:** B {b_acks}/{b_fixed}; C {c_acks}/{c_fixed}. "
        f"(in-window probes only; B total {b_probe.get('total')}, C total {c_probe.get('total')})"
    )
    lines.append("")

    def probe_latency(summary: dict) -> tuple[str, str]:
        fixed = [
            probe for probe in summary.get("probes", [])
            if probe.get("fixed_generation") and probe.get("latency_ms") is not None
        ]
        ack = [probe["latency_ms"] for probe in fixed if probe["ack"]]
        fail = [probe["latency_ms"] for probe in fixed if not probe["ack"]]
        return (
            f"p50 {percentile(ack, 0.50)} ms / p95 {percentile(ack, 0.95)} ms" if ack else "n/a",
            f"p50 {percentile(fail, 0.50)} ms / p95 {percentile(fail, 0.95)} ms" if fail else "n/a",
        )

    b_ack_lat, b_fail_lat = probe_latency(b_summary)
    c_ack_lat, c_fail_lat = probe_latency(c_summary)
    lines.append(
        f"In-window probe latency: B ACK {b_ack_lat}, NAK/timeout {b_fail_lat}; "
        f"C ACK {c_ack_lat}, NAK/timeout {c_fail_lat}."
    )
    lines.append("")

    # ---- interval table ----------------------------------------------- #
    lines.append("## Interval results (scheduled hot-region workload)")
    lines.append("")
    lines.append("| interval | B attempts | B ACKs | B success | B ack p50 | C attempts | C ACKs | C success | C ack p50 |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    b_table = interval_table(b_summary, b_rows)
    c_table = interval_table(c_summary, c_rows)
    for b_item, c_item in zip(b_table, c_table):
        lines.append(
            f"| {b_item['label']} | {b_item['attempts']} | {b_item['acks']} | "
            f"{fmt_pct(b_item['success_pct'])} | {b_item.get('p50')} ms | "
            f"{c_item['attempts']} | {c_item['acks']} | {fmt_pct(c_item['success_pct'])} | "
            f"{c_item.get('p50')} ms |"
        )
    lines.append(
        f"| 180-300 s (idle) | 0 (by design) | - | - | - | 0 (by design) | - | - | - |"
    )
    lines.append("")
    b_fault_lat = b_summary.get("phases", {}).get("fault_window", {}).get("latency_ms", {})
    c_fault_lat = c_summary.get("phases", {}).get("fault_window", {}).get("latency_ms", {})
    def fail_latency_label(latency: dict) -> str:
        if latency.get("fail_p50") is None:
            return "no NAK/timeout"
        return f"p50 {latency.get('fail_p50')} ms / p95 {latency.get('fail_p95')} ms"

    lines.append(
        f"Failure-only latency in the fault window: B {fail_latency_label(b_fault_lat)}; "
        f"C {fail_latency_label(c_fault_lat)}."
    )
    b_decay = b_summary.get("idle", {}).get("decay_s")
    c_decay = c_summary.get("idle", {}).get("decay_s")
    lines.append(
        "180-300 s has no offered writes by design (the ACK fraction is therefore n/a); "
        f"policy N is sampled every 0.5 s. B returned to N=2 at "
        f"{round(180.0 + b_decay, 1) if b_decay is not None else 'n/a'} s, "
        f"C at {round(180.0 + c_decay, 1) if c_decay is not None else 'n/a'} s."
    )
    lines.append("")
    # ---- fault-window dynamics ---------------------------------------- #
    b_reconf = b_summary.get("placement_reconfigured")
    c_reconf = c_summary.get("placement_reconfigured")
    lines.append("### Fault-window dynamics (scheduled workload)")
    lines.append("")

    def fault_rows(summary: dict, rows: list[dict]) -> list[dict]:
        timing = summary.get("timing", {})
        kill = timing.get("kill_time_s")
        rejoin = timing.get("rejoin_time_s")
        if kill is None or rejoin is None:
            return []
        return [
            row for row in rows
            if row["phase"] != "immediate_fault_probe" and kill <= row["time_s"] < rejoin
        ]

    def dynamics(summary: dict, rows: list[dict], series: list[tuple[float, int]]) -> dict:
        timing = summary.get("timing", {})
        policy = summary.get("policy_summary", {})
        kill = timing.get("kill_time_s")
        pre_n = policy.get("pre_fault_n")
        subset = fault_rows(summary, rows)
        ack_times = sorted(row["time_s"] for row in subset if row["ack"])
        rise = None
        if kill is not None and pre_n is not None:
            rise = next((t for t, n in series if t >= kill and n > pre_n), None)
        if kill is None:
            return {"subset": subset, "first_ack": None, "rise": rise, "pre_gap": (0, 0)}
        end = rise if rise is not None else (timing.get("rejoin_time_s") or kill)
        before = [row for row in subset if row["time_s"] < end]
        return {
            "subset": subset,
            "first_ack": ack_times[0] if ack_times else None,
            "rise": rise,
            "pre_gap": (sum(row["ack"] for row in before), len(before)),
        }

    b_series = read_timeline_policy(root / "B" / "replication_timeline.csv")
    b_dyn = dynamics(b_summary, b_rows, b_series)
    c_dyn = dynamics(c_summary, c_rows, c_series)
    b_kill = b_summary.get("timing", {}).get("kill_time_s")
    c_kill = c_summary.get("timing", {}).get("kill_time_s")

    def duration_label(start: float | None, first_ack: float | None) -> str:
        if start is None:
            return "n/a"
        if first_ack is None:
            return "no ACK in fault window"
        return f"{first_ack - start:.2f} s"

    def rise_label(rise: float | None, kill: float | None) -> str:
        if rise is None:
            return "none (N stable)"
        return f"{rise - kill:.1f} s after crash" if kill is not None else f"{rise:.1f} s"

    lines.append("| item | B | C |")
    lines.append("|---|---|---|")
    lines.append(
        f"| placement generation changed | {b_reconf} | {c_reconf} |"
    )
    lines.append(
        f"| first N increase after crash | {rise_label(b_dyn['rise'], b_kill)} | "
        f"{rise_label(c_dyn['rise'], c_kill)} |"
    )
    lines.append(
        f"| first ACKed scheduled write after kill | "
        f"{b_dyn['first_ack']} s | {c_dyn['first_ack']} s |"
    )
    lines.append(
        f"| scheduled-write ACK gap | {duration_label(b_kill, b_dyn['first_ack'])} | "
        f"{duration_label(c_kill, c_dyn['first_ack'])} |"
    )
    lines.append(
        f"| ACKs before the N increase (or whole fault window) | "
        f"{b_dyn['pre_gap'][0]}/{b_dyn['pre_gap'][1]} | "
        f"{c_dyn['pre_gap'][0]}/{c_dyn['pre_gap'][1]} |"
    )
    lines.append("")
    lines.append(
        "Both runs used the same gateway (node 1), which was not a replica target in "
        "either run; C's 100% ACK fraction in the same window rules out a gateway-side "
        "explanation for B's failures."
    )
    lines.append("")

    def zero_ack_seconds(summary: dict, rows: list[dict]) -> tuple[list[int], int]:
        timing = summary.get("timing", {})
        kill = timing.get("kill_time_s")
        rejoin = timing.get("rejoin_time_s")
        if kill is None or rejoin is None:
            return [], 0
        buckets: dict[int, list[dict]] = {}
        for row in rows:
            if row["phase"] != "immediate_fault_probe" and kill <= row["time_s"] < rejoin:
                buckets.setdefault(int(row["time_s"]), []).append(row)
        zero = sorted(b for b, subset in buckets.items() if not any(r["ack"] for r in subset))
        return zero, len(buckets)

    b_zero, b_seconds = zero_ack_seconds(b_summary, b_rows)
    c_zero, c_seconds = zero_ack_seconds(c_summary, c_rows)

    def zero_label(zero: list[int]) -> str:
        if not zero:
            return "none"
        if len(zero) == zero[-1] - zero[0] + 1:
            return f"{len(zero)} contiguous seconds ({zero[0]}-{zero[-1]})"
        return f"{len(zero)} seconds"

    lines.append(
        f"Per-second write buckets over the {b_seconds}/{c_seconds} fault-window seconds: "
        f"B had zero ACKs in {zero_label(b_zero)}; C had zero ACKs in {zero_label(c_zero)}."
    )
    lines.append("")
    if not b_reconf and not c_reconf:
        lines.append(
            "No placement-generation change occurred in either run: the dead members "
            "stayed suspect/unavailable and reconnected before auto-removal committed. "
            "The recovery contrast is therefore driven by policy N and data placement, "
            "not by membership reconfiguration."
        )
        lines.append("")
    b_fault_policy = b_summary.get("policy_summary", {})
    if b_fault_policy.get("fault_n_max", 0) > 2:
        lines.append(
            f"> B began at N={b_fault_policy.get('fault_n')} in the fault window and later "
            f"raised N to {b_fault_policy.get('fault_n_max')} (churn-driven reaction after the "
            "crash). The table above shows how long that reaction took to restore write ACKs; "
            "it does not protect the immediate post-crash window."
        )
        lines.append("")

    # ---- readback ----------------------------------------------------- #
    lines.append("## Read-after-rejoin correctness")
    lines.append("")
    lines.append("| strategy | ACKed probe pixels | visible by GET | first visible | status |")
    lines.append("|---|---|---|---|---|")
    b_readback = b_summary.get("readback", {})
    lines.append(
        f"| B | {b_readback.get('acked_probes', 0)} | {len(b_readback.get('visible', {}))} | "
        f"{b_readback.get('first_visible_s')} | {b_readback.get('status')} |"
    )
    lines.append(
        f"| C | {c_readback.get('acked_probes', 0)} | {len(c_readback.get('visible', {}))} | "
        f"{c_readback.get('first_visible_s')} | {c_readback.get('status')} |"
    )
    lines.append("")
    if c_readback.get("status") == "blocked":
        lines.append("C read-back was blocked by failed/stalled GETs and is not treated as data loss.")
        lines.append("")

    # ---- overhead / decay --------------------------------------------- #
    lines.append("## Replication overhead and idle decay")
    lines.append("")
    b_policy = b_summary.get("policy_summary", {})
    c_policy = c_summary.get("policy_summary", {})
    lines.append("| item | B | C |")
    lines.append("|---|---|---|")
    lines.append(
        f"| target N pre-fault | {b_policy.get('pre_fault_n')} | {c_policy.get('pre_fault_n')} |"
    )
    def fault_n_label(policy: dict) -> str:
        first = policy.get("fault_n")
        high = policy.get("fault_n_max")
        if first is None:
            return "n/a"
        if high is None or high == first:
            return str(first)
        return f"{first} -> {high}"

    lines.append(
        f"| target N during fault | {fault_n_label(b_policy)} | {fault_n_label(c_policy)} |"
    )
    lines.append(
        f"| mean target N (0-180 s) | {b_policy.get('mean_n_0_180')} | {c_policy.get('mean_n_0_180')} |"
    )
    lines.append(
        f"| extra replica-seconds (0-180 s) | {b_policy.get('extra_replica_seconds_0_180')} | "
        f"{c_policy.get('extra_replica_seconds_0_180')} |"
    )
    lines.append(
        f"| final N at 300 s | {b_policy.get('final_n')} | {c_policy.get('final_n')} |"
    )
    b_idle = b_summary.get("idle", {})
    c_idle = c_summary.get("idle", {})
    lines.append(
        f"| idle decay to N=2 | "
        f"{b_idle.get('decay_s')} s (churn-driven) | "
        f"{c_idle.get('decay_s')} s (write window; ok={c_idle.get('ok')}) |"
    )
    lines.append("")

    # ---- checks ------------------------------------------------------- #
    lines.append("## Validity and correctness checks")
    lines.append("")
    lines.append("| check | B | C |")
    lines.append("|---|---|---|")
    b_readback_check = (
        "n/a (0 ACKed)" if b_readback.get("status") == "not_applicable"
        else "True" if b_readback.get("status") == "ok" else "False"
    )
    c_readback_check = "True" if c_readback.get("status") == "ok" else "False"
    b_idle = b_summary.get("idle", {})
    c_idle = c_summary.get("idle", {})
    checks = [
        ("readiness certified", b_ready.get("ready"), c_ready.get("ready")),
        ("placement stable at gate", b_ready.get("placement_stable"), c_ready.get("placement_stable")),
        ("client ACK at gate", b_ready.get("client_ack"), c_ready.get("client_ack")),
        ("fixed-generation probes >= 6/8", b_probe.get("fixed_generation", 0) >= 6, c_probe.get("fixed_generation", 0) >= 6),
        ("gateway node 1 alive", b_summary.get("gateway_alive"), c_summary.get("gateway_alive")),
        ("all ACKed probes readable", b_readback_check, c_readback_check),
        (
            "idle decay to N=2",
            f"{b_idle.get('decay_s')} s (churn)" if b_idle.get("ok") else "not reached",
            f"{c_idle.get('decay_s')} s (write window)" if c_idle.get("ok") else "not reached",
        ),
    ]
    for label, b_value, c_value in checks:
        lines.append(f"| {label} | {b_value} | {c_value} |")
    lines.append("")
    if validity_issues:
        lines.append("**Validity issues:** " + "; ".join(validity_issues))
        lines.append("")
    lines.append("## Run status")
    lines.append("")
    lines.append(f"- B: `{b_status}` — {b_summary.get('status_basis', '')}")
    lines.append(f"- C: `{c_status}` — {c_summary.get('status_basis', '')}")
    lines.append(f"- Overall: `{verdict}` — {verdict_basis}")
    lines.append("")

    lines.append("## Artifacts")
    lines.append("")
    lines.append(f"Everything below is under `{root}`:")
    lines.append("")
    lines.append("- `scenario.json` — shared region/owners/placement/binary manifest")
    lines.append("- `B/` and `C/`: `config.json`, `summary.json`, `readiness.json`, `puts.csv`,")
    lines.append(
        "  `replication_timeline.csv`, `write_buckets.csv`, `gets.csv`, `readback.json`, "
        "`ws_raw.jsonl`, `events.csv`, `node1..10/node.log`"
    )
    lines.append(
        "- `B/node1/policy_node1.csv` and `C/node1/policy_node1.csv` — per-eval policy rows; "
        "`C/node5|node6/replica_repair_events.csv` — completed-backfill evidence"
    )
    lines.append(
        "- Regenerate this file with `python3 experiments/run_targeted_bc.py "
        f"--compare-only --seed {scenario.get('seed', b_summary.get('seed'))} "
        f"--out-root {root.parent}`"
    )
    lines.append("")

    path = root / "comparison.md"
    path.write_text("\n".join(lines))
    return path


def policy_summary(summary: dict, timeline_rows: list[dict]) -> dict:
    """Mean N / extra replica-seconds from the replication timeline."""
    timing = summary.get("timing", {})
    kill = timing.get("kill_time_s")
    rejoin = timing.get("rejoin_time_s")
    heat_until = timing.get("heat_until_s", 180.0)
    values = [
        (row["time_s"], row["policy_n"])
        for row in timeline_rows
        if row.get("policy_n") is not None and row["time_s"] <= heat_until
    ]
    if not values:
        return {}
    mean_n = sum(n for _, n in values) / len(values)
    extra_seconds = sum(max(0, n - 2) for _, n in values) * 0.5
    pre_fault = next((n for t, n in reversed(values) if kill is not None and t < kill), None)
    if kill is not None and rejoin is not None:
        fault_values = [n for t, n in values if kill <= t < rejoin]
    else:
        fault_values = []
    fault_first = fault_values[0] if fault_values else None
    fault_max = max(fault_values) if fault_values else None
    final_policy = summary.get("final_policy") or {}
    final = final_policy.get("assigned_n", values[-1][1])
    return {
        "pre_fault_n": pre_fault,
        "fault_n": fault_first,
        "fault_n_max": fault_max,
        "mean_n_0_180": round(mean_n, 3),
        "extra_replica_seconds_0_180": round(extra_seconds, 2),
        "final_n": final,
    }


def idle_summary(timeline_rows: list[dict], heat_until: float) -> dict:
    idle = [row for row in timeline_rows if row["time_s"] >= heat_until and row.get("policy_n") is not None]
    if not idle:
        return {"decay_s": None, "ok": None}
    decay = None
    for row in idle:
        if row["policy_n"] == 2:
            decay = round(row["time_s"] - heat_until, 1)
            break
    return {"decay_s": decay, "ok": decay is not None, "final_n": idle[-1]["policy_n"]}


# --------------------------------------------------------------------------- #
# Main experiment
# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# Dashboard export — same canonical files/columns as run_experiment_A_vs_B.py
# --------------------------------------------------------------------------- #
# IMPORTANT: these are post-run derivatives; never change workload, faults,
# quorums or the existing targeted-study evidence to generate dashboard data.
DASHBOARD_OPERATION_COLUMNS = (
    'attempt', 'time_s', 'phase', 'workload_class', 'via_node', 'region',
    'x', 'y', 'color', 'success', 'latency_ms', 'error', 'error_category',
)
DASHBOARD_REGION_COLUMNS = (
    'run_id', 'time_s', 'event_type', 'region', 'old_n', 'new_n',
    'placement_gen', 'placement_fp', 'policy_epoch', 'authority_node', 'reason',
)
DASHBOARD_NODE_COLUMNS = (
    'run_id', 'time_s', 'node_id', 'process_running', 'membership_state',
    'health', 'placement_gen', 'placement_fp', 'policy_epoch', 'assigned_regions',
    'n2_regions', 'n3_regions', 'n4_regions', 'avg_target_n',
    'desired_region_copies', 'connected_assigned_regions',
)
DASHBOARD_CLUSTER_COLUMNS = (
    'run_id', 'time_s', 'placement_gen', 'placement_fp', 'policy_epoch',
    'n2_regions', 'n3_regions', 'n4_regions', 'avg_target_n',
    'total_desired_copies', 'total_assigned_copies', 'underassigned_regions',
    'processes_running', 'members_committed',
)


def _dashboard_csv_rows(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(newline='') as handle:
        return list(csv.DictReader(handle))


def _dashboard_write_csv(path: Path, fields, records) -> None:
    with path.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(records)


def _dashboard_error_category(error: str) -> str:
    if not error:
        return 'none'
    error = error.lower()
    if 'connection' in error or 'connect' in error or 'unreachable' in error:
        return 'gateway_connection'
    if 'quorum' in error or 'replica' in error:
        return 'quorum_or_replica'
    if 'timeout' in error or 'timed out' in error or 'no matching' in error:
        return 'timeout_or_missing_ack'
    if 'closed' in error or 'reset' in error:
        return 'connection_closed'
    return 'other'


def _dashboard_state_full(out: Path, node: int):
    """Parse one peer's actual committed placement state (V1/V2 layout)."""
    options = [
        out / f'node{node}.state',                                  # A/B-style
        out / f'node{node}' / 'state' / f'node{node}' / 'placement.state',
        out / f'node{node}' / 'placement.state',
    ]
    for option in options:
        if not option.exists():
            continue
        try:
            fields = option.read_text().split()
            if not fields or fields[0] not in ('P2PLACE_BALANCED_V1', 'P2PLACE_BALANCED_V2'):
                continue
            generation, count = int(fields[1]), int(fields[2])
            members = tuple(map(int, fields[3:3 + count]))
            offset = 3 + count * (2 if fields[0].endswith('V2') else 1)
            owners = tuple(map(int, fields[offset:offset + 256]))
            if len(owners) == 256:
                return {'format': fields[0], 'generation': generation,
                        'members': members, 'owners': owners, 'path': str(option)}
        except (OSError, ValueError, IndexError):
            continue
    return None


def _dashboard_state(out: Path, nodes: int):
    """Read the freshest actual committed fixed-owner table; never invent owners."""
    best = None
    for node in range(1, nodes + 1):
        state = _dashboard_state_full(out, node)
        if state and (best is None or state['generation'] > best['generation']):
            best = state
    return best['owners'] if best else None


def _dashboard_raw_metrics(out: Path, filename: str):
    """Collect actual metrics across peers/boots; do not infer missing events."""
    paths = list(out.glob(f'node[0-9]*/{filename}'))
    paths += list(out.glob(f'node[0-9]*/boot*_metrics/{filename}'))
    paths += list(out.glob(f'metrics/node[0-9]*_boot*/{filename}'))
    for path in sorted(set(paths)):
        if filename.endswith('.jsonl'):
            if path.exists():
                for line in path.read_text(errors='replace').splitlines():
                    try:
                        yield json.loads(line)
                    except (ValueError, TypeError):
                        continue
        else:
            for record in _dashboard_csv_rows(path):
                yield record


def _dashboard_process_events(out: Path, summary: dict, config: dict, nodes: int):
    """Return (events, source, warnings) for node_process_events.csv.

    New runs record launch/kill/restart at source with real PIDs and monotonic
    times.  Legacy exports reconstruct the one known crash/rejoin pair and mark
    the provenance; no PID or exact launch time is invented.
    """
    source = summary.get('process_events_source')
    path = out / 'node_process_events.csv'
    if source in ('recorded', 'startup_aborted') and path.exists():
        events = []
        for row in _dashboard_csv_rows(path):
            try:
                events.append({'time_s': float(row['time_s']),
                               'node_id': int(row['node_id']),
                               'boot': int(row['boot']),
                               'action': str(row['action']),
                               'pid': row.get('pid', '')})
            except (KeyError, ValueError):
                continue
        if events:
            notes = []
            if source == 'startup_aborted':
                notes.append('startup aborted before the workload anchor; real launch '
                             'rows are present but time_s is not workload-relative')
            return events, source, notes

    pair = [int(x) for x in (summary.get('original_owners') or [])]
    killed = float(summary.get('kill_time_s') or config.get('kill_at') or 0)
    rejoined = float(summary.get('rejoin_time_s') or config.get('rejoin_at') or 0)
    complete = summary.get('timing', {}).get('runtime_s') is not None
    events = ([{'time_s': 0.0, 'node_id': n, 'boot': 1, 'action': 'launch', 'pid': ''}
               for n in range(1, nodes + 1)] if complete else [])
    warnings = []
    if complete and pair and 0 < killed < rejoined:
        events += [{'time_s': killed, 'node_id': n, 'boot': 1, 'action': 'kill', 'pid': ''}
                   for n in pair]
        events += [{'time_s': rejoined, 'node_id': n, 'boot': 2, 'action': 'launch', 'pid': ''}
                   for n in pair]
    elif complete:
        warnings.append('No verified kill/rejoin timing; node process timeline is incomplete.')
    events.sort(key=lambda e: (e['time_s'], e['node_id'], e['boot']))
    return events, source or 'reconstructed_legacy', warnings


def _dashboard_health(out: Path, nodes: int, process_events: list[dict]) -> list[dict]:
    """Align peer_health rows to the workload clock using real per-boot launches."""
    launch_t = {(e['node_id'], e['boot']): e['time_s']
                for e in process_events if e['action'] == 'launch'}
    final_boot = {}
    for e in process_events:
        if e['action'] == 'launch':
            final_boot[e['node_id']] = max(final_boot.get(e['node_id'], 0), e['boot'])
    paths = []
    paths += list(out.glob('metrics/node[0-9]*_boot*/peer_health_node*.csv'))
    paths += list(out.glob('node[0-9]*/boot*_metrics/peer_health_node*.csv'))
    paths += list(out.glob('node[0-9]*/peer_health_node*.csv'))
    records = []
    seen = set()
    for path in sorted(set(paths)):
        parts = path.parts
        node = boot = None
        if 'metrics' in parts:
            try:
                fragment = parts[parts.index('metrics') + 1]
                node = int(fragment.split('_boot')[0].replace('node', ''))
                boot = int(fragment.split('_boot')[1])
            except (IndexError, ValueError):
                continue
        elif 'boot1_metrics' in parts:
            try:
                node = int(path.parent.parent.name.replace('node', ''))
                boot = 1
            except ValueError:
                continue
        else:
            try:
                node = int(path.parent.name.replace('node', ''))
                boot = final_boot.get(node, 1)
            except ValueError:
                continue
        anchor_t = launch_t.get((node, boot))
        if anchor_t is None:
            continue
        try:
            for row in _dashboard_csv_rows(path):
                t = anchor_t + float(row['time_s'])
                state = str(row.get('state', 'UNKNOWN'))
                score = row.get('availability_score', '')
                key = (node, boot, round(t, 6), state, score)
                if key in seen:
                    continue
                seen.add(key)
                records.append({'node': node, 'boot': boot, 't': t,
                                'state': state, 'score': score})
        except (OSError, ValueError):
            continue
    records.sort(key=lambda r: (r['t'], r['node']))
    return records


def _dashboard_backfill_rows(out: Path, anchor_ns: int | None, run_id: str) -> list[dict]:
    """Convert completed/failed backfills to the dashboard root CSV.

    ``replica_repair_events.csv`` also contains urgent_enqueued/started/retry
    rows.  The dashboard's backfill chart means completion/failure markers, so
    only terminal events are exported.  The C++ ``replica_backfill_events.csv``
    emits the same completion through a second code path; dedupe by
    (node, region, placement_gen, policy_epoch, result), keeping the first time.
    """
    rows = []
    seen = set()

    def add(node_id, region, gen, epoch, result, ns, reason):
        key = (int(node_id), int(region), int(gen), int(epoch), str(result))
        if key in seen:
            return
        seen.add(key)
        rows.append({
            'run_id': run_id, 'time_s': round((int(ns) - anchor_ns) / 1e9, 6),
            'node_id': int(node_id), 'region': int(region),
            'placement_gen': int(gen), 'policy_epoch': int(epoch),
            'result': str(result), 'reason': str(reason),
        })

    if anchor_ns is not None:
        # The C++ replica_backfill_events.csv is already the canonical
        # completion/failure stream.  The rich replica_repair_events.csv is a
        # targeted diagnostic stream (started/retry/urgent too) and is used only
        # as a fallback when the canonical stream is absent.
        canonical = list(_dashboard_raw_metrics(out, 'replica_backfill_events.csv'))
        if canonical:
            for record in canonical:
                try:
                    add(record['node_id'], record['region'], record['placement_gen'],
                        record['policy_epoch'], str(record.get('result', '')),
                        record['monotonic_ns'], str(record.get('reason', '')))
                except (KeyError, ValueError, TypeError):
                    continue
        else:
            for record in _dashboard_raw_metrics(out, 'replica_repair_events.csv'):
                try:
                    event = str(record.get('event', ''))
                    if event not in ('backfill_completed', 'backfill_failed'):
                        continue
                    classification = str(record.get('classification', ''))
                    successful = event == 'backfill_completed' and classification == 'complete'
                    add(record['node_id'], record['region'], record['placement_gen'],
                        record['policy_epoch'], 'ok' if successful else 'failed',
                        record['monotonic_ns'], f'{event} {classification}'.strip())
                except (KeyError, ValueError, TypeError):
                    continue
    rows.sort(key=lambda r: (r['time_s'], r['node_id'], r['region'], r['result']))
    return rows


def _dashboard_final_placement(out: Path, nodes: int, placement_events: list[dict],
                               warnings: list[str]) -> dict:
    final = {}
    latest = placement_events[-1] if placement_events else None
    for node in range(1, nodes + 1):
        state = _dashboard_state_full(out, node)
        if state:
            final[str(node)] = [state['generation'], list(state['members'])]
        elif latest:
            final[str(node)] = [int(latest['placement_gen']),
                                [int(x) for x in latest['member_ids']]]
            warnings.append(
                f'node{node}.state missing; final membership taken from the latest '
                'committed placement snapshot (not a per-node state file).')
    return final


def _dashboard_profile_files(out: Path, nodes: int) -> dict[str, Path]:
    """Map a clean dashboard ZIP's arcnames to real result files."""
    mapping = {}
    for name in ('summary.json', 'config.json', 'fault_plan.json',
                 'runner_operations.csv', 'node_process_events.csv',
                 'region_replication_events.csv', 'placement_snapshots.jsonl',
                 'replication_summary.csv', 'node_replication_timeline.csv',
                 'replica_backfill_events.csv'):
        path = out / name
        if path.exists():
            mapping[name] = path
    for node in range(1, nodes + 1):
        state = None
        for candidate in (out / f'node{node}.state',
                          out / f'node{node}' / 'state' / f'node{node}' / 'placement.state',
                          out / f'node{node}' / 'placement.state'):
            if candidate.exists():
                state = candidate
                break
        if state is not None:
            mapping[f'node{node}.state'] = state
        for log in sorted(out.glob(f'node{node}_boot*.log')):
            mapping[log.name] = log
    if not any(name.endswith('.log') for name in mapping):
        for node in range(1, nodes + 1):
            legacy = out / f'node{node}' / 'node.log'
            if legacy.exists():
                mapping[f'node{node}.log'] = legacy
    metrics_paths = list(out.glob('metrics/node[0-9]*_boot*/*'))
    metrics_paths += list(out.glob('node[0-9]*/boot*_metrics/*'))
    if metrics_paths:
        for path in sorted(set(metrics_paths)):
            if not path.is_file() or path.suffix not in ('.csv', '.jsonl'):
                continue
            parts = path.parts
            if 'metrics' in parts:
                boot_dir = parts[parts.index('metrics') + 1]
            else:
                node_dir = path.parent.parent.name
                boot_dir = f'{node_dir}_boot1'
            mapping[f'metrics/{boot_dir}/{path.name}'] = path
    else:
        for node in range(1, nodes + 1):
            live = out / f'node{node}'
            for path in sorted(live.glob('*.csv')) + sorted(live.glob('*.jsonl')):
                mapping[f'metrics/node{node}_boot1/{path.name}'] = path
    return mapping


def _write_dashboard_zip(path: Path, mapping: dict[str, Path], prefix: str = '') -> None:
    import zipfile
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for arcname, source in sorted(mapping.items()):
            info = zipfile.ZipInfo(f'{prefix}{arcname}', date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, source.read_bytes())


def build_dashboard_bundle(root: Path, seed: int, nodes: int = 10) -> dict:
    """Write B/C individual ZIPs and a combined two-profile ZIP, deterministically."""
    result = {'zips': [], 'warnings': [], 'complete': True}
    profiles = {}
    for strategy in ('B', 'C'):
        out = root / strategy
        if not (out / 'summary.json').is_file():
            continue
        mapping = _dashboard_profile_files(out, nodes)
        profiles[strategy] = mapping
        zip_path = root / f'{strategy}_targeted_seed{seed}.zip'
        try:
            # A top-level strategy folder matters: the dashboard's readZip
            # excludes nested metrics/*/ replication files only when the path
            # contains '/metrics/'.  A root-level metrics/ folder would be read
            # a second time alongside the canonical root CSVs.
            _write_dashboard_zip(zip_path, mapping, prefix=f'{strategy}/')
            result['zips'].append(str(zip_path))
        except Exception as exc:  # noqa: BLE001
            result['warnings'].append(f'{strategy} ZIP failed: {exc}')
            result['complete'] = False
    if set(profiles) == {'B', 'C'}:
        combined = root / f'targeted_BC_seed{seed}.zip'
        mapping = {}
        for strategy, files in profiles.items():
            for arcname, source in files.items():
                mapping[f'{strategy}/{arcname}'] = source
        try:
            _write_dashboard_zip(combined, mapping)
            result['zips'].append(str(combined))
        except Exception as exc:  # noqa: BLE001
            result['warnings'].append(f'combined ZIP failed: {exc}')
            result['complete'] = False
    return result


def export_targeted_dashboard(out: Path, *, sample_interval: float = 1.0) -> dict:
    """Export A/B dashboard-compatible files from one finished B/C run.

    The targeted scientific files are never altered.  The merged summary is
    rebuilt from ``summary.targeted.json`` (the pre-export original) so repeated
    exports are deterministic.  Missing evidence is marked, not invented.
    """
    summary_path = out / 'summary.json'
    current = load_json(summary_path)
    if not isinstance(current, dict):
        raise FileNotFoundError(f'missing summary.json: {summary_path}')
    if sample_interval <= 0:
        raise ValueError('sample_interval must be positive')

    targeted_path = out / 'summary.targeted.json'
    if not targeted_path.exists():
        # Preserve the original targeted summary before any compatibility keys
        # are merged into summary.json.
        targeted_path.write_text(summary_path.read_text())
    summary = load_json(targeted_path) or current
    if not isinstance(summary, dict):
        raise ValueError(f'invalid targeted summary: {targeted_path}')

    config = summary.get('config') or {}
    strategy = str(summary.get('strategy') or current.get('strategy') or '?')
    seed = int(summary.get('seed') or current.get('seed') or 0)
    run_id = f'{strategy}_seed{seed}'
    nodes = int(config.get('nodes') or summary.get('nodes') or 10)
    requested_duration = float(config.get('seconds') or summary.get('duration_s') or 300.0)
    timing = summary.get('timing') or {}
    runtime_s = timing.get('runtime_s')
    if runtime_s is None:
        runtime_s = summary.get('aborted_runtime_s')
    warnings: list[str] = []
    errors: list[str] = []
    duration = requested_duration
    duration_source = 'configured'
    partial_run = False
    if runtime_s is None:
        duration = 0.0
        duration_source = 'unknown_aborted'
        partial_run = True
        errors.append('no runtime evidence for this run; timeline limited to t=0')
    else:
        runtime_s = float(runtime_s)
        ended_full = abs(runtime_s - requested_duration) <= max(5.0, requested_duration * 0.02)
        if not ended_full:
            duration = max(0.0, min(requested_duration, runtime_s))
            duration_source = 'aborted_runtime'
            partial_run = True
    time_base = summary.get('time_base') or {}
    anchor_ns = time_base.get('workload_start_monotonic_ns')
    if anchor_ns is None and timing.get('start_monotonic') is not None:
        anchor_ns = round(float(timing['start_monotonic']) * 1_000_000_000)
    anchor_ns = int(anchor_ns) if anchor_ns is not None else None
    if anchor_ns is None:
        errors.append('no workload time_base anchor: replication timeline cannot be aligned')

    # ------------------------------------------------------------------ #
    # 1. Canonical per-PUT operations (client-visible ACK semantics).
    # ------------------------------------------------------------------ #
    puts = _dashboard_csv_rows(out / 'puts.csv')
    if not puts:
        errors.append('puts.csv missing or empty: no runner_operations rows')
    operations = []
    for index, entry in enumerate(
            sorted(puts, key=lambda r: (float(r.get('time_s') or 0),
                                        int(r.get('sequence') or 0))), 1):
        successful = str(entry.get('ack', '0')).strip() in ('1', 'True', 'true')
        reason = '' if successful else (entry.get('reason') or entry.get('result') or 'unknown')
        phase = str(entry.get('phase') or '')
        operations.append(dict(
            attempt=index, time_s=entry.get('time_s', ''), phase=phase,
            workload_class=('fault_probe' if phase == 'immediate_fault_probe' else 'targeted'),
            via_node=1, region=entry.get('region', ''),
            x=entry.get('x', ''), y=entry.get('y', ''),
            color=entry.get('color', ''), success=successful,
            latency_ms=entry.get('latency_ms', ''), error=reason,
            error_category=_dashboard_error_category(reason),
        ))
    _dashboard_write_csv(out / 'runner_operations.csv', DASHBOARD_OPERATION_COLUMNS, operations)
    if not operations:
        errors.append('runner_operations.csv would be empty')

    # ------------------------------------------------------------------ #
    # 2. Process events + planned/actual faults.
    # ------------------------------------------------------------------ #
    process_events, process_source, process_warnings = _dashboard_process_events(
        out, summary, config, nodes)
    warnings.extend(process_warnings)
    if process_source != 'recorded':
        _dashboard_write_csv(
            out / 'node_process_events.csv',
            ('time_s', 'node_id', 'boot', 'action', 'pid'),
            [{**event, 'time_s': f"{float(event['time_s']):.3f}"}
             for event in sorted(process_events,
                                 key=lambda e: (float(e['time_s']), e['node_id'], e['boot']))],
        )
        warnings.append(
            f'node_process_events.csv provenance={process_source}: launch times/PIDs '
            'are reconstructed or unavailable, not recorded at source.')
    if not process_events:
        errors.append('node_process_events.csv has no rows')

    pair = [int(x) for x in (summary.get('original_owners') or [])]
    planned_kill = float(config.get('kill_at') or summary.get('kill_at') or 0.0)
    planned_rejoin = float(config.get('rejoin_at') or summary.get('rejoin_time_s') or 0.0)
    fault_plan = []
    if pair and planned_kill > 0 and planned_rejoin > planned_kill:
        fault_plan = ([{'at_s': planned_kill, 'action': 'kill', 'node': n} for n in pair] +
                      [{'at_s': planned_rejoin, 'action': 'restart', 'node': n} for n in pair])
    else:
        warnings.append('planned kill/rejoin timings unavailable; fault_plan.json is empty')
    (out / 'fault_plan.json').write_text(json.dumps(fault_plan, indent=2) + '\n')

    executed = []
    kill_events = {int(e['node_id']): float(e['time_s'])
                   for e in process_events if e['action'] == 'kill'}
    restart_events = {int(e['node_id']): float(e['time_s'])
                      for e in process_events
                      if e['action'] == 'launch' and int(e.get('boot') or 0) > 1}
    legacy_kill = summary.get('kill_time_s')
    legacy_rejoin = summary.get('rejoin_time_s')
    for node in pair:
        actual_kill = kill_events.get(node)
        actual_restart = restart_events.get(node)
        if actual_kill is None and process_source != 'recorded' and legacy_kill is not None:
            actual_kill = float(legacy_kill)
        if actual_restart is None and process_source != 'recorded' and legacy_rejoin is not None:
            actual_restart = float(legacy_rejoin)
        if actual_kill is not None:
            executed.append({'planned_at_s': planned_kill,
                             'actual_at_s': round(float(actual_kill), 3),
                             'action': 'kill', 'node': node})
        if actual_restart is not None:
            executed.append({'planned_at_s': planned_rejoin,
                             'actual_at_s': round(float(actual_restart), 3),
                             'action': 'restart', 'node': node})
    executed.sort(key=lambda e: (e['actual_at_s'], e['node'], e['action']))
    if pair and not executed:
        warnings.append('no actual kill/restart events found; executed_faults is empty')

    # ------------------------------------------------------------------ #
    # 3. Accepted policy events (deduped, time aligned, monotone epochs).
    # ------------------------------------------------------------------ #
    raw_events = []
    malformed_events = 0
    if anchor_ns is not None:
        for record in _dashboard_raw_metrics(out, 'region_replication_events.csv'):
            try:
                ns = int(record['monotonic_ns'])
                gen = int(record['placement_gen'])
                kind = record['event_type']
                if kind not in ('initial', 'n_change', 'placement_change'):
                    continue
                raw_events.append((ns, gen, record))
            except (KeyError, ValueError, TypeError):
                malformed_events += 1
    raw_events.sort(key=lambda item: (item[0], item[1]))
    dedup = set()
    policy_events = []
    for ns, gen, record in raw_events:
        key = (record.get('event_type'), record.get('region'), record.get('old_n'),
               record.get('new_n'), record.get('placement_gen'),
               record.get('placement_fp'), record.get('policy_epoch'))
        if key in dedup:
            continue
        dedup.add(key)
        policy_events.append({**record, 'run_id': run_id,
                              'time_s': round((ns - anchor_ns) / 1e9, 6)})
    policy_events.sort(key=lambda e: (float(e['time_s']),
                                      int(e.get('placement_gen') or 0),
                                      int(e.get('policy_epoch') or 0)))
    stale_dropped = 0
    last_epoch: dict[int, int] = {}
    filtered_events = []
    for event in policy_events:
        region = event.get('region')
        if event.get('event_type') in ('initial', 'n_change') and region not in (None, ''):
            try:
                region_i = int(region)
                epoch_i = int(event.get('policy_epoch'))
            except (TypeError, ValueError):
                filtered_events.append(event)
                continue
            if epoch_i < last_epoch.get(region_i, -1):
                stale_dropped += 1
                continue
            last_epoch[region_i] = epoch_i
        filtered_events.append(event)
    policy_events = filtered_events
    _dashboard_write_csv(out / 'region_replication_events.csv',
                         DASHBOARD_REGION_COLUMNS, policy_events)
    if not policy_events:
        errors.append('region_replication_events.csv has no accepted events')

    # ------------------------------------------------------------------ #
    # 4. Committed placement snapshots.
    # ------------------------------------------------------------------ #
    snapshots = {}
    malformed_snapshots = 0
    if anchor_ns is not None:
        for record in _dashboard_raw_metrics(out, 'placement_snapshots.jsonl'):
            try:
                tns = int(record['monotonic_ns'])
                gen = int(record['placement_gen'])
                members = tuple(int(n) for n in record['member_ids'])
                key = (gen, record.get('placement_fp'), tuple(sorted(members)))
                obj = {**record, 'time_s': round((tns - anchor_ns) / 1e9, 6)}
                previous = snapshots.get(key)
                if previous is None or obj['time_s'] < previous['time_s']:
                    snapshots[key] = obj
            except (KeyError, ValueError, TypeError):
                malformed_snapshots += 1
    placement_events = sorted(snapshots.values(), key=lambda e: e['time_s'])
    with (out / 'placement_snapshots.jsonl').open('w') as handle:
        for event in placement_events:
            handle.write(json.dumps(event) + '\n')
    if not placement_events:
        errors.append('placement_snapshots.jsonl has no committed snapshots')

    # ------------------------------------------------------------------ #
    # 5. Backfill events, owner table, final placement, health.
    # ------------------------------------------------------------------ #
    backfill_rows = _dashboard_backfill_rows(out, anchor_ns, run_id)
    _dashboard_write_csv(
        out / 'replica_backfill_events.csv',
        ('run_id', 'time_s', 'node_id', 'region', 'placement_gen',
         'policy_epoch', 'result', 'reason'),
        backfill_rows,
    )
    if not backfill_rows:
        warnings.append('replica_backfill_events.csv has no rows (optional evidence missing)')

    owners = _dashboard_state(out, nodes)
    if owners is None:
        errors.append('missing committed fixed-owner table; per-node assignments unavailable')
    final_placement = _dashboard_final_placement(out, nodes, placement_events, warnings)
    if not final_placement:
        errors.append('final_placement unavailable; placement view will be empty')
    health = _dashboard_health(out, nodes, process_events)
    if not health:
        warnings.append('peer-health history unavailable; node timeline health is UNKNOWN')

    # ------------------------------------------------------------------ #
    # 6. 1 Hz cluster/node replication timelines.
    # ------------------------------------------------------------------ #
    target = [2] * 256  # B/C documented minimum until actual initial events
    initial_events = [e for e in policy_events if e.get('event_type') == 'initial']
    if not initial_events:
        warnings.append('no accepted initial policy events; using documented N=2 fallback')
    pe = me = 0
    latest_epoch = ''
    last_members: tuple[int, ...] = ()
    last_gen = ''
    last_fp = ''
    conflicts = 0
    other_regions = 0
    assignment_owner_missing = False
    cluster_rows = []
    node_rows = []

    proc_intervals: dict[int, list[tuple[float, bool]]] = {}
    for event in process_events:
        proc_intervals.setdefault(int(event['node_id']), []).append(
            (float(event['time_s']), event['action'] == 'launch'))
    for node in proc_intervals:
        proc_intervals[node].sort(key=lambda item: item[0])

    def running(node: int, t: float) -> bool:
        values = proc_intervals.get(node, [])
        return next((value for at, value in reversed(values) if at <= t), False)

    health_by_node: dict[int, list[tuple[float, str]]] = {}
    for record in health:
        health_by_node.setdefault(int(record['node']), []).append(
            (float(record['t']), str(record['state'])))
    for node in health_by_node:
        health_by_node[node].sort(key=lambda item: item[0])

    def health_at(node: int, t: float) -> str:
        values = health_by_node.get(node, [])
        latest = None
        for at, state in values:
            if at > t:
                break
            latest = state
        return latest or 'UNKNOWN'

    n_samples = int(round(duration / sample_interval)) + 1
    for index in range(n_samples):
        t = round(index * sample_interval, 6)
        while pe < len(policy_events) and float(policy_events[pe]['time_s']) <= t:
            event = policy_events[pe]
            if event.get('event_type') in ('initial', 'n_change'):
                try:
                    region = int(event['region'])
                    new_n = int(event['new_n'])
                    if 0 <= region < 256 and new_n > 0:
                        if event.get('event_type') == 'n_change' and event.get('old_n') not in (None, ''):
                            try:
                                if target[region] != int(event['old_n']):
                                    conflicts += 1
                            except (TypeError, ValueError):
                                pass
                        target[region] = new_n
                        latest_epoch = event.get('policy_epoch', latest_epoch)
                except (TypeError, ValueError, KeyError):
                    pass
            pe += 1
        while me < len(placement_events) and float(placement_events[me]['time_s']) <= t:
            event = placement_events[me]
            last_members = tuple(sorted(int(n) for n in event['member_ids']))
            last_gen = event['placement_gen']
            last_fp = event.get('placement_fp', '')
            me += 1

        counts = [target.count(n) for n in (2, 3, 4)]
        other_regions = 256 - sum(counts)
        desired = sum(target)
        per_node = {n: [0, 0, 0, 0, 0] for n in range(1, nodes + 1)}
        assigned_total = 0
        underassigned = 0
        assignments_available = owners is not None and bool(last_members)
        if assignments_available:
            for region, desired_n in enumerate(target):
                assigned_n = min(desired_n, len(last_members))
                assigned_total += assigned_n
                underassigned += int(assigned_n < desired_n)
                try:
                    owner_index = last_members.index(owners[region])
                except ValueError:
                    assignments_available = False
                    assignment_owner_missing = True
                    break
                for offset in range(assigned_n):
                    node = last_members[(owner_index + offset) % len(last_members)]
                    info = per_node[node]
                    info[0] += 1
                    if desired_n in (2, 3, 4):
                        info[desired_n - 1] += 1
                    info[4] += desired_n
        if not assignments_available:
            assigned_total = ''
            underassigned = ''

        node_assigned_sum = sum(per_node[n][0] for n in range(1, nodes + 1))
        cluster_rows.append(dict(
            run_id=run_id, time_s=f'{t:.1f}', placement_gen=last_gen,
            placement_fp=last_fp, policy_epoch=latest_epoch,
            n2_regions=counts[0], n3_regions=counts[1], n4_regions=counts[2],
            avg_target_n=f'{desired / 256:.4f}', total_desired_copies=desired,
            total_assigned_copies=assigned_total,
            underassigned_regions=underassigned,
            processes_running=sum(bool(running(n, t)) for n in range(1, nodes + 1)),
            members_committed=len(last_members),
        ))
        for node in range(1, nodes + 1):
            assigned, n2, n3, n4, nsum = per_node[node]
            proc_running = bool(running(node, t))
            node_rows.append(dict(
                run_id=run_id, time_s=f'{t:.1f}', node_id=node,
                process_running=proc_running,
                membership_state=('member' if node in last_members else 'removed')
                                if last_members else 'unknown',
                health=health_at(node, t), placement_gen=last_gen,
                placement_fp=last_fp, policy_epoch=latest_epoch,
                assigned_regions=(assigned if assignments_available else ''),
                n2_regions=(n2 if assignments_available else ''),
                n3_regions=(n3 if assignments_available else ''),
                n4_regions=(n4 if assignments_available else ''),
                avg_target_n=(f'{nsum / assigned:.4f}' if assigned else ''),
                desired_region_copies=(nsum if assignments_available else ''),
                connected_assigned_regions=(
                    assigned if proc_running and assignments_available
                    else 0 if assignments_available else ''),
            ))
        if assignments_available and node_assigned_sum != assigned_total:
            warnings.append(
                f'assignment invariant failed at t={t:.1f}: node sum '
                f'{node_assigned_sum} != cluster {assigned_total}')

    _dashboard_write_csv(out / 'replication_summary.csv', DASHBOARD_CLUSTER_COLUMNS, cluster_rows)
    _dashboard_write_csv(out / 'node_replication_timeline.csv', DASHBOARD_NODE_COLUMNS, node_rows)
    if other_regions:
        warnings.append(f'{other_regions} region(s) have target N outside 2/3/4; '
                        'n2+n3+n4 is not 256')
    if assignment_owner_missing:
        warnings.append('committed membership lacked a region owner; assignment columns '
                        'are blank where reconstruction was impossible')
    expected_cluster_rows = n_samples
    expected_node_rows = n_samples * nodes
    if len(cluster_rows) != expected_cluster_rows:
        errors.append(f'replication_summary.csv rows {len(cluster_rows)} != {expected_cluster_rows}')
    if len(node_rows) != expected_node_rows:
        errors.append(f'node_replication_timeline.csv rows {len(node_rows)} != {expected_node_rows}')
    if any(int(row['n2_regions']) + int(row['n3_regions']) + int(row['n4_regions']) != 256
           for row in cluster_rows):
        errors.append('n2_regions + n3_regions + n4_regions != 256 in at least one sample')
    assigned_sum_ok = True
    for index in range(n_samples):
        cluster = cluster_rows[index]
        total = cluster.get('total_assigned_copies')
        if total == '':
            continue
        node_sum = sum(int(node_rows[index * nodes + offset]['assigned_regions'] or 0)
                       for offset in range(nodes))
        if node_sum != int(total):
            assigned_sum_ok = False
            break
    if not assigned_sum_ok:
        errors.append('per-node assigned_regions sum does not equal total_assigned_copies')

    # ------------------------------------------------------------------ #
    # 7. Merge A/B-compatible summary keys; preserve targeted evidence.
    # ------------------------------------------------------------------ #
    total = len(operations)
    acked = sum(1 for entry in operations if entry['success'])
    latencies = [float(entry['latency_ms']) for entry in operations
                 if entry['success'] and entry.get('latency_ms') not in (None, '')]
    readback = summary.get('readback') or {}
    acked_probes = int(readback.get('acked_probes') or 0)
    visible_map = readback.get('visible') or {}
    visible_probes = len(visible_map) if isinstance(visible_map, dict) else 0
    readback_status = readback.get('status')
    readback_blocked = readback_status in ('blocked', 'not_reached')
    if acked_probes == 0:
        verification_missing = None
        verification_checked = []
        read_failures = []
        verification_scope = ('No ACKed immediate probes; post-rejoin probe read-back '
                              'not applicable')
    elif readback_blocked:
        verification_missing = None
        verification_checked = [1]
        read_failures = [{'node': 1, 'error': 'probe read-back unavailable'}]
        verification_scope = ('ACKed immediate probes only (8 pixels); read-back blocked '
                              'by failed GETs, not counted as data loss')
    else:
        verification_missing = max(0, acked_probes - visible_probes)
        verification_checked = [1]
        read_failures = []
        verification_scope = ('ACKed immediate probes only (8 pixels) read back via the '
                              'gateway node 1; not a full-canvas or full-replica check')
    verification = {
        'sampled_pixels': acked_probes,
        'checked_nodes': verification_checked,
        'missing': verification_missing,
        'read_failures': read_failures,
        'examples': [],
        'scope': verification_scope,
        'note': ('Post-rejoin probe read-back. The targeted experiment did not perform '
                 'an independent immediate after-workload snapshot or an all-node '
                 'full-canvas convergence check.'),
    }
    old_final = summary.get('final_placement')
    if isinstance(old_final, dict) and not all(isinstance(v, list) for v in old_final.values()):
        summary['targeted_final_placement'] = old_final
    summary.update({
        'duration_s': duration,
        'nodes': nodes,
        'churn_profile': 'targeted_dual_owner',
        'rate_target': config.get('rate'),
        'gateway_mode': 'node1',
        'targeted_write_percent': 100.0,
        'planned_faults': len(fault_plan),
        'executed_faults': executed,
        'startup_ok': summary.get('initial_placement_gen') is not None,
        'attempts': total,
        'acked': acked,
        'failed': total - acked,
        'success_pct': round(100 * acked / total, 3) if total else None,
        'latency_ms_acked_avg': round(sum(latencies) / len(latencies), 2) if latencies else None,
        'verification': verification,
        'verification_after_settle': verification,
        'by_phase_and_workload': {
            f'{phase}/{workload_class}': {
                'attempts': len(group),
                'acked': sum(1 for entry in group if entry['success']),
                'failed': sum(1 for entry in group if not entry['success']),
                'success_pct': round(100 * sum(1 for entry in group if entry['success'])
                                     / len(group), 2),
            }
            for (phase, workload_class), group in {
                (entry['phase'], entry['workload_class']): [
                    other for other in operations
                    if other['phase'] == entry['phase']
                    and other['workload_class'] == entry['workload_class']]
                for entry in operations
            }.items()
        },
        'final_placement': final_placement,
        'time_base': {
            'workload_start_monotonic_ns': anchor_ns,
            'source': 'time.monotonic_ns() / CLOCK_MONOTONIC',
        } if anchor_ns is not None else summary.get('time_base'),
    })
    # Do not fabricate an immediate checkpoint.  The dashboard will show it as
    # not recorded and display the scope warnings for the single probe check.
    summary.pop('verification_immediate', None)
    summary.pop('verification_comparison', None)

    timeline_complete = bool(
        anchor_ns is not None and owners is not None and placement_events
        and policy_events and other_regions == 0
        and len(cluster_rows) == expected_cluster_rows
        and len(node_rows) == expected_node_rows
        and assigned_sum_ok
    )
    summary['replication_timeline'] = {
        'enabled': True,
        'complete': timeline_complete,
        'partial_run': partial_run,
        'duration_s': duration,
        'sample_interval_s': sample_interval,
        'initial_policy_time_s': next(
            (float(e['time_s']) for e in policy_events
             if e.get('event_type') == 'initial'), None),
        'policy_changes': sum(1 for e in policy_events if e.get('event_type') == 'n_change'),
        'placement_changes': sum(1 for e in policy_events
                                 if e.get('event_type') == 'placement_change'),
        'placement_snapshots': len(placement_events),
        'authority_changes': len({e.get('authority_node') for e in policy_events
                                  if e.get('authority_node') not in (None, '')}),
        'conflicts': conflicts,
        'stale_events_dropped': stale_dropped,
        'malformed_events_dropped': malformed_events,
        'malformed_snapshots_dropped': malformed_snapshots,
        'outputs': ['region_replication_events.csv', 'placement_snapshots.jsonl',
                    'node_replication_timeline.csv', 'replication_summary.csv'],
        'row_counts': {'replication_summary': len(cluster_rows),
                       'node_replication_timeline': len(node_rows)},
        'invariants': {
            'n2_plus_n3_plus_n4_equals_256':
                all(int(r['n2_regions']) + int(r['n3_regions']) + int(r['n4_regions']) == 256
                    for r in cluster_rows),
            'node_assigned_sum_equals_cluster_total': assigned_sum_ok,
        },
        'warnings': list(dict.fromkeys(warnings)),
        'errors': list(dict.fromkeys(errors)),
        'time_alignment': 'source CLOCK_MONOTONIC ns relative to workload start',
        'health_semantics': ('aligned from peer_health CSVs using recorded per-boot launch '
                             'times' if health else
                             'UNKNOWN (peer-health history unavailable)'),
        'connected_assigned_regions_semantics':
            'running assigned holder estimate, not heartbeat-confirmed',
        'process_start_semantics': (
            'real launch/kill/restart events recorded at source'
            if process_source == 'recorded' else
            f'{process_source}: launch timestamps/PIDs reconstructed or unavailable'),
        'duration_source': duration_source,
    }

    profile_zip = f'{strategy}_targeted_seed{seed}.zip'
    combined_zip = f'targeted_BC_seed{seed}.zip'
    export_complete = bool(
        not errors and timeline_complete and operations and process_events
        and policy_events and placement_events and final_placement
    )
    summary['dashboard_export'] = {
        'complete': export_complete,
        'schema': 'p2place_dashboard/A-B-compatible-v1',
        'partial_run': partial_run,
        'duration_s': duration,
        'duration_source': duration_source,
        'profile_zip': profile_zip,
        'combined_zip': combined_zip,
        'warnings': list(dict.fromkeys(warnings)),
        'errors': list(dict.fromkeys(errors)),
        'provenance': {
            'operations': 'puts.csv (matching pixel-echo client ACKs)',
            'process_events': process_source,
            'final_placement': ('per-node *.state' if final_placement
                                else 'unavailable'),
            'owner_table': ('node*.state' if owners is not None else 'unavailable'),
            'health': ('peer_health/*.csv aligned per boot' if health else 'unavailable'),
        },
        'counts': {
            'operations': total,
            'policy_events': len(policy_events),
            'placement_snapshots': len(placement_events),
            'replication_summary_rows': len(cluster_rows),
            'node_timeline_rows': len(node_rows),
            'backfill_events': len(backfill_rows),
        },
        'unknowns': ([warning for warning in warnings
                      if 'unavailable' in warning or 'fallback' in warning
                      or 'missing' in warning or 'UNKNOWN' in warning]
                     + (['run aborted before requested duration; timeline covers only '
                         'the measured interval'] if partial_run else [])),
    }
    summary_path.write_text(json.dumps(summary, indent=2, default=str) + '\n')

    # ------------------------------------------------------------------ #
    # 8. Dashboard ZIPs (deterministic bytes; individual + combined).
    # ------------------------------------------------------------------ #
    try:
        bundle = build_dashboard_bundle(out.parent, seed, nodes)
        if not bundle.get('complete') or not (out.parent / profile_zip).is_file():
            summary['dashboard_export']['complete'] = False
            summary['dashboard_export']['errors'] = list(dict.fromkeys(
                summary['dashboard_export']['errors'] + bundle.get('warnings', [])
                + ['profile ZIP was not produced']))
            summary_path.write_text(json.dumps(summary, indent=2, default=str) + '\n')
            # Rebuild so the ZIP summary agrees with the result summary.
            bundle = build_dashboard_bundle(out.parent, seed, nodes)
    except Exception as exc:  # noqa: BLE001
        summary['dashboard_export']['complete'] = False
        summary['dashboard_export']['errors'].append(f'ZIP build failed: {exc}')
        summary_path.write_text(json.dumps(summary, indent=2, default=str) + '\n')
        raise

    return {
        'strategy': strategy,
        'run': run_id,
        'operations': total,
        'policy_events': len(policy_events),
        'placement_snapshots': len(placement_events),
        'samples': len(cluster_rows),
        'node_samples': len(node_rows),
        'backfill_events': len(backfill_rows),
        'complete': summary['dashboard_export']['complete'],
        'profile_zip': str(out.parent / profile_zip),
        'combined_zip': str(out.parent / combined_zip),
        'zips': bundle.get('zips', []),
        'warnings': summary['dashboard_export']['warnings'],
        'errors': summary['dashboard_export']['errors'],
    }

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--strategy", choices=["B", "C"], help="strategy for this run")
    parser.add_argument("--seed", type=int, default=712367)
    parser.add_argument("--out-root", type=Path, default=Path("experiments/results/bc_targeted_v4"))
    parser.add_argument("--peer", type=Path, default=Path("build/bin/p2place-peer"))
    parser.add_argument("--peer-arg", action="append", default=[], help="extra peer argument; repeatable")
    parser.add_argument("--nodes", type=int, default=10)
    parser.add_argument("--seconds", type=float, default=300.0)
    parser.add_argument("--rate", type=float, default=6.0)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--kill-at", type=float, default=48.0)
    parser.add_argument("--rejoin-at", type=float, default=78.0)
    parser.add_argument("--heat-until", type=float, default=180.0)
    parser.add_argument("--probes", type=int, default=8)
    parser.add_argument("--ack-timeout", type=float, default=4.0)
    parser.add_argument("--get-timeout", type=float, default=25.0)
    parser.add_argument("--readback-timeout", type=float, default=40.0)
    parser.add_argument("--join-spacing", type=float, default=0.3)
    parser.add_argument("--stabilize", type=float, default=15.0)
    parser.add_argument("--policy-interval", type=int, default=2)
    parser.add_argument("--policy-cooldown", type=int, default=3)
    parser.add_argument("--write-window", type=int, default=60)
    parser.add_argument("--write-report-interval", type=int, default=2)
    parser.add_argument("--write-threshold-mid", type=float, default=0.2)
    parser.add_argument("--write-threshold-high", type=float, default=1.0)
    parser.add_argument("--allow-existing-peers", action="store_true",
                        help="do not refuse to start when other p2place-peer processes exist")
    parser.add_argument("--dashboard-sample-interval", type=float, default=1.0,
                        help="dashboard sampling interval in seconds, default 1")
    parser.add_argument("--export-only", action="store_true",
                        help="convert existing B/C runs to dashboard format; no peers launched")
    parser.add_argument("--compare-only", action="store_true",
                        help="regenerate comparison.md from existing B/C summaries and exit")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.dashboard_sample_interval <= 0:
        parser.error('--dashboard-sample-interval must be positive')
    if args.export_only:
        root = args.out_root.resolve() / f"seed{args.seed}"
        found = 0
        infos = []
        for strategy in ("B", "C"):
            if (root / strategy / 'summary.json').is_file():
                try:
                    info = export_targeted_dashboard(
                        root / strategy, sample_interval=args.dashboard_sample_interval)
                    print(f'[dashboard] {strategy} export: {json.dumps(info)}', flush=True)
                    infos.append(info)
                    found += 1
                except Exception as exc:
                    print(f'[dashboard] ERROR exporting {strategy}: {exc}', file=sys.stderr)
                    return 3
        if not found:
            print(f'[dashboard] no existing strategy results under {root}', file=sys.stderr)
            return 3
        bundle = build_dashboard_bundle(root, args.seed, args.nodes)
        for path in bundle.get('zips', []):
            print(f'[dashboard] profile ZIP: {path}', flush=True)
        for warning in bundle.get('warnings', []):
            print(f'[dashboard] WARNING: {warning}', file=sys.stderr, flush=True)
        complete = all(info.get('complete') for info in infos) and bundle.get('complete')
        print(f'[dashboard] export complete={complete} '
              f'({len(infos)} profile(s))', flush=True)
        return 0 if complete else 3
    if args.compare_only:
        root = args.out_root.resolve() / f"seed{args.seed}"
        path = write_comparison(root)
        if not path:
            print(f"cannot compare: need both {root/'B'/'summary.json'} and {root/'C'/'summary.json'}")
            return 3
        print(f"wrote {path}")
        bundle = build_dashboard_bundle(root, args.seed, args.nodes)
        for zpath in bundle.get('zips', []):
            print(f'[dashboard] profile ZIP: {zpath}', flush=True)
        for warning in bundle.get('warnings', []):
            print(f'[dashboard] WARNING: {warning}', file=sys.stderr, flush=True)
        return 0 if bundle.get('complete') else 3
    if not args.strategy:
        parser.error("--strategy is required unless --compare-only is used")
    if args.nodes != 10:
        parser.error("this targeted experiment requires exactly 10 nodes")
    if not (0 < args.kill_at < args.rejoin_at < args.heat_until < args.seconds):
        parser.error("invalid timeline: require 0 < kill < rejoin < heat-until < seconds")
    if args.rate <= 0 or args.probes < 1 or args.concurrency < 1:
        parser.error("invalid rate/probes/concurrency")
    args.peer = args.peer.resolve()
    if not args.peer.is_file():
        parser.error(f"peer binary missing: {args.peer}")
    peer_sha = sha256_file(args.peer)

    root = args.out_root.resolve() / f"seed{args.seed}"
    root.mkdir(parents=True, exist_ok=True)
    scenario_path = root / "scenario.json"
    scenario = load_json(scenario_path)
    out = root / args.strategy
    if out.exists():
        parser.error(f"{out} already exists; choose a new --out-root or move the previous run")
    out.mkdir()

    config = {key: (str(value) if isinstance(value, Path) else value) for key, value in vars(args).items()}
    (out / "config.json").write_text(json.dumps(config, indent=2))

    summary: dict = {
        "strategy": args.strategy,
        "seed": args.seed,
        "status": "INCONCLUSIVE",
        "status_basis": "run did not reach a decision",
        "started_utc": utc_now(),
        "binary_sha256": peer_sha,
        "config": scenario_config(args),
        "scenario": {
            "seed": args.seed,
            "region": scenario.get("region") if scenario else None,
            "original_owners": scenario.get("owners") if scenario else None,
        },
        "cmd": " ".join(sys.argv),
    }
    exit_code = 3
    ports = free_ports(args.nodes)
    summary["ports"] = ports
    recorder = ProcessRecorder(out)
    cluster = Cluster(args, args.strategy, out, ports, recorder)
    workload: Workload | None = None
    timeline: Timeline | None = None
    getter: Getter | None = None
    stop_events = {"timeline": threading.Event(), "getter": threading.Event()}
    events: list[dict] = []
    events_lock = threading.Lock()
    put_rows: list[dict] = []
    put_lock = threading.Lock()
    get_rows: list[dict] = []
    get_lock = threading.Lock()
    timeline_rows: list[dict] = []
    state: dict = {}
    readiness_detail: dict = {}
    probes: list[dict] = []
    readback: dict = {}
    policy_follower: PolicyFollower | None = None
    log_follower: LogFollower | None = None
    region = None
    owners: list[int] = []

    def add_event(name: str, detail: str = "") -> None:
        seconds = None
        if "start" in state:
            seconds = round(time.monotonic() - state["start"], 3)
        with events_lock:
            events.append({"time_s": seconds, "event": name, "detail": detail})
        if seconds is not None:
            print(f"[{args.strategy} {seconds}] {name} {detail}", flush=True)

    try:
        if not args.allow_existing_peers:
            existing = existing_peer_processes(args.peer)
            if existing:
                raise Inconclusive(
                    "other p2place-peer processes are running; refusing to mix experiments: "
                    + " | ".join(existing[:5])
                )
        if scenario is not None:
            validate_scenario(scenario, args, peer_sha)
            summary["scenario"].update(
                region=scenario.get("region"), original_owners=scenario.get("owners")
            )
        print(f"[{args.strategy}] launching {args.nodes} peers ...", flush=True)
        cluster.launch()
        log_follower = LogFollower(out / "node1" / "node.log")
        policy_follower = PolicyFollower(out / "node1" / "policy_node1.csv")
        log_follower.poll()
        initial_gen = wait_full_cluster(log_follower, args.nodes)
        summary["initial_placement_gen"] = initial_gen
        add_event("cluster_committed", f"generation={initial_gen}")
        if scenario is not None:
            if scenario.get("initial_placement_gen") != initial_gen:
                raise Inconclusive(
                    f"initial placement generation mismatch: B={scenario.get('initial_placement_gen')} "
                    f"C={initial_gen}; the second cluster did not reproduce the scenario"
                )
        time.sleep(args.stabilize)

        region, owners = find_selection(
            args, args.strategy, out, cluster, log_follower, scenario
        )
        state["region"] = region
        args.region = region
        summary["region"] = region
        summary["original_owners"] = owners
        summary["scenario"].update(region=region, original_owners=owners)
        print(f"[{args.strategy}] SHARED SCENARIO region={region} owners={owners}", flush=True)
        if scenario is None:
            scenario_payload = {
                "seed": args.seed,
                "region": region,
                "owners": owners,
                "nodes": args.nodes,
                "initial_placement_gen": initial_gen,
                "peer_sha256": peer_sha,
                "config": scenario_config(args),
                "pixel_function": "sha256(seed:sequence) -> x=(region%16)*16+s%16, y=(region//16)*16+(s//16)%16",
                "created_utc": utc_now(),
                "created_by": "run_targeted_bc.py --strategy B",
            }
            scenario_path.write_text(json.dumps(scenario_payload, indent=2))
        else:
            # find_selection already validated the baseline; keep the summary in sync.
            if scenario.get("region") != region or list(scenario.get("owners")) != list(owners):
                raise Inconclusive("scenario region/owner validation failed")
        add_event("scenario_selected", f"region={region} owners={owners}")

        pre_fault_gen = log_follower.latest_gen()
        if pre_fault_gen is None:
            raise Inconclusive("no committed placement visible after selection")
        state["pre_fault_gen"] = pre_fault_gen
        state["kill_time_s"] = None

        probe_pixels = {
            (pixel(region, PROBE_BASE_SEQUENCE + index, args.seed)[0],
             pixel(region, PROBE_BASE_SEQUENCE + index, args.seed)[1])
            for index in range(args.probes)
        }
        puts_path = out / "puts.csv"
        puts_handle = puts_path.open("w", newline="")
        puts_writer = csv.DictWriter(
            puts_handle,
            fieldnames=[
                "time_s", "phase", "sequence", "region", "x", "y", "color",
                "ack", "result", "reason", "latency_ms", "total_ms", "offered_offset_s",
            ],
        )
        puts_writer.writeheader()

        start = time.monotonic()
        anchor_ns = time.monotonic_ns()
        recorder.set_anchor(anchor_ns)
        state["start"] = start
        state["anchor_ns"] = anchor_ns
        summary["time_base"] = {
            "workload_start_monotonic_ns": anchor_ns,
            "source": "time.monotonic_ns() / CLOCK_MONOTONIC",
        }
        summary["process_events_source"] = "recorded"
        workload = Workload(
            args, cluster, region, start, probe_pixels, put_rows, puts_writer,
            put_lock, puts_handle.flush,
        )
        timeline = Timeline(
            args, start, log_follower, policy_follower, workload,
            stop_events["timeline"], out, state, events, events_lock,
        )
        workload.start()
        timeline.start()
        add_event("workload_started", f"rate={args.rate}/s concurrency={args.concurrency}")

        # ---- pre-fault window and readiness gate ----------------------- #
        if not sleep_until(start + args.kill_at):
            raise Inconclusive("interrupted before the readiness gate")
        drained = workload.pause_and_drain(timeout=5.0)
        if not drained:
            raise Inconclusive("workload did not drain before the readiness gate")
        log_follower.poll()
        policy_follower.poll()
        current_gen = log_follower.latest_gen()
        if current_gen != pre_fault_gen:
            raise Inconclusive(
                f"placement changed before the gate: {pre_fault_gen} -> {current_gen}"
            )
        with put_lock:
            snapshot_rows = list(put_rows)
        readiness_detail = evaluate_readiness(
            args, args.strategy, out, region, owners, pre_fault_gen,
            log_follower, policy_follower, snapshot_rows,
        )
        (out / "readiness.json").write_text(json.dumps(readiness_detail, indent=2))
        summary["readiness"] = readiness_detail
        summary["pre_fault_gen"] = pre_fault_gen
        if not readiness_detail.get("proceed"):
            raise Inconclusive(
                "pre-fault readiness not certified; crash NOT executed. "
                + json.dumps(readiness_detail.get("reason", ""))
            )
        add_event("readiness_certified", readiness_detail.get("reason", "ok"))
        if args.strategy == "B" and readiness_detail.get("hypothesis_applicable") is False:
            add_event(
                "hypothesis_not_applicable",
                "B had already increased N before the fault; continuing to observe its behavior",
            )

        # ---- simultaneous crash and fixed-generation probes ----------- #
        kill_wall = time.monotonic()
        cluster.kill_owners(owners)
        kill_time = kill_wall - start
        state["kill_time_s"] = kill_time
        summary["kill_time_s"] = round(kill_time, 3)
        add_event("owners_killed", f"nodes={owners}")

        log_follower.poll()
        gen_before = log_follower.latest_gen()
        # B's expected set is the certified routed set (the original owner pair
        # for a valid control; an N>2 set if B had already adapted).
        expected_targets = list(readiness_detail["latest_put"]["targets"])
        probes = run_probe_burst(args, cluster, region, start, log_follower)
        log_follower.poll()
        gen_after = log_follower.latest_gen()
        correlate_probe_evidence(
            probes, log_follower, pre_fault_gen, expected_targets, args.strategy
        )
        for probe in probes:
            probe["issue_time_s"] = probe.get("issue_time_s", 0.0)
            probe["time_s"] = probe["issue_time_s"]
            probe["phase"] = "immediate_fault_probe"
            probe["region"] = region
            probe["gen_before_burst"] = gen_before
            probe["gen_after_burst"] = gen_after
        with put_lock:
            for probe in probes:
                put_rows.append(probe)
                puts_writer.writerow(
                    {
                        "time_s": probe["issue_time_s"],
                        "phase": "immediate_fault_probe",
                        "sequence": probe["sequence"],
                        "region": region,
                        "x": probe["x"],
                        "y": probe["y"],
                        "color": probe["color"],
                        "ack": probe["ack"],
                        "result": probe["result"],
                        "reason": probe["reason"],
                        "latency_ms": probe["latency_ms"],
                        "total_ms": probe["total_ms"],
                        "offered_offset_s": probe["issue_time_s"],
                    }
                )
            puts_handle.flush()
        summary["probes"] = probes
        in_window = [probe for probe in probes if probe.get("fixed_generation")]
        summary["probe_summary"] = {
            "total": len(probes),
            "in_window": len(in_window),
            "fixed_generation": len(in_window),
            "fixed_generation_acks": sum(probe["ack"] for probe in in_window),
            "fixed_generation_naks": sum(1 for probe in in_window if probe["result"] == "nak"),
            "fixed_generation_timeouts": sum(1 for probe in in_window if probe["result"] == "timeout"),
            "outside_window": len(probes) - len(in_window),
        }
        print(
            f"[{args.strategy}] IMMEDIATE PROBES in-window "
            f"{summary['probe_summary']['fixed_generation_acks']}/{len(in_window)} ACK "
            f"(of {len(probes)} sent)",
            flush=True,
        )
        workload.resume()

        # ---- fault window, rejoin, recovery ---------------------------- #
        if not sleep_until(start + args.rejoin_at):
            raise Inconclusive("interrupted before rejoin")
        rejoin_wall = time.monotonic()
        for owner in owners:
            cluster.restart(owner)
        rejoin_time = rejoin_wall - start
        state["rejoin_time_s"] = rejoin_time
        summary["rejoin_time_s"] = round(rejoin_time, 3)
        add_event("owners_restarted", f"nodes={owners}")

        getter = Getter(
            args, cluster, start, rejoin_time, stop_events["getter"],
            [probe for probe in probes if probe["ack"]],
            list(readiness_detail.get("additional_replicas", [])),
            get_rows, get_lock, None, events,
        )
        gets_handle = (out / "gets.csv").open("w", newline="")
        gets_writer = csv.DictWriter(
            gets_handle,
            fieldnames=["time_s", "purpose", "reader_port", "complete", "pixel_count",
                        "latency_ms", "reason"],
        )
        gets_writer.writeheader()
        getter.writer = gets_writer
        getter.start()

        if not sleep_until(start + args.seconds):
            raise Inconclusive("interrupted during the run")
        workload.stop()
        workload.join(timeout=10)
        stop_events["getter"].set()
        if getter is not None:
            getter.join(timeout=args.get_timeout + 10)
        stop_events["timeline"].set()
        if timeline is not None:
            timeline.join(timeout=5)
        puts_handle.close()
        gets_handle.close()
        log_follower.poll()
        policy_follower.poll()

        # ---- status ---------------------------------------------------- #
        gateway_alive = cluster.nodes[1]["proc"].poll() is None
        summary["gateway_alive"] = gateway_alive
        timing = {
            "start_monotonic": start,
            "kill_time_s": round(kill_time, 3),
            "rejoin_time_s": round(rejoin_time, 3),
            "heat_until_s": args.heat_until,
            "runtime_s": round(time.monotonic() - start, 2),
        }
        summary["timing"] = timing
        summary["reconfig_detected_s"] = state.get("reconfig_detected_s")
        summary["skipped_collisions"] = workload.skipped_collisions

        # Raw client-visible responses for the first few workload PUTs and
        # every immediate probe (the ACK shape is protocol evidence).
        with (out / "ws_raw.jsonl").open("w") as handle:
            for sample in workload.raw_samples:
                handle.write(json.dumps({"kind": "workload_put", **sample}) + "\n")
            for probe in probes:
                handle.write(
                    json.dumps(
                        {
                            "kind": "immediate_probe",
                            "sequence": probe["sequence"],
                            "x": probe["x"],
                            "y": probe["y"],
                            "color": probe["color"],
                            "result": probe["result"],
                            "reason": probe["reason"],
                            "latency_ms": probe["latency_ms"],
                            "messages": probe.get("raw_first_messages", []),
                        }
                    )
                    + "\n"
                )

        # Phase summaries are windows over the actual issue times.  Using the
        # recorded issue time (not the nominal label) keeps a write issued
        # just after the SIGKILL out of the "heat" bucket.
        def window_rows(begin: float, end: float, include_probes: bool = False) -> list[dict]:
            return [
                row for row in put_rows
                if begin <= row["time_s"] < end
                and (include_probes or row["phase"] != "immediate_fault_probe")
            ]

        def window_summary(begin: float, end: float) -> dict:
            subset = window_rows(begin, end)
            out_summary = summarize_subset(subset, end - begin)
            out_summary["offered_rate"] = args.rate
            return out_summary

        probe_duration = None
        if probes:
            first_probe = min(probe["issue_time_s"] for probe in probes)
            last_probe = max(
                probe["issue_time_s"] + (probe["total_ms"] or 0.0) / 1000.0
                for probe in probes
            )
            probe_duration = max(0.001, last_probe - first_probe)
        summary["phases"] = {
            "heat": window_summary(0.0, kill_time),
            "immediate_fault_probe": summarize_subset(
                [row for row in put_rows if row["phase"] == "immediate_fault_probe"],
                probe_duration,
            ),
            "fault_window": window_summary(kill_time, rejoin_time),
            "recovery": window_summary(rejoin_time, args.heat_until),
        }
        if probe_duration:
            summary["phases"]["immediate_fault_probe"]["offered_rate"] = round(
                len(probes) / probe_duration, 3
            )
        summary["probe_pause_s"] = round(
            max(0.0, min((p["issue_time_s"] for p in probes), default=kill_time) - kill_time),
            3,
        )
        reconfig_s = state.get("reconfig_detected_s")
        summary["placement_reconfigured"] = bool(
            reconfig_s is not None and reconfig_s > kill_time
        )
        if summary["placement_reconfigured"]:
            summary["pre_reconfig_fault_window"] = window_summary(kill_time, reconfig_s)
        else:
            # The cluster can keep placement generation 10 while the dead
            # members are only suspect/unavailable (auto-removal can be
            # dropped when they reconnect).  Then the whole fault window is
            # still pre-placement-reconfiguration; policy N is what changes.
            summary["pre_reconfig_fault_window"] = window_summary(kill_time, rejoin_time)

        summary["write_buckets"] = bucket_rows(
            [row for row in put_rows if row["phase"] != "immediate_fault_probe"]
        )
        with (out / "write_buckets.csv").open("w", newline="") as handle:
            fieldnames = list(summary["write_buckets"][0].keys()) if summary["write_buckets"] else [
                "bin_s", "phase", "attempts", "acks", "naks", "timeouts", "closed",
                "connect_errors", "success_pct", "ack_p50_ms", "fail_p50_ms", "max_ms",
            ]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in summary["write_buckets"]:
                writer.writerow(row)

        # Timeline-derived policy view.
        timeline_rows = []
        try:
            with (out / "replication_timeline.csv").open(newline="") as handle:
                for row in csv.DictReader(handle):
                    try:
                        timeline_rows.append(
                            {
                                "time_s": float(row["time_s"]),
                                "policy_n": int(row["policy_n"]) if row["policy_n"] else None,
                                "placement_gen": int(row["placement_gen"]) if row["placement_gen"] else None,
                            }
                        )
                    except (KeyError, ValueError):
                        continue
        except OSError:
            pass
        summary["policy_summary"] = policy_summary(summary, timeline_rows)
        summary["idle"] = idle_summary(timeline_rows, args.heat_until)

        # Read-back result.
        readback = dict(getter.readback_state) if getter is not None else {}
        acked = [probe for probe in probes if probe["ack"]]
        visible = readback.get("visible", {})
        if not acked:
            readback_status = "not_applicable"
        elif len(visible) == len(acked):
            readback_status = "ok"
        elif readback.get("complete_gets", 0) > 0:
            readback_status = "failed"
        else:
            readback_status = "blocked"
        readback.update(
            {
                "acked_probes": len(acked),
                "all_visible": bool(acked) and len(visible) == len(acked),
                "status": readback_status,
                "deadline_s": args.readback_timeout,
            }
        )
        summary["readback"] = readback
        (out / "readback.json").write_text(json.dumps(readback, indent=2))

        # Final placement / gateway health.
        final_placement = log_follower.latest_placement()
        summary["final_placement"] = final_placement
        summary["final_policy"] = (
            policy_follower.latest(region) if policy_follower else None
        )
        if summary.get("policy_summary") and summary.get("final_policy"):
            summary["policy_summary"]["final_n"] = summary["final_policy"]["assigned_n"]

        # ---- decide run status ----------------------------------------- #
        in_window_count = summary["probe_summary"]["fixed_generation"]
        fixed_acks = summary["probe_summary"]["fixed_generation_acks"]
        background_error = state.get("timeline_error") or (getter.error if getter is not None else None)
        if background_error:
            summary["background_error"] = background_error
        if not gateway_alive:
            status, basis = "INCONCLUSIVE", "gateway node 1 died; availability is uninterpretable"
        elif background_error:
            status, basis = (
                "INCONCLUSIVE",
                f"background measurement thread failed: {background_error}",
            )
        elif in_window_count < 6:
            status, basis = (
                "INCONCLUSIVE",
                f"only {in_window_count}/{len(probes)} probes landed in the fixed-generation "
                "window; shifted fault timing is not a policy result",
            )
        elif args.strategy == "B":
            if readiness_detail.get("hypothesis_applicable") is False:
                status, basis = (
                    "FAIL",
                    "B had already increased N before the fault; the targeted N=2 control "
                    "condition no longer holds",
                )
            elif fixed_acks > 0:
                status, basis = (
                    "FAIL",
                    f"{fixed_acks}/{in_window_count} fixed-generation probes were ACKed although "
                    "both original N=2 owners were dead; B retained availability unexpectedly",
                )
            else:
                status, basis = (
                    "PASS",
                    f"valid N=2 control: {in_window_count}/{len(probes)} probes in the fixed-generation "
                    f"window, {fixed_acks}/{in_window_count} ACK",
                )
        else:
            if fixed_acks == 0:
                status, basis = (
                    "FAIL",
                    f"certified N=4 with completed backfill, but 0/{in_window_count} "
                    "fixed-generation probes were ACKed",
                )
            elif readback_status == "blocked":
                status, basis = (
                    "INCONCLUSIVE",
                    "C retained client availability, but read-back failed due to failed GETs, "
                    "not absent data",
                )
            elif readback_status == "failed":
                status, basis = (
                    "FAIL",
                    f"C ACKed {fixed_acks}/{in_window_count} probes but not every ACKed pixel "
                    "was readable within the documented polling deadline",
                )
            elif not summary["idle"].get("ok"):
                status, basis = (
                    "FAIL",
                    f"C did not decay to N=2 within the idle interval (decay={summary['idle'].get('decay_s')} s)",
                )
            else:
                status, basis = (
                    "PASS",
                    f"certified N=4 with complete backfill; {fixed_acks}/{in_window_count} probes "
                    f"ACKed and {len(visible)}/{len(acked)} readable; idle decay "
                    f"{summary['idle'].get('decay_s')} s",
                )
        summary["status"] = status
        summary["status_basis"] = basis
        add_event("run_completed", f"status={status}")

    except Inconclusive as exc:
        summary["status"] = "INCONCLUSIVE"
        summary["status_basis"] = str(exc)
        print(f"[{args.strategy}] INCONCLUSIVE: {exc}", flush=True)
        if log_follower is not None:
            log_follower.poll()
        if policy_follower is not None and region is not None:
            try:
                summary["abort_policy_history"] = policy_follower.history(region)[-40:]
            except Exception:  # noqa: BLE001 - diagnostics must never mask the abort
                pass
        if region is not None:
            try:
                summary["abort_repair_events"] = read_repair_events(out, region)[-40:]
            except Exception:  # noqa: BLE001
                pass
        if readiness_detail:
            summary["readiness"] = readiness_detail
    except HypothesisFailure as exc:
        summary["status"] = "FAIL"
        summary["status_basis"] = str(exc)
        print(f"[{args.strategy}] FAIL: {exc}", flush=True)
    except Exception as exc:  # noqa: BLE001 - convert to a diagnostics-bearing abort
        summary["status"] = "INCONCLUSIVE"
        summary["status_basis"] = f"harness error: {type(exc).__name__}: {exc}"
        summary["traceback"] = traceback.format_exc()
        print(f"[{args.strategy}] HARNESS ERROR: {exc}", flush=True)
        traceback.print_exc()
    finally:
        # Stop background threads without waiting indefinitely.
        stop_events["timeline"].set()
        stop_events["getter"].set()
        if workload is not None:
            workload.stop()
            try:
                workload.join(timeout=5)
            except RuntimeError:
                pass
        for thread in (timeline, getter):
            if thread is not None and thread.is_alive():
                try:
                    thread.join(timeout=3)
                except RuntimeError:
                    pass
        try:
            cluster.teardown()
        except Exception:  # noqa: BLE001
            pass
        try:
            recorder.close()
        except Exception:  # noqa: BLE001
            pass
        if recorder.anchor_ns is None:
            # No workload anchor: process times are not comparable to t=0.
            summary["process_events_source"] = "startup_aborted"
        if "timing" not in summary and "start" in state:
            # Aborted before the normal timing block: retain the observed
            # runtime so dashboard samples stop at the abort, not at a
            # fabricated full-duration timeline.
            summary["aborted_runtime_s"] = round(time.monotonic() - state["start"], 3)

        # Ensure phase/probe summaries exist even for aborts.
        summary.setdefault("probe_summary", {
            "total": len(probes), "in_window": 0, "fixed_generation": 0,
            "fixed_generation_acks": 0, "fixed_generation_naks": 0,
            "fixed_generation_timeouts": 0, "outside_window": len(probes),
        })
        if probes:
            summary["probes"] = probes
        summary.setdefault("readback", readback or {
            "acked_probes": 0, "visible": {}, "all_visible": False, "status": "not_reached"
        })
        summary["finished_utc"] = utc_now()
        summary["events"] = events
        if log_follower is not None:
            try:
                log_follower.poll()
                summary["placement_history"] = [
                    {"gen": item["gen"], "owners": item["owners"]}
                    for item in log_follower.placements
                ]
            except Exception:  # noqa: BLE001
                pass
        # Write events.
        try:
            with (out / "events.csv").open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["time_s", "event", "detail"])
                writer.writeheader()
                for event in events:
                    writer.writerow(event)
        except OSError:
            pass
        (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
        try:
            exported = export_targeted_dashboard(out, sample_interval=args.dashboard_sample_interval)
            print(f'[dashboard] exported canonical CSV/JSONL: {json.dumps(exported)}', flush=True)
        except Exception as exc:
            # Export failures must never silently alter experimental verdicts.
            print(f'[dashboard] WARNING: dashboard export failed: {exc}', file=sys.stderr, flush=True)
            summary['dashboard_export_error'] = str(exc)
            (out / 'summary.json').write_text(json.dumps(summary, indent=2, default=str))
        comparison = write_comparison(root)
        print(f"[{args.strategy}] RESULTS: {out}", flush=True)
        if comparison:
            print(f"[{args.strategy}] COMPARISON: {comparison}", flush=True)
        print(f"[{args.strategy}] STATUS: {summary['status']} — {summary['status_basis']}", flush=True)

    if summary["status"] == "PASS":
        exit_code = 0
    elif summary["status"] == "FAIL":
        exit_code = 2
    else:
        exit_code = 3
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
