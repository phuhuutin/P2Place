#!/usr/bin/env python3
"""Lab 06 membership regression (live cluster).

Test A: empty 3-node cluster; node 4 joins through a NON-authority seed only.
        Asserts exactly one gen-4 transition, identical committed placement on
        all four peers, and no stale .pending markers anywhere.
Test B: populated cluster (one pixel per region); node 4 joins through node 2.
        Asserts the replicated handoff completes and every node serves an
        identical full canvas afterwards (no lost acknowledged writes).

Uses isolated ports and temp state files; existing state/ is untouched.
"""
import argparse
import json
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_websocket_manual import WebSocketClient  # noqa: E402


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def placement_fields(path):
    if not path.exists():
        return None
    fields = path.read_text().split()
    if not fields or fields[0] not in ('P2PLACE_BALANCED_V1', 'P2PLACE_BALANCED_V2'):
        return None
    gen = int(fields[1])
    count = int(fields[2])
    members = tuple(map(int, fields[3:3 + count]))
    offset = 3 + count * (2 if fields[0].endswith('V2') else 1)
    owners = tuple(map(int, fields[offset:offset + 256]))
    return gen, members, owners


def wait_for(pred, what, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if pred():
                return
        except Exception:
            pass
        time.sleep(0.15)
    raise AssertionError('Timed out: ' + what)


def log_count(path, needle):
    try:
        return path.read_text().count(needle)
    except FileNotFoundError:
        return 0


def write_region_pixels(ws_port, timeout=90):
    """Write one pixel per region (256 total) through one WebSocket.

    One background reader drains broadcasts and records acknowledged pixels;
    the main thread only sends, so the two never race on the socket.
    """
    ws = WebSocketClient('127.0.0.1', ws_port)
    ws.connect()
    ws.sock.settimeout(2.0)
    stop = threading.Event()
    errors = []
    seen = set()
    lock = threading.Lock()

    def reader():
        try:
            while not stop.is_set():
                try:
                    opcode, payload = ws.recv_frame()
                except socket.timeout:
                    continue
                except (ConnectionError, OSError) as e:
                    if not stop.is_set():
                        errors.append(str(e))
                    return
                if opcode == 0x9:
                    ws.send_frame(0xA, payload)
                elif opcode == 0x8:
                    errors.append('websocket closed')
                    return
                elif opcode == 0x1:
                    msg = json.loads(payload)
                    if msg.get('op') == 'error':
                        errors.append(str(msg.get('message', msg)))
                    elif msg.get('op') == 'pixel':
                        with lock:
                            seen.add((int(msg['x']), int(msg['y'])))
        except Exception as e:  # noqa: BLE001
            if not stop.is_set():
                errors.append(str(e))

    drain = threading.Thread(target=reader, daemon=True)
    drain.start()
    try:
        for key in range(256):
            x = (key % 16) * 16
            y = (key // 16) * 16
            color = '#%02X%02X%02X' % ((key * 7) & 255, (key * 13) & 255, (key * 29) & 255)
            ws.send_frame(0x1, json.dumps({'op': 'put', 'x': x, 'y': y, 'color': color}))
            if errors:
                raise AssertionError('node rejected PUT: ' + errors[0])
            time.sleep(0.005)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with lock:
                count = len(seen)
            if count >= 256:
                break
            if errors:
                raise AssertionError('node rejected PUT: ' + errors[0])
            time.sleep(0.1)
        with lock:
            count = len(seen)
        if count < 256:
            raise AssertionError(f'only {count}/256 writes acknowledged')
    finally:
        stop.set()
        try:
            ws.sock.close()
        except OSError:
            pass
        drain.join(timeout=2)


def snapshot_pixels(ws_port, timeout=30):
    from test_lab04_canvas_sync import snapshot
    return snapshot('127.0.0.1', ws_port, timeout)


class Cluster:
    def __init__(self, binary):
        self.binary = str(pathlib.Path(binary).resolve())
        self.root = None
        self.cleanup_dir = None
        self.procs = {}
        self.logs = {}
        self.ws_ports = {}
        self.peer_ports = {}

    def __enter__(self):
        self.cleanup_dir = tempfile.TemporaryDirectory(prefix='p2place-lab06-join-')
        self.root = pathlib.Path(self.cleanup_dir.name)
        return self

    def __exit__(self, *exc):
        for p in self.procs.values():
            if p.poll() is None:
                p.terminate()
        for p in self.procs.values():
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
        for f in self.logs.values():
            f.close()
        self.cleanup_dir.cleanup()

    def start(self, n, seed=None, ports=None):
        if ports is not None:
            peer_port, ws_port = ports
        else:
            peer_port, ws_port = free_port(), free_port()
        log = (self.root / f'node{n}.log').open('w')
        cmd = [self.binary, '--id', str(n), '--port', str(peer_port),
               '--ws-port', str(ws_port), '--state-file',
               str(self.root / f'node{n}.state'),
               '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2']
        if seed:
            cmd += ['--peers', f'{seed}:127.0.0.1:{self.peer_ports[seed]}']
        if shutil.which('stdbuf'):
            cmd = ['stdbuf', '-oL', '-eL'] + cmd
        self.procs[n] = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        self.logs[n] = log
        self.ws_ports[n] = ws_port
        self.peer_ports[n] = peer_port

    def state(self, n):
        return placement_fields(self.root / f'node{n}.state')

    def state_path(self, n):
        return self.root / f'node{n}.state'

    def has_pending(self, n):
        return pathlib.Path(str(self.root / f'node{n}.state') + '.pending').exists()

    def log(self, n):
        return (self.root / f'node{n}.log').read_text()

    def stop(self, n):
        p = self.procs[n]
        if p.poll() is None:
            p.terminate()
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()
        self.logs[n].close()
        del self.procs[n]


def assert_gen4_clean(cluster, label):
    states = [cluster.state(n) for n in (1, 2, 3, 4)]
    assert all(s is not None for s in states), f'{label}: missing committed state'
    gens = [s[0] for s in states]
    assert all(g == 4 for g in gens), f'{label}: generations {gens}'
    assert all(s[1] == (1, 2, 3, 4) for s in states), f'{label}: members diverge'
    assert all(s[2] == states[0][2] for s in states), f'{label}: owners diverge'
    assert not any(cluster.has_pending(n) for n in (1, 2, 3, 4)), \
        f'{label}: stale .pending marker remains'
    assert 'RELEASE generation 4' in cluster.log(1), f'{label}: no release on authority'
    assert 'release-received' in cluster.log(4), f'{label}: node 4 never released'
    print(f'PASS ({label}): all four peers committed identical generation 4 '
          f'members=(1,2,3,4), no pending markers')


def test_a_empty(binary):
    with Cluster(binary) as c:
        c.start(1)
        wait_for(lambda: c.state(1) is not None, 'node1 bootstrap')
        c.start(2, seed=1)
        wait_for(lambda: c.state(2) is not None, 'node2 join')
        c.start(3, seed=1)
        wait_for(lambda: all((s := c.state(n)) and s[0] >= 3 for n in (1, 2, 3)),
                 'generation 3')
        time.sleep(1.0)  # let gossip settle so no unrelated traffic interferes
        c.start(4, seed=2)  # node 4 knows ONLY node 2's address
        wait_for(lambda: all((s := c.state(n)) and s[0] >= 4 for n in (1, 2, 3, 4)),
                 'generation 4 via node 2 seed')
        assert_gen4_clean(c, 'testA seed=2')
        # Exactly one admission: repeated referrals/HELLOs must not re-enqueue
        # or restart the transition.
        assert c.log(1).count('PREPARE generation 4') == 1, \
            'authority prepared generation 4 more than once'
        assert c.log(1).count('queued JOIN 4') == 1, \
            'duplicate referrals re-enqueued the candidate'
        assert '[join-forward]' in c.log(2), 'seed never forwarded the referral'
        assert '[join-referral]' in c.log(1), 'authority never logged the referral'
        print('PASS: exactly one gen-4 transition; duplicate referrals absorbed')

    # Repeat with node 3 as the only seed.
    with Cluster(binary) as c:
        c.start(1)
        wait_for(lambda: c.state(1) is not None, 'node1 bootstrap (2nd run)')
        c.start(2, seed=1)
        wait_for(lambda: c.state(2) is not None, 'node2 join (2nd run)')
        c.start(3, seed=1)
        wait_for(lambda: all((s := c.state(n)) and s[0] >= 3 for n in (1, 2, 3)),
                 'generation 3 (2nd run)')
        time.sleep(1.0)
        c.start(4, seed=3)
        wait_for(lambda: all((s := c.state(n)) and s[0] >= 4 for n in (1, 2, 3, 4)),
                 'generation 4 via node 3 seed')
        assert_gen4_clean(c, 'testA seed=3')
        assert c.log(1).count('PREPARE generation 4') == 1, \
            'authority prepared generation 4 more than once (seed=3)'
        print('PASS: seed-independent join also succeeds with node 3 as the only seed')


def test_b_populated(binary):
    with Cluster(binary) as c:
        c.start(1)
        wait_for(lambda: c.state(1) is not None, 'node1 bootstrap')
        c.start(2, seed=1)
        wait_for(lambda: c.state(2) is not None, 'node2 join')
        c.start(3, seed=1)
        wait_for(lambda: all((s := c.state(n)) and s[0] >= 3 for n in (1, 2, 3)),
                 'generation 3')
        write_region_pixels(c.ws_ports[1])
        print('PASS: 256 region pixels acknowledged at W=2')
        c.start(4, seed=2)
        wait_for(lambda: all((s := c.state(n)) and s[0] >= 4 for n in (1, 2, 3, 4)),
                 'generation 4 (populated)', 180)
        assert_gen4_clean(c, 'testB')
        log1 = c.log(1)
        assert '[handoff-plan]' in log1 and 'moves=' in log1, 'no handoff plan logged'
        assert log1.count('[handoff-ok]') > 0, 'no handoff completion logged'
        assert log1.count('PREPARE generation 4') == 1, \
            'authority prepared generation 4 more than once (populated)'
        print('PASS: replicated handoff ran and completed')
        # After writes stop, quorum get_canvas through all four nodes must be
        # identical with no missing acknowledged writes.
        snapshots = {}
        for n in (1, 2, 3, 4):
            snapshots[n] = snapshot_pixels(c.ws_ports[n])
        baseline = snapshots[1]
        assert len(baseline) == 256, f'expected 256 pixels, got {len(baseline)}'
        for n in (2, 3, 4):
            assert snapshots[n] == baseline, \
                f'node {n} canvas diverges from node 1 ' \
                f'({len(snapshots[n])} vs {len(baseline)} pixels)'
        print('PASS: get_canvas identical on all four nodes; no lost writes')


def test_c_committed_restart_recovery(binary):
    """A member that committed but crashed before RELEASE restarts with its
    .pending marker; the authority must re-release it and the node must serve
    again (fail-closed marker removed only by the verified RELEASE)."""
    with Cluster(binary) as c:
        c.start(1)
        wait_for(lambda: c.state(1) is not None, 'node1 bootstrap')
        c.start(2, seed=1)
        wait_for(lambda: c.state(2) is not None, 'node2 join')
        c.start(3, seed=1)
        wait_for(lambda: all((s := c.state(n)) and s[0] >= 3 for n in (1, 2, 3)),
                 'generation 3')
        c.start(4, seed=2)
        wait_for(lambda: all((s := c.state(n)) and s[0] >= 4 for n in (1, 2, 3, 4)),
                 'generation 4')
        # Simulate "committed but crashed before RELEASE": stop node 4 and
        # leave a .pending marker of the same generation behind.
        c.stop(4)
        state = c.state_path(4)
        pending = pathlib.Path(str(state) + '.pending')
        pending.write_text(state.read_text())
        # Restart on the same ports.
        c.start(4, seed=2, ports=(c.peer_ports[4], c.ws_ports[4]))
        wait_for(lambda: 'release-received' in c.log(4),
                 'authority re-release of committed peer', 60)
        assert 'recovery: re-release' in c.log(1), \
            'authority never re-released the committed-but-unreleased peer'
        assert not c.has_pending(4), '.pending survived the verified RELEASE'
        # Node 4 must serve again, not stay frozen.
        from test_lab04_canvas_sync import snapshot
        s1 = snapshot('127.0.0.1', c.ws_ports[1], 15)
        s4 = snapshot('127.0.0.1', c.ws_ports[4], 15)
        assert s1 == s4, 'recovered node 4 canvas diverges from authority'
        print('PASS: committed-but-unreleased member recovers via re-RELEASE '
              'and serves again')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--peer', default='./build/bin/p2place-peer')
    args = parser.parse_args()
    binary = pathlib.Path(args.peer).resolve()
    if not binary.is_file():
        raise SystemExit('Missing peer executable: ' + str(binary))
    test_a_empty(binary)
    test_b_populated(binary)
    test_c_committed_restart_recovery(binary)
    print('ALL PASS: Lab 06 seed-independent join + populated handoff + '
          'committed restart recovery')


if __name__ == '__main__':
    main()
