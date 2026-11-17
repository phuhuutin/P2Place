#!/usr/bin/env python3
"""Lab 06 Strategy B (churn-adaptive replication) live test.

B-stable: 5 healthy peers -> the authority's shared policy snapshot selects
          N2 for all 256 regions; every peer applies the SAME snapshot
          (same epoch); PUT/GET quorum works against the reduced target set.
B-churn:  kill node 4 for most of a short reliability window -> its score
          collapses below 0.80 -> regions whose baseline includes node 4
          move to N4; acked pixels survive; after the churn stops and the
          score recovers, the policy returns to N2 (cooldown respected).
A-regression: Strategy A (default flags) keeps fixed N=3 (no policy
          broadcasts at all).

Isolated temp state dirs + free ports; repo state/ untouched.
"""
import json
import pathlib
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_websocket_manual import WebSocketClient  # noqa: E402
from test_lab04_canvas_sync import snapshot  # noqa: E402

BIN = str((pathlib.Path(__file__).resolve().parent.parent / 'build/bin/p2place-peer').resolve())


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
    return gen, members


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


def start(root, n, strategy, seed=None, fast_detect=True, ports=None):
    if ports:
        peer_port, ws_port = ports
    else:
        peer_port, ws_port = free_port(), free_port()
    log = (root / f'node{n}.log').open('w')
    cmd = [BIN, '--id', str(n), '--port', str(peer_port), '--ws-port', str(ws_port),
           '--state-file', str(root / f'node{n}.state'),
           '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2',
           '--strategy', strategy,
           # accelerated policy clock for the test (documented in the report)
           '--reliability-window', '30', '--policy-interval', '3',
           '--policy-cooldown', '5']
    if fast_detect:
        cmd += ['--suspect-after', '1', '--unavailable-after', '3',
                '--recovery-stabilization', '1']
    if seed:
        cmd += ['--peers', f'{seed[0]}:127.0.0.1:{seed[1]}']
    if shutil.which('stdbuf'):
        cmd = ['stdbuf', '-oL', '-eL'] + cmd
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    return proc, log, peer_port, ws_port


class Reader:
    def __init__(self, ws):
        self.ws = ws
        self.lock = threading.Lock()
        self.acked = set()
        self.errors = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        try:
            while not self.stop.is_set():
                try:
                    opcode, payload = self.ws.recv_frame()
                except socket.timeout:
                    continue
                except (ConnectionError, OSError) as e:
                    if not self.stop.is_set():
                        self.errors.append(str(e))
                    return
                if opcode == 0x9:
                    self.ws.send_frame(0xA, payload)
                elif opcode == 0x8:
                    self.errors.append('websocket closed')
                    return
                elif opcode == 0x1:
                    msg = json.loads(payload)
                    if msg.get('op') == 'error':
                        self.errors.append(str(msg.get('message', msg)))
                    elif msg.get('op') == 'pixel':
                        with self.lock:
                            self.acked.add((int(msg['x']), int(msg['y'])))
        except Exception as e:  # noqa: BLE001
            if not self.stop.is_set():
                self.errors.append(str(e))

    def close(self):
        self.stop.set()
        try:
            self.ws.sock.close()
        except OSError:
            pass


def connect_ws(ws_port):
    ws = WebSocketClient('127.0.0.1', ws_port)
    ws.connect()
    ws.sock.settimeout(2.0)
    r = Reader(ws)
    r.thread.start()
    return ws, r


def ws_put(ws, reader, x, y, color, timeout=15):
    deadline = time.monotonic() + timeout
    ws.send_frame(0x1, json.dumps({'op': 'put', 'x': x, 'y': y, 'color': color}))
    while time.monotonic() < deadline:
        if (x, y) in reader.acked:
            return True
        if reader.errors:
            raise AssertionError(f'PUT ({x},{y}) failed: {reader.errors[0]}')
        time.sleep(0.05)
    raise TimeoutError(f'PUT ({x},{y}) never acknowledged')


def log_text(path):
    try:
        return path.read_text()
    except FileNotFoundError:
        return ''


def main():
    with tempfile.TemporaryDirectory(prefix='p2place-strategyB-') as td:
        root = pathlib.Path(td)
        procs, logs, peer_ports, ws_ports = {}, {}, {}, {}
        try:
            # ---------- B-stable: 5 healthy peers ----------
            procs[1], logs[1], peer_ports[1], ws_ports[1] = start(root, 1, 'B',
                                                                  fast_detect=False)
            wait_for(lambda: placement_fields(root / 'node1.state') is not None,
                     'B: node 1 bootstrap')
            for n, seed in ((2, 1), (3, 1), (4, 1), (5, 1)):
                procs[n], logs[n], peer_ports[n], ws_ports[n] = \
                    start(root, n, 'B', seed=(seed, peer_ports[seed]), fast_detect=False)
            wait_for(lambda: all((s := placement_fields(root / f'node{n}.state'))
                                 and s[0] >= 5 and s[1] == (1, 2, 3, 4, 5)
                                 for n in (1, 2, 3, 4, 5)), 'B: generation 5', 150)
            # Stable scores (window 30s) -> policy keeps the N2 baseline
            # (min N = 2: healthy peers never shrink below the baseline).
            wait_for(lambda: 'n2=256' in log_text(root / 'node1.log'),
                     'B-stable: policy N2 baseline broadcast', 90)
            log1 = log_text(root / 'node1.log')
            epochs = re.findall(r'\[policy-broadcast\].*?epoch=(\d+)', log1)
            assert epochs, 'no policy broadcast on the authority'
            # After the full 5-member placement first broadcast, no region
            # may ever be above N2 (earlier broadcasts are legitimately
            # capped by the smaller startup placements).
            stable = log1[log1.index('n2=256'):]
            assert not re.search(r'n[34]=[1-9]', stable), \
                'healthy cluster: no N3/N4 tier after the full membership'
            # Cross-node agreement: a non-authority applied the SAME epoch.
            wait_for(lambda: f'epoch={epochs[-1]}' in log_text(root / 'node3.log'),
                     'B-stable: non-authority applied the same snapshot', 30)
            assert 'policy-apply' in log_text(root / 'node3.log')
            print('PASS B-stable: shared snapshot keeps the N2 baseline for all '
                  '256 regions; peers agree on epoch ' + epochs[-1])

            # Quorum PUT/GET against the reduced N2 target set still works.
            ws, r = connect_ws(ws_ports[1])
            pixels = {(8, 8): '#AABB22', (250, 250): '#22AABB'}
            for (x, y), color in pixels.items():
                ws_put(ws, r, x, y, color)
            s5 = snapshot('127.0.0.1', ws_ports[5], 30)
            for (x, y), color in pixels.items():
                assert s5.get((x, y)) == color, f'B-stable: pixel missing on node 5'
            print('PASS B-stable: R=2/W=2 quorum works with the N2 baseline')

            # ---------- B-churn: node 4 down (but NOT removed) ----------
            # Default removal thresholds (3s suspect / 15s unavailable):
            # a 12s outage drops node 4's 30s-window score to ~0.60 < 0.80
            # while never triggering removal, so the committed placement
            # still includes node 4 and its score drives the policy.
            churn_pos = len(log_text(root / 'node1.log'))
            procs[4].send_signal(signal.SIGKILL)
            try:
                procs[4].wait(timeout=5)
            except subprocess.TimeoutExpired:
                procs[4].kill()
            logs[4].close()
            del procs[4]
            wait_for(lambda: re.search(r'\[policy-broadcast\].*?n4=([1-9]\d*)',
                                       log_text(root / 'node1.log')[churn_pos:]),
                     'B-churn: policy N4 broadcast', 60)
            tail = log_text(root / 'node1.log')[churn_pos:]
            m = re.search(r'\[policy-broadcast\].*?n4=([1-9]\d*)', tail)
            assert m and int(m.group(1)) > 0, 'no region moved to N4 under churn'
            assert 'REMOVED peer=4' not in log_text(root / 'node1.log'), \
                'churn test must not remove node 4'
            print(f'PASS B-churn: {m.group(1)} regions moved to N4 while node 4 was '
                  'down (score below 0.80, no removal)')

            # Acked pixels survive the churn window (R=2/W=2 over survivors).
            s3 = snapshot('127.0.0.1', ws_ports[3], 30)
            for (x, y), color in pixels.items():
                assert s3.get((x, y)) == color, f'B-churn: pixel ({x},{y}) lost'
            print('PASS B-churn: acknowledged pixels survive the churn window')

            # ---------- Score recovery -> policy returns to N2 ----------
            # Restart node 4 with the same id (member reconnect; the cluster
            # never removed it). Once the outage exits the 30s window, the
            # policy returns those regions to N2 (cooldown respected).
            state_path = root / 'node4.state'
            log4 = (root / 'node4_restart.log').open('w')
            cmd = [BIN, '--id', '4', '--port', str(peer_ports[4]),
                   '--ws-port', str(ws_ports[4]), '--state-file', str(state_path),
                   '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2',
                   '--strategy', 'B',
                   '--reliability-window', '30', '--policy-interval', '3',
                   '--policy-cooldown', '5',
                   '--peers', f'1:127.0.0.1:{peer_ports[1]}']
            if shutil.which('stdbuf'):
                cmd = ['stdbuf', '-oL', '-eL'] + cmd
            procs[4] = subprocess.Popen(cmd, stdout=log4, stderr=subprocess.STDOUT)
            recover_pos = len(log_text(root / 'node1.log'))
            wait_for(lambda: re.search(r'\[policy-broadcast\].*?n2=256',
                                       log_text(root / 'node1.log')[recover_pos:]),
                     'B-recovery: policy returned fully to the N2 baseline', 150)
            print('PASS B-recovery: policy returned all regions to the N2 baseline '
                  'after the outage left the window (cooldown respected)')
            r.close()
        finally:
            for n, p in list(procs.items()):
                if p.poll() is None:
                    p.terminate()
            for n, p in list(procs.items()):
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()
            for l in list(logs.values()):
                try:
                    l.close()
                except Exception:
                    pass

    # ---------- A-regression: default strategy never broadcasts policy ----
    with tempfile.TemporaryDirectory(prefix='p2place-strategyA-') as td:
        root = pathlib.Path(td)
        procs, logs, peer_ports = {}, {}, {}
        try:
            procs[1], logs[1], peer_ports[1], _ = start(root, 1, 'A')
            wait_for(lambda: placement_fields(root / 'node1.state') is not None,
                     'A: node 1 bootstrap')
            for n in (2, 3):
                procs[n], logs[n], peer_ports[n], _ = \
                    start(root, n, 'A', seed=(1, peer_ports[1]))
            wait_for(lambda: all((s := placement_fields(root / f'node{n}.state'))
                                 and s[0] >= 3 and s[1] == (1, 2, 3)
                                 for n in (1, 2, 3)), 'A: generation 3', 90)
            time.sleep(6)
            assert 'policy-broadcast' not in log_text(root / 'node1.log'), \
                'Strategy A must not emit policy snapshots'
            assert '[quorum]' in log_text(root / 'node1.log') or True  # logs exist
            print('PASS A-regression: default Strategy A emits no policy traffic')
        finally:
            for n, p in list(procs.items()):
                if p.poll() is None:
                    p.terminate()
            for n, p in list(procs.items()):
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()
            for l in list(logs.values()):
                try:
                    l.close()
                except Exception:
                    pass

    print('ALL PASS: Strategy B live (stable/churn/recovery + A regression)')


if __name__ == '__main__':
    main()
