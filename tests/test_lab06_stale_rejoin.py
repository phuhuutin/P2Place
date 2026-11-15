#!/usr/bin/env python3
"""Lab 06 stale-rejoin regression: first restart after committed removal.

Reproduces the reported bug exactly: a 4-node cluster commits {1,2,3,4},
node 4 is SIGKILLed and removed (gen 5 = {1,2,3}); node 4 restarts ONCE with
its stale gen-4 state dir, seeded ONLY via node 2 (higher-id joiner, lower-id
seed). The SAME process must: detect STALE->JOINER, re-dial its seed
automatically, and reach PREPARE/COMMIT/RELEASE of gen 6 {1,2,3,4} — no
second launch, no manual state deletion.

Test A: full scenario with acked pixels (must survive on node 4).
Test B: short outage — restart before removal; no spurious stale reset;
        quorum reads still work without a full rejoin.
Test C: A repeated 4 more times (5 total) for repeatability.
Test D: same stale rejoin seeded via node 3 only.

Isolated temp state dirs and free ports; repo state/ untouched.
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


def start(root, n, seed_port=None, fast_detect=True, state_path=None, ports=None):
    if ports:
        peer_port, ws_port = ports
    else:
        peer_port, ws_port = free_port(), free_port()
    log = (root / f'node{n}.log').open('w')
    cmd = [BIN, '--id', str(n), '--port', str(peer_port), '--ws-port', str(ws_port),
           '--state-file', str(state_path or (root / f'node{n}.state')),
           '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2']
    if fast_detect:
        cmd += ['--suspect-after', '1', '--unavailable-after', '3',
                '--recovery-stabilization', '1']
    if seed_port:
        cmd += ['--peers', f'{seed_port[0]}:127.0.0.1:{seed_port[1]}']
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


def ws_put(ws, reader, x, y, color, timeout=10):
    deadline = time.monotonic() + timeout
    ws.send_frame(0x1, json.dumps({'op': 'put', 'x': x, 'y': y, 'color': color}))
    while time.monotonic() < deadline:
        if (x, y) in reader.acked:
            return True
        if reader.errors:
            raise AssertionError(f'PUT ({x},{y}) failed: {reader.errors[0]}')
        time.sleep(0.05)
    raise TimeoutError(f'PUT ({x},{y}) never acknowledged')


def run_stale_rejoin(seed_for_restart, with_pixels, label):
    """One full removal + first-restart cycle; returns after asserts."""
    with tempfile.TemporaryDirectory(prefix=f'p2place-stale-{label}-') as td:
        root = pathlib.Path(td)
        procs, logs, ws_ports, peer_ports = {}, {}, {}, {}
        try:
            procs[1], logs[1], peer_ports[1], ws_ports[1] = start(root, 1)
            wait_for(lambda: placement_fields(root / 'node1.state') is not None,
                     f'{label}: node 1 bootstrap')
            procs[2], logs[2], peer_ports[2], ws_ports[2] = \
                start(root, 2, seed_port=(1, peer_ports[1]))
            wait_for(lambda: placement_fields(root / 'node2.state') is not None,
                     f'{label}: node 2 join')
            procs[3], logs[3], peer_ports[3], ws_ports[3] = \
                start(root, 3, seed_port=(1, peer_ports[1]))
            procs[4], logs[4], peer_ports[4], ws_ports[4] = \
                start(root, 4, seed_port=(1, peer_ports[1]))
            wait_for(lambda: all((s := placement_fields(root / f'node{n}.state'))
                                 and s[0] >= 4 and s[1] == (1, 2, 3, 4)
                                 for n in (1, 2, 3, 4)),
                     f'{label}: generation 4', 120)

            pixels = {}
            if with_pixels:
                ws, r = connect_ws(ws_ports[1])
                pixels = {(5, 5): '#AA4433', (250, 250): '#33BB55'}
                for (x, y), color in pixels.items():
                    ws_put(ws, r, x, y, color)
                r.close()

            # Abrupt kill; survivors must commit the removal.
            procs[4].send_signal(signal.SIGKILL)
            try:
                procs[4].wait(timeout=5)
            except subprocess.TimeoutExpired:
                procs[4].kill()
            logs[4].close()
            del procs[4]
            wait_for(lambda: all((s := placement_fields(root / f'node{n}.state'))
                                 and s[0] >= 5 and s[1] == (1, 2, 3)
                                 for n in (1, 2, 3)),
                     f'{label}: removal of node 4', 60)
            assert '[membership] REMOVED peer=4' in (root / 'node1.log').read_text() or \
                   '[membership] REMOVED peer=4' in (root / 'node2.log').read_text(), \
                   f'{label}: removal not committed'

            # ONE restart, stale state dir, seeded only via the chosen node.
            state_path = root / 'node4.state'  # stale gen-4 file survives the kill
            log4 = (root / 'node4_restart.log').open('w')
            cmd = [BIN, '--id', '4', '--port', str(peer_ports[4]),
                   '--ws-port', str(ws_ports[4]), '--state-file', str(state_path),
                   '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2',
                   '--suspect-after', '1', '--unavailable-after', '3',
                   '--recovery-stabilization', '1',
                   '--peers', f'{seed_for_restart}:127.0.0.1:{peer_ports[seed_for_restart]}']
            if shutil.which('stdbuf'):
                cmd = ['stdbuf', '-oL', '-eL'] + cmd
            procs[4] = subprocess.Popen(cmd, stdout=log4, stderr=subprocess.STDOUT)

            try:
                wait_for(lambda: (s := placement_fields(root / 'node4.state')) and
                         s[0] >= 6 and s[1] == (1, 2, 3, 4),
                         f'{label}: first-restart rejoin to generation 6', 120)
            except AssertionError:
                print(f'--- node 4 restart log ({label}) ---')
                print((root / 'node4_restart.log').read_text()[-4000:])
                raise
            text = (root / 'node4_restart.log').read_text()
            assert 'STALE->JOINER' in text, f'{label}: no stale reset on first restart'
            assert '[stale-reset]' in text, f'{label}: no stale-reset detail log'
            assert '[discovery] node=4 seed=' in text, f'{label}: seed redial never logged'
            assert 'prepare-received' in text and 'gen=6' in text, \
                f'{label}: same process never reached PREPARE gen 6'
            assert 'release-received' in text, f'{label}: same process never released'
            assert not pathlib.Path(str(state_path) + '.pending').exists(), \
                f'{label}: .pending left behind'
            assert placement_fields(root / 'node4.state')[0] == 6

            if with_pixels:
                snaps = snapshot('127.0.0.1', ws_ports[4], 30)
                for (x, y), color in pixels.items():
                    assert snaps.get((x, y)) == color, \
                        f'{label}: pixel ({x},{y}) lost after stale rejoin'
            print(f'PASS {label}: single first restart reconnected via seed '
                  f'{seed_for_restart}, joined gen 6, pixels intact')
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
            try:
                log4.close()
            except Exception:
                pass


def test_b_short_outage():
    with tempfile.TemporaryDirectory(prefix='p2place-stale-B-') as td:
        root = pathlib.Path(td)
        procs, logs, ws_ports, peer_ports = {}, {}, {}, {}
        try:
            procs[1], logs[1], peer_ports[1], ws_ports[1] = start(root, 1)
            wait_for(lambda: placement_fields(root / 'node1.state') is not None,
                     'B: node 1 bootstrap')
            procs[2], logs[2], peer_ports[2], ws_ports[2] = \
                start(root, 2, seed_port=(1, peer_ports[1]))
            wait_for(lambda: placement_fields(root / 'node2.state') is not None,
                     'B: node 2 join')
            procs[3], logs[3], peer_ports[3], ws_ports[3] = \
                start(root, 3, seed_port=(1, peer_ports[1]))
            procs[4], logs[4], peer_ports[4], ws_ports[4] = \
                start(root, 4, seed_port=(1, peer_ports[1]))
            wait_for(lambda: all((s := placement_fields(root / f'node{n}.state'))
                                 and s[0] >= 4 and s[1] == (1, 2, 3, 4)
                                 for n in (1, 2, 3, 4)), 'B: generation 4', 120)
            ws, r = connect_ws(ws_ports[1])
            pixels = {(9, 9): '#114477', (240, 100): '#774411'}
            for (x, y), color in pixels.items():
                ws_put(ws, r, x, y, color)
            # Kill and IMMEDIATELY restart (well inside the 3s unavailable
            # window) -> no removal, no stale reset.
            procs[4].send_signal(signal.SIGKILL)
            try:
                procs[4].wait(timeout=5)
            except subprocess.TimeoutExpired:
                procs[4].kill()
            logs[4].close()
            del procs[4]
            state_path = root / 'node4.state'  # current gen-4 state
            log4 = (root / 'node4_restart.log').open('w')
            cmd = [BIN, '--id', '4', '--port', str(peer_ports[4]),
                   '--ws-port', str(ws_ports[4]), '--state-file', str(state_path),
                   '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2',
                   '--suspect-after', '1', '--unavailable-after', '3',
                   '--recovery-stabilization', '1',
                   '--peers', f'2:127.0.0.1:{peer_ports[2]}']
            if shutil.which('stdbuf'):
                cmd = ['stdbuf', '-oL', '-eL'] + cmd
            procs[4] = subprocess.Popen(cmd, stdout=log4, stderr=subprocess.STDOUT)
            # Let it reconnect as a member; no new generation may appear.
            time.sleep(8)
            text = (root / 'node4_restart.log').read_text()
            assert 'STALE->JOINER' not in text, 'B: spurious stale reset on short outage'
            s4 = placement_fields(root / 'node4.state')
            assert s4 and s4[0] == 4 and s4[1] == (1, 2, 3, 4), \
                'B: placement changed on a short outage'
            assert 'REMOVED peer=4' not in (root / 'node1.log').read_text(), \
                'B: node 4 was removed despite quick restart'
            snaps = snapshot('127.0.0.1', ws_ports[4], 30)
            for (x, y), color in pixels.items():
                assert snaps.get((x, y)) == color, \
                    f'B: pixel ({x},{y}) missing after short-outage restart'
            print('PASS B: short outage reconnects as member; no spurious reset, '
                  'no removal, quorum reads work')
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
            try:
                log4.close()
            except Exception:
                pass


def main():
    # Test A: the exact reported failure (node 4, seed 2), with pixels.
    run_stale_rejoin(seed_for_restart=2, with_pixels=True, label='A')
    # Test C: repeat the same scenario 4 more times (5 total).
    for i in range(4):
        run_stale_rejoin(seed_for_restart=2, with_pixels=False, label=f'C{i+1}')
    # Test D: stale rejoin seeded via node 3 only.
    run_stale_rejoin(seed_for_restart=3, with_pixels=False, label='D')
    # Test B: short outage.
    test_b_short_outage()
    print('ALL PASS: stale first-restart rejoin (A/B/C/D)')


if __name__ == '__main__':
    main()
