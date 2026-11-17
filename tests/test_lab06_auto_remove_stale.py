#!/usr/bin/env python3
"""Lab 06 regression: a queued AUTO_REMOVE must be re-validated at dequeue.

Reproduces the B_10_seed43_extreme defect: node 6 is killed, its removal is
queued while a slow join transition (populated handoff) holds the queue;
node 6 restarts and recovers BEFORE the queue drains. The authority must
DROP the stale removal — never remove a currently healthy, connected member
on a stale suspicion (which would make the member exit permanently via the
graceful-departure path).

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
    return int(fields[1]), tuple(map(int, fields[3:3 + int(fields[2])]))


def wait_for(pred, what, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if pred():
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise AssertionError('Timed out: ' + what)


def start(root, n, seed_port=None, state_path=None, ports=None):
    if ports:
        peer_port, ws_port = ports
    else:
        peer_port, ws_port = free_port(), free_port()
    log = (root / f'node{n}.log').open('w')
    cmd = [BIN, '--id', str(n), '--port', str(peer_port), '--ws-port', str(ws_port),
           '--state-file', str(state_path or (root / f'node{n}.state')),
           '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2',
           '--suspect-after', '1', '--unavailable-after', '3',
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


def write_all_regions(ws, reader, timeout=90):
    """One pixel per region (256) through one WebSocket; W=2 acknowledged."""
    for key in range(256):
        x = (key % 16) * 16
        y = (key // 16) * 16
        color = '#%02X%02X%02X' % ((key * 7) & 255, (key * 13) & 255, (key * 29) & 255)
        ws.send_frame(0x1, json.dumps({'op': 'put', 'x': x, 'y': y, 'color': color}))
        time.sleep(0.004)
    deadline = time.monotonic() + timeout
    while len(reader.acked) < 256 and time.monotonic() < deadline:
        if reader.errors:
            raise AssertionError('PUT rejected: ' + reader.errors[0])
        time.sleep(0.1)
    if len(reader.acked) < 256:
        raise AssertionError(f'only {len(reader.acked)}/256 region writes acknowledged')


def main():
    with tempfile.TemporaryDirectory(prefix='p2place-stale-autoremove-') as td:
        root = pathlib.Path(td)
        procs, logs, peer_ports, ws_ports = {}, {}, {}, {}
        try:
            procs[1], logs[1], peer_ports[1], ws_ports[1] = start(root, 1)
            wait_for(lambda: placement_fields(root / 'node1.state') is not None,
                     'node 1 bootstrap')
            for n in range(2, 7):
                procs[n], logs[n], peer_ports[n], ws_ports[n] = \
                    start(root, n, seed_port=(1, peer_ports[1]))
            wait_for(lambda: all((s := placement_fields(root / f'node{n}.state'))
                                 and s[0] >= 6 and s[1] == (1, 2, 3, 4, 5, 6)
                                 for n in range(1, 7)), 'generation 6', 150)

            # Populate every region: the node-7 join handoff becomes slow
            # enough to hold the removal queue while node 6 recovers.
            ws, r = connect_ws_reader(ws_ports[1])
            write_all_regions(ws, r)
            print('PASS setup: 256 region pixels acknowledged at W=2')
            r.close()

            # Slow join transition first: node 7 joins (handoff ~hundreds of
            # moves) while we kill/restart node 6 underneath it.
            procs[7], logs[7], peer_ports[7], ws_ports[7] = \
                start(root, 7, seed_port=(1, peer_ports[1]))
            t0 = time.monotonic()
            time.sleep(1.0)
            procs[6].send_signal(signal.SIGKILL)
            try:
                procs[6].wait(timeout=5)
            except subprocess.TimeoutExpired:
                procs[6].kill()
            logs[6].close()
            # Wait for the AUTO_REMOVE enqueue (unavailable 3s + 1s stabilize),
            # then restart node 6 IMMEDIATELY — while the join transition
            # still holds the queue.
            wait_for(lambda: 'queued AUTO_REMOVE peer=6' in
                     (root / 'node1.log').read_text(), 'AUTO_REMOVE enqueue', 30)
            logs[6] = (root / 'node6_boot2.log').open('w')
            cmd = [BIN, '--id', '6', '--port', str(peer_ports[6]),
                   '--ws-port', str(ws_ports[6]), '--state-file', str(root / 'node6.state'),
                   '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2',
                   '--suspect-after', '1', '--unavailable-after', '3',
                   '--recovery-stabilization', '1',
                   '--peers', f'1:127.0.0.1:{peer_ports[1]}']
            if shutil.which('stdbuf'):
                cmd = ['stdbuf', '-oL', '-eL'] + cmd
            procs[6] = subprocess.Popen(cmd, stdout=logs[6], stderr=subprocess.STDOUT)
            print(f'PASS setup: node 6 restarted at +{time.monotonic()-t0:.1f}s '
                  '(removal still queued behind the join)')

            # Let the join complete and the queue drain.
            try:
                wait_for(lambda: all((s := placement_fields(root / f'node{n}.state'))
                                     and s[0] >= 7 and s[1] == (1, 2, 3, 4, 5, 6, 7)
                                     for n in range(1, 8)), 'generation 7 with node 6', 120)
            except AssertionError:
                print('--- node1 tail ---')
                print('\n'.join((root / 'node1.log').read_text().splitlines()[-25:]))
                print('--- node6_boot2 tail ---')
                print('\n'.join((root / 'node6_boot2.log').read_text().splitlines()[-15:]))
                print('--- node7 tail ---')
                print('\n'.join((root / 'node7.log').read_text().splitlines()[-12:]))
                raise
            time.sleep(5)  # queue drain + any (incorrect) follow-up transition
            log1 = (root / 'node1.log').read_text()
            assert 'dropped stale AUTO_REMOVE peer=6' in log1, \
                'stale auto-remove was not re-validated at dequeue'
            assert 'REMOVED peer=6' not in log1, \
                'healthy node 6 was removed on a stale suspicion'
            assert procs[6].poll() is None, 'node 6 exited (graceful-departure path)'
            assert 'graceful departure' not in (root / 'node6_boot2.log').read_text(), \
                'node 6 logged a graceful departure'
            gens = {placement_fields(root / f'node{n}.state')[0] for n in range(1, 8)}
            assert len(gens) == 1, f'divergent generations: {gens}'
            print('PASS: stale AUTO_REMOVE dropped; node 6 remains a running member; '
                  'all seven nodes share one generation')

            # Node 6 must serve the acknowledged pixels (survivor handoff +
            # anti-entropy filled it during the join).
            s6 = snapshot('127.0.0.1', ws_ports[6], 45)
            assert (0, 0) in s6 and (240, 240) in s6, \
                f'node 6 lost acknowledged pixels ({len(s6)} present)'
            print('PASS: node 6 serves acknowledged pixels after recovery')
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


def connect_ws_reader(ws_port):
    ws = WebSocketClient('127.0.0.1', ws_port)
    ws.connect()
    ws.sock.settimeout(2.0)
    r = Reader(ws)
    r.thread.start()
    return ws, r


if __name__ == '__main__':
    main()
