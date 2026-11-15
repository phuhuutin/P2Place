#!/usr/bin/env python3
"""P2Place Lab 06: controlled multi-node churn/regression stress test.

Copy beside tests/test_websocket_manual.py and tests/test_lab04_canvas_sync.py.
Python standard library only. Launches isolated peers and state files; does NOT
alter the repo's state/ directory. Requires the current Lab 06 peer executable.

Scenarios:
 A) 1,2,3,4 join; write distinct pixels through all nodes; verify every view.
 B) kill 4, majority automatically removes it; write while 4 is absent;
    restart 4 ONCE with old state, seed only 2; verify fresh admission/data.
 C) crash authority 1 in a new 3-node cluster, let 2/3 elect/remove it,
    join 4 via 2 and verify acknowledged data and quorum behavior.

The test is intentionally not a proof against arbitrary partitions or all
concurrency races. Actual source expected to support --state-file,
--suspect-after, --unavailable-after, --recovery-stabilization.
"""
import argparse
import json
import pathlib
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_lab04_canvas_sync import open_ws, receive_json, snapshot


def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def await_condition(pred, seconds, label):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            if pred():
                return
        except (OSError, ValueError, IndexError) as exc:
            last = str(exc)
        time.sleep(.20)
    raise AssertionError(f'TIMEOUT {label}; last_error={last}')


def parse_state(path):
    if not path.exists():
        return None
    parts = path.read_text().split()
    if len(parts) < 4 or parts[0] not in ('P2PLACE_BALANCED_V1', 'P2PLACE_BALANCED_V2'):
        return None
    count = int(parts[2])
    return int(parts[1]), tuple(map(int, parts[3:3+count]))


def put_confirm(portno, x, y, color, timeout=12):
    """Only count a write after its matching WebSocket pixel success event."""
    ws = open_ws('127.0.0.1', portno, timeout)
    try:
        ws.send_frame(1, json.dumps({'op': 'put', 'x': x, 'y': y, 'color': color}))
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            ws.sock.settimeout(max(.1, end-time.monotonic()))
            msg = receive_json(ws)
            if msg.get('op') == 'error':
                raise AssertionError(f'PUT rejected port={portno} ({x},{y}): {msg}')
            if msg.get('op') == 'pixel' and int(msg.get('x', -1)) == x and int(msg.get('y', -1)) == y:
                if msg.get('color', '').upper() != color.upper():
                    raise AssertionError(f'PUT color mismatch: {msg}')
                return
        raise AssertionError(f'No PUT completion for ({x},{y}) via ws={portno}')
    finally:
        ws.close()


def live_broadcast_check(cluster, nodes, x, y, color, timeout=12):
    """All already-connected browsers must get the same pixel event without GET."""
    sockets = {}
    try:
        for n in nodes:
            sockets[n] = open_ws('127.0.0.1', cluster.ports[n][1], timeout)
        entry = nodes[0]
        sockets[entry].send_frame(1, json.dumps({'op': 'put', 'x': x, 'y': y, 'color': color}))
        for n, ws in sockets.items():
            until = time.monotonic() + timeout
            while time.monotonic() < until:
                ws.sock.settimeout(max(.1, until-time.monotonic()))
                msg = receive_json(ws)
                if msg.get('op') == 'error':
                    raise AssertionError(f'live event: Node {n} error {msg}')
                if (msg.get('op') == 'pixel' and msg.get('x') == x
                        and msg.get('y') == y and msg.get('color', '').upper() == color.upper()):
                    break
            else:
                raise AssertionError(f'Node {n} did not receive live event without Refresh Canvas')
        print(f'  PASS live browser notification on nodes {list(nodes)} without get_canvas', flush=True)
    finally:
        for ws in sockets.values():
            ws.close()


class Cluster:
    def __init__(self, root, peer, timeout, suspect, unavailable, stabilization):
        self.root, self.peer, self.timeout = root, str(pathlib.Path(peer).resolve()), timeout
        self.suspect, self.unavailable, self.stabilization = suspect, unavailable, stabilization
        self.procs, self.files, self.ports = {}, {}, {}
        self.starts = {}

    def start(self, n, seed=None, reuse=False):
        if not reuse or n not in self.ports:
            self.ports[n] = (port(), port())
        tcp, ws = self.ports[n]
        cmd = [self.peer, '--id', str(n), '--port', str(tcp), '--ws-port', str(ws),
               '--state-file', str(self.root / f'node{n}.state'), '--replicas', '3',
               '--read-quorum', '2', '--write-quorum', '2',
               '--suspect-after', str(self.suspect),
               '--unavailable-after', str(self.unavailable),
               '--recovery-stabilization', str(self.stabilization)]
        if seed is not None:
            cmd += ['--peers', f'{seed}:127.0.0.1:{self.ports[seed][0]}']
        if shutil.which('stdbuf'):
            cmd = ['stdbuf', '-oL', '-eL', *cmd]
        count = self.starts.get(n, 0) + 1
        self.starts[n] = count
        log = self.root / f'node{n}_run{count}.log'
        fh = open(log, 'w', buffering=1)
        self.files[n] = fh
        self.procs[n] = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT)
        print(f'  START node {n} run={count} tcp={tcp} ws={ws} seed={seed}', flush=True)
        await_condition(lambda: self.procs[n].poll() is not None or self.tcp_listening(ws),
                        8, f'node {n} listener')
        if self.procs[n].poll() is not None:
            raise AssertionError(f'node {n} exited during startup; see {log}')

    @staticmethod
    def tcp_listening(p):
        try:
            with socket.create_connection(('127.0.0.1', p), timeout=.2):
                return True
        except OSError:
            return False

    def stop(self, n, abrupt=True):
        proc = self.procs[n]
        if proc.poll() is None:
            proc.kill() if abrupt else proc.terminate()
        try:
            proc.wait(timeout=4)
        except subprocess.TimeoutExpired:
            proc.kill(); proc.wait(timeout=4)
        self.files[n].close()
        print(f'  STOP node {n} abrupt={abrupt}', flush=True)

    def state(self, n):
        return parse_state(self.root / f'node{n}.state')

    def log(self, n):
        return (self.root / f'node{n}_run{self.starts[n]}.log').read_text(errors='replace')

    def expect_members(self, ids, expected_gen=None):
        ids = tuple(ids)
        def check():
            rows = [self.state(i) for i in ids]
            return (all(r is not None and r[1] == ids for r in rows)
                    and len(set(r[0] for r in rows)) == 1
                    and (expected_gen is None or all(r[0] == expected_gen for r in rows))
                    and all(not pathlib.Path(str(self.root / f'node{i}.state') + '.pending').exists()
                            for i in ids))
        await_condition(check, self.timeout, f'committed members={ids} generation={expected_gen}')
        print(f'  PASS placement gen={self.state(ids[0])[0]} members={ids}; no pending', flush=True)

    def check_pixels(self, nodes, expected, label):
        """Retry fresh R=2 snapshots to allow eventual repair/convergence."""
        last = None
        until = time.monotonic() + self.timeout
        while time.monotonic() < until:
            ok = True
            for n in nodes:
                try:
                    pixels = snapshot('127.0.0.1', self.ports[n][1], min(20, self.timeout))
                    wrong = [(xy, rgb, pixels.get(xy)) for xy, rgb in expected.items()
                             if pixels.get(xy) != rgb]
                    if wrong:
                        last = f'node {n}: {len(wrong)} mismatches, sample={wrong[:3]}'
                        ok = False; break
                except Exception as exc:
                    last = f'node {n}: {exc}'; ok = False; break
            if ok:
                print(f'  PASS {label}: {len(expected)} acknowledged pixels via nodes {list(nodes)}', flush=True)
                return
            time.sleep(.5)
        raise AssertionError(f'{label}: {last}')

    def close(self):
        for n, p in self.procs.items():
            if p.poll() is None:
                p.terminate()
        for n, p in self.procs.items():
            try: p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill(); p.wait(timeout=3)
        for f in self.files.values():
            if not f.closed: f.close()


def color(rng):
    return '#%06X' % rng.randrange(0x1000000)


def writes(c, nodes, expected, rng, count, unique_counter):
    for i in range(count):
        # Distinct coordinate for every write; avoid overlapping previous values.
        k = unique_counter[0]; unique_counter[0] += 1
        x = (k * 53 + 7) % 256
        y = (k * 97 + 11) % 256
        # For this test choose first 256 points distinct, then re-use with new color.
        value = color(rng)
        n = nodes[i % len(nodes)]
        put_confirm(c.ports[n][1], x, y, value)
        expected[(x, y)] = value
    print(f'  PASS {count} quorum-confirmed writes via nodes {list(nodes)}', flush=True)




def visible_color(x, y):
    """High-contrast, deterministic pattern that's easy to recognize on the canvas."""
    # 16x16 regions have subtly different color, with a bright within-region checker.
    r = (x * 7 + (y // 16) * 13) & 255
    g = (y * 5 + (x // 16) * 23) & 255
    b = ((x // 16 ^ y // 16) * 27 + x + y * 3) & 255
    return f'#{r:02X}{g:02X}{b:02X}'


def visible_draw(c, expected, args):
    """Stream confirmed writes across every 16x16 region while browsers are open.

    The order visits one pixel in each region before cycling back for its next
    pixel. Even a partial run is visually distributed over the full canvas.
    """
    count = 65536 if args.draw_full_canvas else args.draw_pixels
    if count <= 0:
        return
    nodes = (1, 2, 3, 4)
    urls = [f'http://localhost:{args.web_port}/?ws={c.ports[n][1]}' for n in nodes]
    print('\n=== WATCH THE LIVE DRAWING IN YOUR BROWSER ===', flush=True)
    for n, url in zip(nodes, urls):
        print(f'  Node {n} browser: {url}', flush=True)
    print(f'  Writes: {count} | rate: {args.draw_rate:g} PUT/s '
          f'| opening delay: {args.viewer_wait:g}s', flush=True)
    print('  Start your web frontend separately (e.g. cd web && python3 -m http.server '
          f'{args.web_port}).', flush=True)
    print('  Open at least one URL now. The test will draw into those live browsers.', flush=True)
    if args.viewer_wait:
        time.sleep(args.viewer_wait)

    clients = {}
    period = 1.0 / args.draw_rate
    confirmed = 0
    started = time.monotonic()
    try:
        for n in nodes:
            clients[n] = open_ws('127.0.0.1', c.ports[n][1], args.write_timeout)
        for i in range(count):
            # Full canvas: 256 regions * 256 pixels each = 65,536 unique coords.
            reg = i % 256
            pos = i // 256
            x = (reg % 16) * 16 + (pos % 16)
            y = (reg // 16) * 16 + (pos // 16)
            rgb = visible_color(x, y)
            node = nodes[i % len(nodes)]
            ws = clients[node]
            ws.send_frame(1, json.dumps({'op': 'put', 'x': x, 'y': y, 'color': rgb}))
            deadline = time.monotonic() + args.write_timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(f'VISIBLE DRAW timeout at pixel {i+1}/{count} '
                                         f'node={node} ({x},{y})')
                ws.sock.settimeout(max(.1, remaining))
                try:
                    reply = receive_json(ws)
                except (OSError, TimeoutError) as e:
                    raise AssertionError(f'VISIBLE DRAW websocket receive failed '
                                         f'at {i+1}/{count} node={node}: {e}') from e
                if reply.get('op') == 'error':
                    raise AssertionError(f'VISIBLE DRAW quorum failure at '
                                         f'{i+1}/{count} node={node} ({x},{y}): {reply}')
                if (reply.get('op') == 'pixel' and reply.get('x') == x
                        and reply.get('y') == y):
                    if reply.get('color', '').upper() != rgb:
                        raise AssertionError(f'VISIBLE DRAW mismatched pixel at ({x},{y}): {reply}')
                    break
            expected[(x, y)] = rgb
            confirmed += 1
            if confirmed % 256 == 0 or confirmed == count:
                print(f'  DRAW {confirmed}/{count} acknowledged '
                      f'({time.monotonic()-started:.1f}s elapsed)', flush=True)
            # The rate is a maximum; network / quorum latency can reduce it.
            due = started + confirmed * period
            if due > time.monotonic():
                time.sleep(due - time.monotonic())
    finally:
        for ws in clients.values():
            ws.close()
    print(f'  PASS visible draw: {confirmed} quorum-acknowledged PUTs', flush=True)
    if args.draw_verify:
        c.check_pixels(nodes, expected, 'visible drawing verification')


def scenario_rejoin(c, rng, pixels, args):
    print('SCENARIO A/B: four-node writes -> node 4 crash/removal -> stale first restart', flush=True)
    c.start(1)
    c.start(2, seed=1); c.expect_members((1, 2), 2)
    c.start(3, seed=2); c.expect_members((1, 2, 3), 3)
    c.start(4, seed=2); c.expect_members((1, 2, 3, 4), 4)
    expected = {}; counter = [0]
    writes(c, (1, 2, 3, 4), expected, rng, pixels, counter)
    c.check_pixels((1, 2, 3, 4), expected, 'baseline four nodes')
    visible_draw(c, expected, args)
    live_broadcast_check(c, (1, 2, 3, 4), 250, 250, '#D14F8A')
    expected[(250, 250)] = '#D14F8A'
    c.stop(4)
    c.expect_members((1, 2, 3), 5)
    writes(c, (1, 2, 3), expected, rng, max(8, pixels // 3), counter)
    c.check_pixels((1, 2, 3), expected, 'writes during node 4 outage')
    c.start(4, seed=2, reuse=True)  # Preserve OLD state file; no manual restart.
    c.expect_members((1, 2, 3, 4), 6)
    log = c.log(4)
    if 'STALE->JOINER' not in log:
        raise AssertionError('Node 4 did not report stale generation on first restart')
    if 'release-received' not in log:
        raise AssertionError('Node 4 missed RELEASE after rejoining')
    c.check_pixels((1, 2, 3, 4), expected, 'rejoin preserved newest acknowledged pixels')
    writes(c, (4, 2, 3, 1), expected, rng, max(8, pixels // 3), counter)
    c.check_pixels((1, 2, 3, 4), expected, 'post-rejoin writes')
    return len(expected)


def scenario_authority(c, rng, pixels):
    print('SCENARIO C: authority crash -> survivor election/removal -> new peer joins', flush=True)
    c.start(1)
    c.start(2, seed=1); c.expect_members((1, 2), 2)
    c.start(3, seed=1); c.expect_members((1, 2, 3), 3)
    expected = {}; counter = [0]
    writes(c, (1, 2, 3), expected, rng, max(12, pixels // 2), counter)
    c.stop(1)
    c.expect_members((2, 3), 4)
    if 'RING elected authority=2' not in c.log(2) and 'RING elected authority=2' not in c.log(3):
        raise AssertionError('No evidence Node 2 elected after Node 1 failure')
    c.check_pixels((2, 3), expected, 'surviving nodes after authority crash')
    writes(c, (2, 3), expected, rng, max(8, pixels // 4), counter)
    c.start(4, seed=2)
    c.expect_members((2, 3, 4), 5)
    c.check_pixels((2, 3, 4), expected, 'new node 4 backfilled after authority crash')
    return len(expected)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--peer', default='./build/bin/p2place-peer')
    ap.add_argument('--pixels', type=int, default=64, help='Base PUT count; start at 32/64 then try 256')
    ap.add_argument('--rounds', type=int, default=1)
    ap.add_argument('--seed', type=int, default=20261114)
    ap.add_argument('--timeout', type=float, default=90)
    ap.add_argument('--suspect-after', type=float, default=1)
    ap.add_argument('--unavailable-after', type=float, default=3)
    ap.add_argument('--recovery-stabilization', type=float, default=1)
    ap.add_argument('--skip-authority', action='store_true')
    ap.add_argument('--draw-pixels', type=int, default=2048,
                    help='Visible quorum-confirmed drawing phase (0 disables; max 65536)')
    ap.add_argument('--draw-full-canvas', action='store_true',
                    help='Draw all 65536 pixels (can take a long time)')
    ap.add_argument('--draw-rate', type=float, default=30,
                    help='Maximum visible draw PUTs/second (default: 30)')
    ap.add_argument('--viewer-wait', type=float, default=20,
                    help='Seconds to open browser tabs before visible drawing (default: 20)')
    ap.add_argument('--web-port', type=int, default=8080,
                    help='Port of your separately running P2Place web frontend')
    ap.add_argument('--write-timeout', type=float, default=15,
                    help='Timeout per visible drawing PUT confirmation')
    ap.add_argument('--draw-verify', action='store_true',
                    help='Fetch all four snapshots immediately after visible drawing')
    ap.add_argument('--artifacts', default='', help='Directory to retain isolated state and node logs')
    args = ap.parse_args()
    if args.pixels < 1 or args.rounds < 1:
        ap.error('--pixels and --rounds must be positive')
    if args.draw_pixels < 0 or args.draw_pixels > 65536:
        ap.error('--draw-pixels must be between 0 and 65536')
    if args.draw_rate <= 0 or args.viewer_wait < 0 or args.write_timeout <= 0:
        ap.error('--draw-rate and --write-timeout must be positive; --viewer-wait must be nonnegative')
    if args.unavailable_after <= args.suspect_after:
        ap.error('unavailable-after must be greater than suspect-after')
    peer = pathlib.Path(args.peer).resolve()
    if not peer.is_file():
        ap.error(f'Peer binary not found: {peer}')
    rng = random.Random(args.seed)
    if args.artifacts:
        root = pathlib.Path(args.artifacts).resolve()
        root.mkdir(parents=True, exist_ok=True)
        temp = None
    else:
        temp = tempfile.TemporaryDirectory(prefix='p2place-lab06-stress-')
        root = pathlib.Path(temp.name)
    summary = {'seed': args.seed, 'pixels': args.pixels, 'rounds': [], 'passed': False}
    try:
        for run in range(args.rounds):
            print(f'\n===== ROUND {run+1}/{args.rounds} seed={args.seed} =====', flush=True)
            rroot = root / f'round_{run+1}'
            rroot.mkdir(parents=True, exist_ok=True)
            stats = {}
            for label, fn in [('rejoin', scenario_rejoin)] + ([] if args.skip_authority else [('authority', scenario_authority)]):
                croot = rroot / label
                croot.mkdir(parents=True, exist_ok=True)
                c = Cluster(croot, peer, args.timeout, args.suspect_after,
                            args.unavailable_after, args.recovery_stabilization)
                try:
                    stats[label] = fn(c, rng, args.pixels, args) if label == 'rejoin' else fn(c, rng, args.pixels)
                finally:
                    c.close()
            summary['rounds'].append({'round': run+1, 'passed': True, 'acknowledged_pixels': stats})
        summary['passed'] = True
        print('\nALL PASS: Lab 06 multi-node churn + recovery stress test', flush=True)
        return 0
    except Exception as exc:
        summary['error'] = str(exc)
        print(f'\nFAIL: {exc}', file=sys.stderr, flush=True)
        print(f'Logs and state: {root}', file=sys.stderr, flush=True)
        return 1
    finally:
        (root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        # Preserve temp directory on failure, otherwise remove unless specified.
        if temp is not None:
            if not summary['passed']:
                saved = pathlib.Path.cwd() / f'lab06_stress_failure_{int(time.time())}'
                shutil.copytree(root, saved)
                print(f'Failure artifacts copied to {saved}', file=sys.stderr)
            temp.cleanup()


if __name__ == '__main__':
    sys.exit(main())
