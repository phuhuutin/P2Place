#!/usr/bin/env python3
"""Strategy C (churn + write-aware replication) live tests.

T1 aggregation:  3-node C cluster; writes from every gateway into one hot
                  region; the authority's deduplicated aggregate crosses the
                  high threshold and the policy raises that region to N4.
T2 B contrast:    same cluster under Strategy B: identical writes keep the
                  N2 baseline (n4=0) — B has no write awareness.
T3 controlled:   10-node cluster; hot region = one whose first two preferred
                  owners are nodes 8 and 9. C pre-positions N4 before the
                  crash; after killing 8/9 simultaneously (inside the
                  pre-reconfiguration window) hot writes still ACK via the
                  remaining targets. Under B the same region stays N2 and the
                  same writes fail W=2. Recovery restores convergence.

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
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_websocket_manual import WebSocketClient  # noqa: E402
from test_lab06_strategy_b import Reader, placement_fields, ws_put  # noqa: E402

BIN = str((pathlib.Path(__file__).resolve().parent.parent / 'build/bin/p2place-peer').resolve())


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def wait_for(pred, what, seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            if pred():
                return
        except Exception:
            pass
        time.sleep(0.15)
    raise AssertionError('Timed out: ' + what)


def start(root, n, strategy, seed=None, ports=None):
    if ports:
        peer_port, ws_port = ports
    else:
        peer_port, ws_port = free_port(), free_port()
    log = (root / f'node{n}.log').open('w')
    cmd = [BIN, '--id', str(n), '--port', str(peer_port), '--ws-port', str(ws_port),
           '--state-file', str(root / f'node{n}.state'),
           '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2',
           '--strategy', strategy,
           '--min-replicas', '2', '--max-replicas', '4',
           # Accelerated policy/report clocks for the test.
           '--reliability-window', '20', '--policy-interval', '2',
           '--policy-cooldown', '3',
           '--write-window', '20', '--write-report-interval', '2']
    if seed:
        cmd += ['--peers', f'{seed[0]}:127.0.0.1:{seed[1]}']
    if shutil.which('stdbuf'):
        cmd = ['stdbuf', '-oL', '-eL'] + cmd
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    return proc, log, peer_port, ws_port


def connect_ws(ws_port):
    ws = WebSocketClient('127.0.0.1', ws_port)
    ws.connect()
    ws.sock.settimeout(3.0)
    r = Reader(ws)
    r.thread.start()
    return ws, r


def put_expect(ws, reader, x, y, color, timeout=12):
    """Returns (acked, message). Never raises for a quorum failure."""
    ws.send_frame(0x1, json.dumps({'op': 'put', 'x': x, 'y': y, 'color': color}))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if (x, y) in reader.acked:
            return True, ''
        if reader.errors:
            return False, reader.errors[0]
        time.sleep(0.05)
    return False, 'no ACK within timeout'


def put_retry(ws, reader, x, y, color, attempts=6):
    """Retry a write across early-cluster races (a fresh member connection
    may not be up for the first seconds after the commit)."""
    for attempt in range(attempts):
        ok, why = put_expect(ws, reader, x, y, color)
        if ok:
            return True
        if attempt + 1 < attempts:
            time.sleep(1.0)
    raise AssertionError(f'PUT ({x},{y}) never acknowledged: {why}')


def kill(proc):
    proc.send_signal(signal.SIGKILL)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def log_text(path):
    try:
        return path.read_text()
    except FileNotFoundError:
        return ''


def hot_region_with_pair(root, pair=(8, 9)):
    """Pick a region whose first two preferred owners are `pair` under the
    committed 10-member placement (same logic as the deep diagnostic)."""
    fields = (root / 'node1.state').read_text().split()
    count = int(fields[2])
    members = [int(x) for x in fields[3:3 + count]]
    offset = 3 + count * (2 if fields[0].endswith('V2') else 1)
    owners = [int(x) for x in fields[offset:offset + 256]]
    for r in range(256):
        pref = [members[(members.index(owners[r]) + j) % count] for j in range(4)]
        if pref[0] == pair[0] and pref[1] == pair[1]:
            return r, pref
    raise AssertionError('no region with first-two preferred owners ' + str(pair))


def t1_aggregation():
    """5 nodes, Strategy C: hot writes from all gateways -> N4 for the hot
    region via the authority's multi-gateway aggregate."""
    with tempfile.TemporaryDirectory(prefix='p2place-C-agg-') as td:
        root = pathlib.Path(td)
        procs, logs, peer_ports, ws_ports = {}, {}, {}, {}
        try:
            procs[1], logs[1], peer_ports[1], ws_ports[1] = start(root, 1, 'C')
            wait_for(lambda: placement_fields(root / 'node1.state') is not None,
                     'C: node 1 bootstrap')
            for n in (2, 3, 4, 5):
                procs[n], logs[n], peer_ports[n], ws_ports[n] = \
                    start(root, n, 'C', seed=(1, peer_ports[1]))
            wait_for(lambda: all((s := placement_fields(root / f'node{n}.state'))
                                 and s[0] >= 5 and s[1] == (1, 2, 3, 4, 5)
                                 for n in (1, 2, 3, 4, 5)), 'C: generation 5', 120)
            # Let the first policy broadcast land and target connections settle.
            wait_for(lambda: 'policy-broadcast' in log_text(root / 'node1.log'),
                     'C: initial policy broadcast', 60)
            time.sleep(6)
            # One hot region: 30 writes per gateway = 150 cluster attempts
            # over the 20s window -> 7.5 writes/s >> 1.0 threshold.
            hot = 163
            socks = []
            for n in (1, 2, 3, 4, 5):
                ws, r = connect_ws(ws_ports[n])
                socks.append((ws, r))
                for i in range(30):
                    x = (hot % 16) * 16 + (i % 16)
                    y = (hot // 16) * 16 + (i // 16)
                    put_retry(ws, r, x, y, '#112233')
            for ws, r in socks:
                r.close()
                ws.sock.close()
            wait_for(lambda: re.search(r'\[policy-broadcast\].*?n4=[1-9]\d*',
                                       log_text(root / 'node1.log')),
                     'C: hot region raised to N4', 90)
            print('PASS C-aggregation: multi-gateway write rate raised the hot '
                  'region to N4')
            # The authority's aggregate must have accepted reports from the
            # other gateways (not only its own local counts).
            acc = re.findall(r'\[write-report\] node=1 from=(\d+).*accepted=true',
                             log_text(root / 'node1.log'))
            for n in ('2', '3', '4', '5'):
                assert n in acc, f'authority must aggregate peer {n}'
            print('PASS C-aggregation: authority deduplicated reports from '
                  'gateways 2-5')
        finally:
            for p in procs.values():
                if p.poll() is None:
                    p.terminate()
            for p in procs.values():
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()


def t2_b_contrast():
    """Same writes under Strategy B: no write awareness -> n4 stays 0."""
    with tempfile.TemporaryDirectory(prefix='p2place-B-contrast-') as td:
        root = pathlib.Path(td)
        procs, logs, peer_ports, ws_ports = {}, {}, {}, {}
        try:
            procs[1], logs[1], peer_ports[1], ws_ports[1] = start(root, 1, 'B')
            wait_for(lambda: placement_fields(root / 'node1.state') is not None,
                     'B: node 1 bootstrap')
            for n in (2, 3, 4, 5):
                procs[n], logs[n], peer_ports[n], ws_ports[n] = \
                    start(root, n, 'B', seed=(1, peer_ports[1]))
            wait_for(lambda: all((s := placement_fields(root / f'node{n}.state'))
                                 and s[0] >= 5 and s[1] == (1, 2, 3, 4, 5)
                                 for n in (1, 2, 3, 4, 5)), 'B: generation 5', 120)
            wait_for(lambda: 'policy-broadcast' in log_text(root / 'node1.log'),
                     'B: initial policy broadcast', 60)
            time.sleep(6)
            hot = 163
            socks = []
            for n in (1, 2, 3, 4, 5):
                ws, r = connect_ws(ws_ports[n])
                socks.append((ws, r))
                for i in range(30):
                    x = (hot % 16) * 16 + (i % 16)
                    y = (hot // 16) * 16 + (i // 16)
                    put_retry(ws, r, x, y, '#445566')
            for ws, r in socks:
                r.close()
                ws.sock.close()
            time.sleep(12)  # several policy evals: any N4 would have appeared
            tail = log_text(root / 'node1.log')
            assert 'policy-broadcast' in tail, 'B must broadcast a policy table'
            assert not re.search(r'\[policy-broadcast\].*?n4=[1-9]', tail), \
                'B must ignore write rate entirely (n4=0)'
            print('PASS B-contrast: identical hot writes leave B at N2 (n4=0)')
        finally:
            for p in procs.values():
                if p.poll() is None:
                    p.terminate()
            for p in procs.values():
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()


def t3_two_owner_survival():
    """10 nodes; hot region with first-two owners {8,9}; kill both; C writes
    ACK via the extra targets while the B control fails W=2."""
    results = {}
    for strategy in ('B', 'C'):
        with tempfile.TemporaryDirectory(prefix=f'p2place-{strategy}-two-') as td:
            root = pathlib.Path(td)
            procs, logs, peer_ports, ws_ports = {}, {}, {}, {}
            try:
                procs[1], logs[1], peer_ports[1], ws_ports[1] = start(root, 1, strategy)
                wait_for(lambda: placement_fields(root / 'node1.state') is not None,
                         f'{strategy}: node 1 bootstrap')
                for n in range(2, 11):
                    procs[n], logs[n], peer_ports[n], ws_ports[n] = \
                        start(root, n, strategy, seed=(1, peer_ports[1]))
                wait_for(lambda: all((s := placement_fields(root / f'node{n}.state'))
                                     and s[0] >= 10 and len(s[1]) == 10
                                     for n in range(1, 11)),
                         f'{strategy}: generation 10', 180)
                wait_for(lambda: 'policy-broadcast' in log_text(root / 'node1.log'),
                         f'{strategy}: initial policy broadcast', 60)
                time.sleep(6)
                hot, pref = hot_region_with_pair(root)
                # Preheat: ~30 cluster attempts in the hot region (well above
                # 1.0/s over the 20s window). Gateway = node 2 only (healthy,
                # non-authority, outside the {8,9} pair).
                ws, r = connect_ws(ws_ports[2])
                for i in range(30):
                    x = (hot % 16) * 16 + (i % 16)
                    y = (hot // 16) * 16 + (i // 16)
                    put_retry(ws, r, x, y, f'#{i:06X}')
                # Wait for the policy reaction + cooldown + backfill window.
                if strategy == 'C':
                    wait_for(lambda: re.search(r'\[policy-broadcast\].*?n4=[1-9]\d*',
                                               log_text(root / 'node1.log')),
                             'C: policy reacted to the hot region', 90)
                else:
                    wait_for(lambda: 'policy-broadcast' in log_text(root / 'node1.log'),
                             'B: initial policy broadcast', 60)
                    time.sleep(10)  # any (incorrect) N4 raise would appear here
                log1 = log_text(root / 'node1.log')
                # The hot region's N4 epoch decays once the 20s write window
                # expires, so take the MAX n4 ever broadcast (not the first
                # or last line).
                n4 = max((int(m.group(1))
                          for m in re.finditer(r'\[policy-broadcast\].*?n4=(\d+)', log1)),
                         default=0)
                if strategy == 'C':
                    assert n4 >= 1, 'C must raise the hot region to N4'
                else:
                    assert n4 == 0, 'B must keep the hot region at N2 (n4=0)'
                time.sleep(6)  # extra backfill/eval time for the new targets
                # Kill the first two preferred owners simultaneously.
                kill(procs[8])
                kill(procs[9])
                time.sleep(1)
                # New writes into the hot region through a healthy gateway.
                acked, failed = 0, 0
                for i in range(6):
                    x = (hot % 16) * 16 + ((30 + i) % 16)
                    y = (hot // 16) * 16 + ((30 + i) // 16)
                    ok, why = put_expect(ws, r, x, y, '#ABABAB')
                    if ok:
                        acked += 1
                    else:
                        failed += 1
                results[strategy] = {'n4': n4, 'acked': acked, 'failed': failed,
                                     'pref': pref}
                print(f'[{strategy}] hot region N4 count={n4}; post-crash writes '
                      f'acked={acked} failed={failed}')
                r.close()
            finally:
                for p in procs.values():
                    if p.poll() is None:
                        p.terminate()
                for p in procs.values():
                    try:
                        p.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        p.kill()
    c = results['C']
    b = results['B']
    assert c['n4'] >= 1, 'C must have pre-positioned N4 before the crash'
    assert b['n4'] == 0, 'B control must have stayed at N2'
    assert c['acked'] >= 4, \
        f'C must ACK most post-crash writes via the extra targets ({c})'
    assert b['failed'] >= 4, \
        f'B must fail W=2 on the crashed pair ({b})'
    print('PASS C-survival: after killing both first-two owners, C ACKed '
          f'{c["acked"]}/{c["acked"]+c["failed"]} hot writes while B failed '
          f'{b["failed"]}/{b["acked"]+b["failed"]} (pre-reconfiguration window)')


def main():
    t1_aggregation()
    t2_b_contrast()
    t3_two_owner_survival()
    print('ALL PASS: Strategy C live (aggregation, B contrast, two-owner survival)')


if __name__ == '__main__':
    main()
