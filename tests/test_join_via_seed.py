#!/usr/bin/env python3
"""Validate a fresh node joins through a non-authority seed.

Requires a real built p2place-peer. Uses isolated ports/state and checks every
peer has the same committed generation, membership and partition owners.
"""
import argparse
import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def placement(path):
    if not path.exists():
        return None
    fields = path.read_text().split()
    if not fields or fields[0] not in {'P2PLACE_BALANCED_V1', 'P2PLACE_BALANCED_V2'}:
        return None
    generation, count = int(fields[1]), int(fields[2])
    members = tuple(map(int, fields[3:3 + count]))
    offset = 3 + count * (2 if fields[0].endswith('V2') else 1)
    owners = tuple(map(int, fields[offset:offset + 256]))
    return generation, members, owners


def wait_for(predicate, what, seconds=50):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(.15)
    raise AssertionError('Timed out: ' + what)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--peer', default='./build/bin/p2place-peer')
    opts = parser.parse_args()
    binary = pathlib.Path(opts.peer).resolve()
    if not binary.is_file():
        raise SystemExit('Missing peer executable: ' + str(binary))
    with tempfile.TemporaryDirectory(prefix='p2place-join-via-seed-') as d:
        root = pathlib.Path(d)
        ports = {n: (free_port(), free_port()) for n in [1, 2, 4]}
        procs, logs = {}, {}
        def start(n, seed=None):
            cmd = [str(binary), '--id', str(n), '--port', str(ports[n][0]),
                   '--ws-port', str(ports[n][1]),
                   '--state-file', str(root / f'node{n}.state'),
                   '--replicas', '3', '--read-quorum', '2', '--write-quorum', '2']
            if seed:
                cmd += ['--peers', f'{seed}:127.0.0.1:{ports[seed][0]}']
            if shutil.which('stdbuf'):
                cmd = ['stdbuf', '-oL', '-eL'] + cmd
            logs[n] = (root / f'node{n}.log').open('w')
            procs[n] = subprocess.Popen(cmd, stdout=logs[n], stderr=subprocess.STDOUT,
                                        env=os.environ.copy())

        try:
            start(1)
            wait_for(lambda: (s:=placement(root/'node1.state')) and s[0]>=1, 'Node 1 bootstrap')
            start(2, 1)
            wait_for(lambda: all((s:=placement(root/f'node{n}.state')) and s[0]>=2
                                 for n in (1,2)), 'Node 2 joining', 50)
            start(4, 2)  # No Node 1 address on the joining node!
            wait_for(lambda: all((s:=placement(root/f'node{n}.state')) and s[0]>=3
                                 for n in (1,2,4)), 'Node 4 joining through Node 2', 65)
            signatures = [placement(root/f'node{n}.state') for n in (1,2,4)]
            assert signatures[0] == signatures[1] == signatures[2], 'Divergent placement'
            gen, members, owners = signatures[0]
            assert gen == 3 and members == (1,2,4), (gen, members)
            assert len(owners) == 256
            assert '[join-forward]' in (root/'node2.log').read_text(), 'No seed forwarding log'
            assert '[join-referral]' in (root/'node1.log').read_text(), 'Authority received no referral'
            print('PASS: Node 4 joined via Node 2 only')
            print('PASS: all nodes committed identical generation 3 [1,2,4]')
        except Exception:
            for n in (1,2,4):
                f = root/f'node{n}.log'
                if f.exists():
                    print(f'--- Node {n} log ---\n{f.read_text()[-6000:]}')
            raise
        finally:
            for p in procs.values():
                if p.poll() is None:
                    p.terminate()
            for p in procs.values():
                try: p.wait(timeout=3)
                except subprocess.TimeoutExpired: p.kill()
            for f in logs.values(): f.close()


if __name__ == '__main__':
    main()
