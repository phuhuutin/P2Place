#!/usr/bin/env python3
"""Lab 04+ live membership smoke test. Requires a built p2place-peer binary.

Tests automated joins, coordinated graceful leave, same-ID rejoin, and
identical placement state on every peer. Runs on localhost with fresh state.
This does NOT verify crash recovery of in-memory pixel data (Lab 06).
"""
import argparse
import pathlib
import socket
import subprocess
import tempfile
import time
import os
import shutil


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def wait_for(fn, label, timeout=25):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if fn():
            return
        time.sleep(.12)
    raise AssertionError(f'Timed out waiting for {label}')


def manifest(path):
    parts = path.read_text().split()
    assert parts[0] in ('P2PLACE_BALANCED_V1', 'P2PLACE_BALANCED_V2')
    generation, count = int(parts[1]), int(parts[2])
    members = list(map(int, parts[3:3+count]))
    offset = 3 + count
    if parts[0] == 'P2PLACE_BALANCED_V2':
        offset += count  # admission orders
    owners = list(map(int, parts[offset:offset+256]))
    assert len(owners) == 256
    return generation, members, owners


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--peer', default='./build/bin/p2place-peer')
    args = parser.parse_args()
    exe = pathlib.Path(args.peer).resolve()
    assert exe.is_file(), f'Build peer first: {exe}'
    processes = {}
    files = {}
    ports = {i: (free_port(), free_port()) for i in range(1,5)}
    with tempfile.TemporaryDirectory(prefix='p2place-auto-membership-') as root:
        root = pathlib.Path(root)
        def launch(i, leave_after=None):
            args = [str(exe), '--id', str(i), '--port', str(ports[i][0]),
                    '--ws-port', str(ports[i][1]),
                    '--state-file', str(root / f'node{i}.state')]
            if i != 1:
                args += ['--peers', f'1:127.0.0.1:{ports[1][0]}']
            if leave_after is not None:
                args += ['--leave-after', str(leave_after)]
            if i in files:
                files[i].close()
            files[i] = open(root / f'node{i}.log', 'a', buffering=1)
            if shutil.which('stdbuf'):
                args = ['stdbuf', '-oL', '-eL'] + args
            processes[i] = subprocess.Popen(args, stdout=files[i],
                    stderr=subprocess.STDOUT, env={**os.environ, 'PYTHONUNBUFFERED':'1'})

        def has_generation(i, version):
            path = root / f'node{i}.state'
            return path.is_file() and manifest(path)[0] >= version

        try:
            launch(1)
            wait_for(lambda:has_generation(1,1), 'authority bootstrap')
            # Node 2 leaves only after the 4-node cluster is established.
            launch(2,leave_after=18)
            wait_for(lambda:all(has_generation(i,2) for i in (1,2)), 'Node 2 admission')
            launch(3)
            wait_for(lambda:all(has_generation(i,3) for i in (1,2,3)), 'Node 3 admission')
            launch(4)
            wait_for(lambda:all(has_generation(i,4) for i in (1,2,3,4)), 'Node 4 admission')
            wait_for(lambda:all(has_generation(i,5) for i in (1,2,3,4)), 'Node 2 graceful departure', timeout=35)
            for i in (1,3,4):
                gen, members, owners = manifest(root/f'node{i}.state')
                assert gen == 5 and members == [1,3,4]
            processes[2].terminate()
            processes[2].wait(timeout=3)
            launch(2)  # Same NodeId and the previously committed state.
            wait_for(lambda:all(has_generation(i,6) for i in (1,2,3,4)),
                     'Node 2 rejoining', timeout=35)
            signatures = []
            for i in (1,2,3,4):
                gen, members, owners = manifest(root/f'node{i}.state')
                assert gen == 6 and members == [1,2,3,4]
                assert [owners.count(n) for n in members] == [64,64,64,64]
                signatures.append((gen,tuple(members),tuple(owners)))
            assert len(set(signatures))==1, 'Peers disagree on committed placement'
            # Unexpected failure must NOT cause a membership-version change.
            processes[3].kill()
            processes[3].wait(timeout=3)
            time.sleep(1)
            assert manifest(root/'node1.state')[0]==6
            launch(3)
            wait_for(lambda: 'HELLO from peer 3' in (root/'node1.log').read_text(),
                     'restarted Node 3 HELLO', timeout=12)
            assert manifest(root/'node1.state')[0]==6
            print('PASS: temporary crash did not change canonical ownership')
            print('PASS: automatic joins (2,3,4), graceful leave 2, rejoin 2')
            print('PASS: all peers agree on version 6; balanced 64/64/64/64')
        except Exception:
            for i in range(1,5):
                p = root / f'node{i}.log'
                if p.exists():
                    print(f'--- Node {i} log ---\n{p.read_text()[-4000:]}')
            raise
        finally:
            for p in processes.values():
                if p.poll() is None: p.terminate()
            for p in processes.values():
                try:p.wait(timeout=3)
                except subprocess.TimeoutExpired:p.kill()
            for f in files.values():f.close()

if __name__ == '__main__':
    main()
