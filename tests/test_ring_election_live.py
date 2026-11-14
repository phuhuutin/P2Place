#!/usr/bin/env python3
"""3-node local TCP failover smoke test (no external Python deps).

Usage: python3 tests/test_ring_election_live.py --peer ./build/bin/p2place-peer
Creates temporary isolated peer state and ports. Real JSON/CMake build required.
"""
import argparse
import pathlib
import socket
import subprocess
import tempfile
import time

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

def wait_for(path, match, timeout=20):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if path.exists() and match in path.read_text(errors='replace'):
            return
        time.sleep(.2)
    raise AssertionError(f'{path.name}: did not see {match!r}\n'+
                         (path.read_text(errors='replace')[-2000:] if path.exists() else '<no log>'))

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--peer', default='./build/bin/p2place-peer')
    args=ap.parse_args()
    peer=str(pathlib.Path(args.peer).resolve())
    with tempfile.TemporaryDirectory(prefix='p2place-ring-') as tmp:
        work=pathlib.Path(tmp)
        ports={i:(free_port(), free_port()) for i in (1,2,3)}
        procs=[]
        handles=[]
        def start(i, attempt=1):
            logfile=work/f'node{i}-{attempt}.log'
            h=logfile.open('w')
            handles.append(h)
            cmd=['stdbuf','-oL','-eL',peer,'--id',str(i),
                 '--port',str(ports[i][0]),'--ws-port',str(ports[i][1]),
                 '--state-file',str(work/f'node{i}.state')]
            if i!=1:
                cmd+=['--peers',f'1:127.0.0.1:{ports[1][0]}']
            elif attempt>1:
                cmd+=['--peers',f'2:127.0.0.1:{ports[2][0]},3:127.0.0.1:{ports[3][0]}']
            process=subprocess.Popen(cmd,stdout=h,stderr=subprocess.STDOUT)
            procs.append(process)
            return process,logfile
        try:
            p1,l1=start(1)
            time.sleep(.5)
            p2,l2=start(2)
            wait_for(l2,'RELEASE, active=2')
            p3,l3=start(3)
            wait_for(l3,'RELEASE, active=3')
            print('PASS: joined 3 nodes, committed generation 3')
            p1.terminate()
            p1.wait(timeout=5)
            wait_for(l2,'RING elected authority=2')
            wait_for(l3,'learned authority=2')
            print('PASS: Oldest-member Ring election selected Node 2 after Node 1 failure')
            restarted,lr=start(1,2)
            wait_for(lr,'learned authority=2')
            print('PASS: recovering Node 1 learned replacement authority')
        finally:
            for p in procs:
                if p.poll() is None:p.terminate()
            for p in procs:
                try:p.wait(timeout=5)
                except subprocess.TimeoutExpired:p.kill();p.wait()
            for h in handles:h.close()

if __name__=='__main__':main()
