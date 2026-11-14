#!/usr/bin/env python3
"""P2Place Lab05 acceptance: actual 3 peers, 3 copies, R2/W2 after one crash.
Requires real nlohmann_json-enabled p2place-peer, not compiler-only mock.
Runs isolated peers and overwrites only pixels in their temporary canvases.
"""
import argparse
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from test_lab04_canvas_sync import open_ws, snapshot, receive_json

def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0))
        return sock.getsockname()[1]

def wait_for(fn, timeout, label):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if fn():return
        time.sleep(.2)
    raise TimeoutError(label)

def manifest_version(file):
    if not file.exists():return 0
    try:return int(file.read_text().split()[1])
    except (OSError,IndexError,ValueError):return 0

def send_put(port,x,y,color):
    ws=open_ws('127.0.0.1',port,6)
    try:
        ws.send_frame(1,json.dumps({'op':'put','x':x,'y':y,'color':color}))
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            ws.sock.settimeout(max(.1,deadline-time.monotonic()))
            msg=receive_json(ws)
            if msg.get('op')=='error':raise AssertionError(f'PUT failed on {port}: {msg}')
            if msg.get('op')=='pixel' and msg.get('x')==x and msg.get('y')==y:
                return
        raise TimeoutError(f'No confirmation on WebSocket {port}')
    finally:ws.close()

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--peer',default='./build/bin/p2place-peer')
    parser.add_argument('--timeout',type=float,default=50)
    args=parser.parse_args()
    executable=str(pathlib.Path(args.peer).resolve())
    ports={i:(free_port(),free_port()) for i in (1,2,3)}
    procs={};logs={}
    with tempfile.TemporaryDirectory(prefix='p2place-lab05-live-') as temp:
        root=pathlib.Path(temp)
        try:
            for i in (1,2,3):
                log=open(root/f'node{i}.log','w',buffering=1);logs[i]=log
                cmd=[executable,'--id',str(i),'--port',str(ports[i][0]),
                     '--ws-port',str(ports[i][1]),'--state-file',str(root/f'node{i}.state'),
                     '--replicas','3','--read-quorum','2','--write-quorum','2']
                if i>1:cmd+=['--peers',f'1:127.0.0.1:{ports[1][0]}']
                if shutil.which('stdbuf'):cmd=['stdbuf','-oL','-eL']+cmd
                procs[i]=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT)
                time.sleep(.3)
            wait_for(lambda:all(manifest_version(root/f'node{i}.state')==3 for i in (1,2,3)),args.timeout,'generation 3 membership')
            # These three pixels are deliberately on three distinct primary regions.
            pixels=[(3,3,'#A132FA'),(18,3,'#15EF29'),(34,3,'#9176BC')]
            for i,(x,y,c) in enumerate(pixels,1):send_put(ports[i][1],x,y,c)
            for i in (1,2,3):
                data=snapshot('127.0.0.1',ports[i][1],args.timeout)
                for x,y,c in pixels:
                    assert data.get((x,y))==c,f'node {i} expected {c} at {x},{y} got {data.get((x,y))}'
            print('PASS: 3 replicated copies reachable through 3 node snapshots')
            procs[3].kill();procs[3].wait(timeout=4)
            time.sleep(1)
            send_put(ports[1][1],54,68,'#DA8501')
            for i in (1,2):
                data=snapshot('127.0.0.1',ports[i][1],args.timeout)
                assert data.get((54,68))=='#DA8501',f'R2 read failed with Node 3 down at reader {i}'
                for x,y,c in pixels:
                    assert data.get((x,y))==c,f'old pixel lost after failure at reader {i}'
            print('PASS: R2/W2 continue with 1 of 3 replicas unavailable')
        except Exception:
            for i,log in logs.items():
                log.flush()
                print(f'--- node {i} last log ---',file=sys.stderr)
                print((root/f'node{i}.log').read_text()[-3500:],file=sys.stderr)
            raise
        finally:
            for proc in procs.values():
                if proc.poll() is None:proc.terminate()
            for proc in procs.values():
                try:proc.wait(timeout=3)
                except subprocess.TimeoutExpired:proc.kill()
            for log in logs.values():log.close()

if __name__=='__main__':main()
