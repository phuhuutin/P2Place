#!/usr/bin/env python3
"""Lab06 live browser broadcast test, no browser refresh/get_canvas used.
Run with all three peers already connected and committed, or use --peer to
start an isolated three-peer cluster. Requires the real nlohmann_json build.
"""
import argparse
import json
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time
from test_lab04_canvas_sync import open_ws, receive_json

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0))
        return s.getsockname()[1]

def wait_for_generation(files, seconds=50):
    limit=time.monotonic()+seconds
    while time.monotonic()<limit:
        if all(p.exists() and p.read_text().split()[1]=='3' for p in files):return
        time.sleep(.2)
    raise TimeoutError('cluster did not commit generation 3')

def test(ports):
    clients=[open_ws('127.0.0.1',port,6) for port in ports]
    try:
        for writer in range(3):
            x,y=22+writer*16,33+writer*11
            color=['#AA4433','#33BB55','#4488FF'][writer]
            command={'op':'put','x':x,'y':y,'color':color}
            clients[writer].send_frame(1,json.dumps(command))
            for reader in range(3):
                deadline=time.monotonic()+7
                while time.monotonic()<deadline:
                    clients[reader].sock.settimeout(max(.2,deadline-time.monotonic()))
                    msg=receive_json(clients[reader])
                    if msg.get('op')=='error':raise AssertionError(f'node {reader+1} rejected PUT: {msg}')
                    if msg.get('op')=='pixel' and (msg.get('x'),msg.get('y'))==(x,y):
                        assert msg.get('color','').upper()==color,f'wrong color from node {reader+1}: {msg}'
                        break
                else:raise TimeoutError(f'node {reader+1} missed live pixel from writer {writer+1}')
            print(f'PASS origin Node {writer+1} → all 3 WebSocket clients, no refresh')
    finally:
        for c in clients:c.close()

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--ports',nargs=3,type=int,default=[9001,9011,9021])
    parser.add_argument('--peer',help='Optional executable; launch isolated three-peer cluster')
    args=parser.parse_args()
    if not args.peer:return test(args.ports)
    executable=str(pathlib.Path(args.peer).resolve())
    with tempfile.TemporaryDirectory(prefix='p2place-live-sync-') as td:
        root=pathlib.Path(td);ports=[(free_port(),free_port()) for _ in range(3)]
        procs=[];logs=[]
        try:
            for i in range(1,4):
                log=open(root/f'node{i}.log','w');logs.append(log)
                cmd=[executable,'--id',str(i),'--port',str(ports[i-1][0]),
                    '--ws-port',str(ports[i-1][1]),'--state-file',str(root/f'node{i}.state'),
                    '--replicas','3','--read-quorum','2','--write-quorum','2']
                if i>1:cmd+=['--peers',f'1:127.0.0.1:{ports[0][0]}']
                if shutil.which('stdbuf'):cmd=['stdbuf','-oL','-eL']+cmd
                procs.append(subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT))
                time.sleep(.3)
            wait_for_generation([root/f'node{i}.state' for i in range(1,4)])
            time.sleep(1.5)  # allow endpoint gossip to establish routes
            test([p[1] for p in ports])
        except Exception:
            for i,l in enumerate(logs,1):
                l.flush();print(f'--- node {i} log ---\n'+(root/f'node{i}.log').read_text()[-5000:])
            raise
        finally:
            for p in procs:
                if p.poll() is None:p.terminate()
            for p in procs:
                try:p.wait(timeout=3)
                except subprocess.TimeoutExpired:p.kill()
            for l in logs:l.close()

if __name__=='__main__':main()
