#!/usr/bin/env python3
"""P2Place 10-node targeted-churn experiment, one strategy per invocation (stdlib only).

Purpose: measure whether *adaptive replication* preserves acknowledged writes
when both baseline N=2 owners fail. A deliberately targeted stress test, not a
representative random-churn comparison or evidence that C dominates B.

Requires the actual P2Place WebSocket JSON protocol and verbose [quorum] logs.
Use --help and --dry-run to check compatibility before running.
"""
import argparse
import base64
import csv
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import time
from datetime import datetime, timezone

QUORUM = re.compile(r'\[quorum\]\s+node=1\s+op=PUT\b[^\n]*?\bregion=(\d+)\b[^\n]*?\bN=(\d+)\b[^\n]*?\btargets=([\d,]+)')


def ws_put(port, x, y, color, timeout=2.0):
    """One fresh WebSocket per PUT; a received error is never called an ACK."""
    key = base64.b64encode(os.urandom(16)).decode()
    payload = json.dumps({'op': 'put_pixel', 'x': x, 'y': y, 'color': color}, separators=(',', ':')).encode()
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall((f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\n'
                       f'Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
            hs = b''
            while b'\r\n\r\n' not in hs and len(hs) < 8192:
                c = s.recv(4096)
                if not c: break
                hs += c
            if not hs.startswith(b'HTTP/1.1 101'):
                return False, 'handshake_failed'
            # NOTE: A coalesced frame after the handshake is not expected for this protocol.
            mask = os.urandom(4)
            n = len(payload)
            header = bytes([0x81, 0x80 | n]) if n < 126 else bytes([0x81, 0x80 | 126]) + struct.pack('!H', n)
            s.sendall(header + mask + bytes(v ^ mask[i % 4] for i, v in enumerate(payload)))
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    h = recv_exact(s, 2)
                    opcode = h[0] & 0xf
                    ln = h[1] & 0x7f
                    if ln == 126: ln = struct.unpack('!H', recv_exact(s, 2))[0]
                    if ln == 127: ln = struct.unpack('!Q', recv_exact(s, 8))[0]
                    if ln > 4_000_000: return False, 'oversize_response'
                    masking = recv_exact(s, 4) if h[1] & 0x80 else None
                    data = recv_exact(s, ln)
                    if masking: data = bytes(b ^ masking[i % 4] for i, b in enumerate(data))
                    if opcode == 8: return False, 'closed'
                    if opcode != 1: continue
                    msg = json.loads(data)
                    if not isinstance(msg, dict): continue
                    op = str(msg.get('op', msg.get('type', ''))).lower()
                    if op in ('pixel_updated', 'pixel_update', 'pixel', 'canvas_pixel', 'region_update'):
                        continue  # a broadcast is NOT a write acknowledgment
                    if op in ('put_ok', 'put_pixel_ok', 'put_pixel_ack', 'put_ack'):
                        return True, op
                    if op in ('put_error', 'error', 'put_fail', 'put_failed'):
                        return False, f'{op}:{msg.get("error", "")}'
                    if op in ('put_result', 'put_pixel_result', 'put_response'):
                        if msg.get('success') is True or msg.get('ok') is True:
                            return True, op
                        return False, f'{op}:{msg.get("error", "not_successful")}'
                    # Unknown messages are ignored, never marked ACK.
                except socket.timeout:
                    break
            return False, 'no_recognized_ack'
    except (OSError, ValueError, EOFError, json.JSONDecodeError) as e:
        return False, type(e).__name__ + ':' + str(e)[:100]


def recv_exact(s, n):
    out = bytearray()
    while len(out) < n:
        c = s.recv(n - len(out))
        if not c: raise EOFError('socket closed')
        out.extend(c)
    return bytes(out)


def free_port():
    s = socket.socket(); s.bind(('127.0.0.1', 0)); p = s.getsockname()[1]; s.close(); return p


def wait_port(port, proc, timeout=12):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if proc.poll() is not None: raise RuntimeError(f'node exited early ({proc.returncode})')
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=.15): return
        except OSError: time.sleep(.1)
    raise RuntimeError(f'port {port} did not become ready')


def read_targets(logpath, region):
    if not logpath.exists(): return None
    # Keep only the most recent N=2 quorum for this region.
    text = logpath.read_text(errors='replace')
    for m in reversed(list(QUORUM.finditer(text))):
        if int(m.group(1)) == region and int(m.group(2)) == 2:
            nodes = tuple(int(v) for v in m.group(3).strip(',').split(',') if v)
            if len(nodes) == 2 and len(set(nodes)) == 2: return nodes
    return None


def read_target_n(metrics_dir, region):
    candidates = list(metrics_dir.rglob('policy_node1.csv'))
    if not candidates: return None
    try:
        with candidates[0].open(newline='') as f:
            rows = csv.DictReader(f)
            ns = [int(row.get('assigned_n') or row.get('desired_n') or 0)
                  for row in rows if row.get('region') == str(region)]
            return ns[-1] if ns else None
    except (OSError, ValueError, csv.Error): return None


def start_cluster(args, strategy, outdir, ports):
    nodes = {}
    flags = args.strategy_flag
    common = args.extra_arg
    for node_id in range(1, 11):
        nd = outdir / f'node{node_id}'
        nd.mkdir(parents=True, exist_ok=True)
        cmd = [str(args.binary), '--id', str(node_id), '--port', str(ports[node_id][0]),
               '--ws-port', str(ports[node_id][1]), '--replicas', '3',
               '--read-quorum', '2', '--write-quorum', '2', flags, strategy]
        if node_id != 1:
            cmd += ['--peers', f'1:127.0.0.1:{ports[1][0]}']
        cmd += common
        if args.dry_run:
            print(' '.join(cmd), flush=True)
            continue
        log = (nd / 'node.log').open('w', buffering=1)
        proc = subprocess.Popen(cmd, cwd=nd, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
        nodes[node_id] = {'proc': proc, 'log': log, 'cmd': cmd, 'workdir': nd}
        try:
            wait_port(ports[node_id][1], proc)
        except Exception:
            stop_cluster(nodes)
            raise
        time.sleep(args.join_spacing)
    return nodes


def stop_node(nodes, node_id):
    info = nodes.get(node_id)
    if info and info['proc'].poll() is None:
        os.killpg(info['proc'].pid, signal.SIGKILL)
        try: info['proc'].wait(timeout=3)
        except subprocess.TimeoutExpired: pass


def restart_node(args, nodes, node_id, strategy, ports):
    info = nodes[node_id]
    info['log'].close()
    nd = info['workdir']
    log = (nd / 'node.log').open('a', buffering=1)
    cmd = info['cmd']
    proc = subprocess.Popen(cmd, cwd=nd, stdout=log, stderr=subprocess.STDOUT,
                            start_new_session=True)
    nodes[node_id] = {'proc': proc, 'log': log, 'cmd': cmd, 'workdir': nd}
    wait_port(ports[node_id][1], proc)


def stop_cluster(nodes):
    for node_id in list(nodes):
        try: stop_node(nodes, node_id)
        except (ProcessLookupError, OSError): pass
    for info in nodes.values():
        try: info['log'].close()
        except OSError: pass


def pixel(region, sequence, seed):
    # 16x16 regions, 256x256 canvas. Deterministic, disjoint-ish coords.
    rx, ry = region % 16, region // 16
    index = sequence % 256
    x, y = rx * 16 + index % 16, ry * 16 + index // 16
    digest = hashlib.sha256(f'{seed}:{sequence}'.encode()).digest()
    return x, y, f'#{digest[0]:02X}{digest[1]:02X}{digest[2]:02X}'


def report_event(writer, fh, start, strategy, kind, **fields):
    row = {'time_s': round(time.monotonic()-start, 3), 'strategy': strategy, 'event': kind, **fields}
    writer.writerow({'time_s': row['time_s'], 'strategy': strategy, 'event': kind,
                     'details_json': json.dumps(fields, separators=(',', ':'))})
    fh.flush()
    print(f'[{strategy} {row["time_s"]:6.1f}s] {kind} {fields}', flush=True)


def find_region(args, nodes, ports, known_region, known_owners, seed):
    logfile = nodes[1]['workdir'] / 'node.log'
    candidates = ([known_region] if known_region is not None else
                  [190] + [r for r in range(256) if r != 190])
    for r in candidates:
        x,y,c = pixel(r, 0, seed)
        ws_put(ports[1][1], x,y,c, timeout=args.ack_timeout)
        time.sleep(.10)
        owners = read_targets(logfile, r)
        if owners and 1 not in owners and (known_owners is None or tuple(owners)==tuple(known_owners)):
            return r, owners
    raise RuntimeError('Could not certify a baseline N=2 owner pair excluding gateway 1; '
                       'inspect node1.log quorum output and placement readiness')


def run_trial(args, strategy, root, known_region=None, known_owners=None):
    outdir = root / strategy; outdir.mkdir(parents=True, exist_ok=True)
    ports = {n:(free_port(),free_port()) for n in range(1,11)}
    nodes = {}
    trials = []
    with (outdir/'timeline.csv').open('w', newline='') as fh, (outdir/'puts.csv').open('w',newline='') as pf:
        tw = csv.DictWriter(fh, fieldnames=['time_s','strategy','event','details_json']); tw.writeheader()
        pw = csv.DictWriter(pf,fieldnames=['time_s','phase','seq','region','x','y','color','ack','reason']); pw.writeheader()
        try:
            nodes = start_cluster(args,strategy,outdir,ports)
            if args.dry_run: return {'status':'DRY_RUN'}
            # Cluster startup and joins are not counted inside the 300 s workload.
            time.sleep(args.stabilize)
            r, owners = find_region(args,nodes,ports,known_region,known_owners,args.seed)
            start = time.monotonic()
            report_event(tw,fh,start,strategy,'baseline_ready',region=r,owners=owners,nodes=10)
            if known_region is not None and (r != known_region or owners != tuple(known_owners)):
                raise RuntimeError('B and C did not select the same baseline region and original owners')
            # Warm hot region BEFORE t=48 and throughout C adaptive scale-up.
            seq = 1; phase_totals={}
            next_put = start; killed=False; rejoined=False
            late_fail=False; late_rejoin=False
            while time.monotonic()-start < args.duration:
                elapsed = time.monotonic()-start
                if elapsed >= args.kill_at and not killed:
                    n = read_target_n(nodes[1]['workdir'],r)
                    report_event(tw,fh,start,strategy,'pre_fault_policy',target_n=n)
                    # SIMULTANEOUS means both signals dispatched back-to-back, not sequential graceful exits.
                    for owner in owners:
                        if nodes[owner]['proc'].poll() is None:
                            os.killpg(nodes[owner]['proc'].pid, signal.SIGKILL)
                    for owner in owners:
                        nodes[owner]['proc'].wait(timeout=3)
                    killed=True
                    report_event(tw,fh,start,strategy,'dual_owner_crash',owners=owners,region=r)
                    # Four immediate foreground PUT probes, before waiting for membership timeout.
                    for j in range(4):
                        x,y,color=pixel(r,10000+j,args.seed)
                        ack,reason=ws_put(ports[1][1],x,y,color,args.ack_timeout)
                        t=time.monotonic()-start
                        pw.writerow({'time_s':round(t,3),'phase':'immediate_fault_probe','seq':10000+j,'region':r,
                                     'x':x,'y':y,'color':color,'ack':int(ack),'reason':reason});pf.flush()
                        trials.append({'x':x,'y':y,'color':color,'ack':ack,'reason':reason,'time_s':round(t,3)})
                    report_event(tw,fh,start,strategy,'fault_probes',acks=sum(p['ack'] for p in trials),attempts=len(trials))
                    next_put=max(next_put,time.monotonic())
                if elapsed >= args.rejoin_at and killed and not rejoined:
                    for owner in owners: restart_node(args,nodes,owner,strategy,ports)
                    rejoined=True
                    report_event(tw,fh,start,strategy,'original_owners_rejoined',owners=owners)
                # Optional secondary single-node fault pair, after recovery, identical IDs and times.
                if args.late_fault_node and elapsed >= args.late_kill_at and not late_fail:
                    stop_node(nodes,args.late_fault_node);late_fail=True
                    report_event(tw,fh,start,strategy,'late_single_crash',node=args.late_fault_node)
                if late_fail and not late_rejoin and elapsed >= args.late_rejoin_at:
                    restart_node(args,nodes,args.late_fault_node,strategy,ports);late_rejoin=True
                    report_event(tw,fh,start,strategy,'late_single_rejoin',node=args.late_fault_node)
                if time.monotonic() >= next_put:
                    x,y,color=pixel(r,seq,args.seed)
                    ack,reason=ws_put(ports[1][1],x,y,color,args.ack_timeout)
                    t=time.monotonic()-start
                    phase=('heat' if t < args.kill_at else 'dual_fault' if t < args.rejoin_at
                           else 'recovery' if t < args.heat_until else 'idle')
                    pw.writerow({'time_s':round(t,3),'phase':phase,'seq':seq,'region':r,'x':x,'y':y,
                                 'color':color,'ack':int(ack),'reason':reason});pf.flush()
                    s=phase_totals.setdefault(phase,{'attempts':0,'acks':0});s['attempts']+=1;s['acks']+=int(ack)
                    seq+=1
                    next_put += 1.0 / args.rate
                    if next_put < time.monotonic()-2: next_put=time.monotonic()
                if elapsed >= args.heat_until:
                    # After the writing window ends, no new writes: observe N decay.
                    break
                time.sleep(.025)
            # Keep the cluster alive until t=300 to observe decay, no writes during idle.
            while time.monotonic()-start < args.duration:
                time.sleep(min(1, max(.01, args.duration-(time.monotonic()-start))))
            post_n=read_target_n(nodes[1]['workdir'],r)
            report_event(tw,fh,start,strategy,'end',target_n=post_n,counts=phase_totals)
            result={'status':'COMPLETED','region':r,'baseline_owners':list(owners),
                    'phase_counts':phase_totals,'fault_probes':trials,'final_target_n':post_n,
                    'runtime_s':round(time.monotonic()-start,2),'ports':ports}
            (outdir/'summary.json').write_text(json.dumps(result,indent=2))
            return result
        finally:
            stop_cluster(nodes)


def main():
    ap=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--binary',type=Path,default=Path('build/bin/p2place-peer'))
    ap.add_argument('--output',type=Path,default=Path('experiments/results/bc_targeted'))
    ap.add_argument('--seed',type=int,default=712367)
    ap.add_argument('--strategy',choices=['B','C'],required=True,help='run only this strategy')
    ap.add_argument('--region',type=int,default=None,help='use the same chosen region as another run')
    ap.add_argument('--owners',type=int,nargs=2,default=None,metavar=('NODE1','NODE2'),help='assert identical baseline N=2 owners, useful for B/C comparison')
    ap.add_argument('--duration',type=float,default=300)
    ap.add_argument('--rate',type=float,default=3,help='attempted PUTs/sec; paced, not concurrent')
    ap.add_argument('--kill-at',type=float,default=48)
    ap.add_argument('--rejoin-at',type=float,default=78)
    ap.add_argument('--heat-until',type=float,default=180,help='stop writes; remaining time tests idle decay')
    ap.add_argument('--late-fault-node',type=int,default=0,help='optional additional node crash; 0 disables')
    ap.add_argument('--late-kill-at',type=float,default=114)
    ap.add_argument('--late-rejoin-at',type=float,default=159)
    ap.add_argument('--join-spacing',type=float,default=.3)
    ap.add_argument('--stabilize',type=float,default=12)
    ap.add_argument('--ack-timeout',type=float,default=1.0)
    ap.add_argument('--strategy-flag',default='--strategy',help='change if your binary uses different CLI option')
    ap.add_argument('--extra-arg',action='append',default=[],help='repeat for additional binary flags/values')
    ap.add_argument('--dry-run',action='store_true',help='print node commands without running')
    args=ap.parse_args()
    if not (0 < args.kill_at < args.rejoin_at < args.heat_until <= args.duration): ap.error('fault/heat timeline invalid')
    if args.rate <= 0 or (args.late_fault_node != 0 and not 1 <= args.late_fault_node <= 10):
        ap.error('invalid rate or late fault node')
    if args.region is not None and not 0 <= args.region < 256: ap.error('region must be 0..255')
    if args.owners is not None and (args.region is None or len(set(args.owners)) != 2 or 1 in args.owners or any(x < 1 or x > 10 for x in args.owners)):
        ap.error('--owners requires --region and two distinct nodes in 2..10')
    args.binary=args.binary.resolve()
    if not args.binary.is_file() and not args.dry_run: ap.error(f'missing binary: {args.binary}')
    root=args.output.resolve()/('run_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_seed'+str(args.seed)+'_'+args.strategy)
    root.mkdir(parents=True,exist_ok=False)
    (root/'config.json').write_text(json.dumps({k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},indent=2))
    try:
        result=run_trial(args,args.strategy,root,args.region,args.owners)
        (root/'result.json').write_text(json.dumps({
            'strategy':args.strategy,'seed':args.seed,'result':result,
            'design':'targeted simultaneous failure of baseline N=2 owners; not a representative random failure experiment',
            'limitations':['Inspect actual backfill completion metrics; target N alone is insufficient.',
                           'This script does not independently confirm post-rejoin pixel convergence.']
        },indent=2))
        print('\nRESULTS:',root,'\nStrategy:',args.strategy,'\nRegion:',result.get('region'),
              '\nBaseline owners:',result.get('baseline_owners'),'\nPhases:',result.get('phase_counts'),flush=True)
    except Exception as e:
        (root/'failure.txt').write_text(repr(e)+'\n')
        raise

if __name__=='__main__':main()
