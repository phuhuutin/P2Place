#!/usr/bin/env python3
"""P2Place B/C targeted dual-owner fault experiment; one strategy per command.

A deliberately adversarial demonstration of write-aware replication, NOT a random
churn benchmark or proof that C generally outperforms B.

Launch B first to write a reproducible scenario.json; run C using the same --out-root
and --seed to reuse exactly the same region and original owner pair.

Assumptions: P2Place --strategy B/C, browser put_pixel ACK protocol, [quorum]
PUT logs, policy_node1.csv and replica_repair_events.csv from priority-backfill fix.
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
import signal
import socket
import struct
import subprocess
import time
from datetime import datetime, timezone

QUORUM = re.compile(r'\[quorum\]\s+node=1\s+op=PUT\b[^\n]*?\bregion=(\d+)\b[^\n]*?\bN=(\d+)\b[^\n]*?\btargets=([\d,]+)')


def recv_exact(s, n):
    b = bytearray()
    while len(b) < n:
        chunk = s.recv(n-len(b))
        if not chunk:
            raise EOFError('websocket closed')
        b.extend(chunk)
    return bytes(b)


def ws_put(port, x, y, color, timeout):
    """A broadcast never counts as an ACK. One connection per attempted PUT."""
    key = base64.b64encode(os.urandom(16)).decode()
    msg = json.dumps({'op':'put_pixel','x':x,'y':y,'color':color}, separators=(',',':')).encode()
    try:
        with socket.create_connection(('127.0.0.1',port),timeout=timeout) as s:
            s.settimeout(timeout)
            req = (f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\n'
                   f'Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode()
            s.sendall(req)
            buf = b''
            while b'\r\n\r\n' not in buf and len(buf) < 16384:
                chunk=s.recv(4096)
                if not chunk: return False,'handshake_eof'
                buf+=chunk
            if not buf.startswith(b'HTTP/1.1 101'):
                return False,'handshake_failed'
            head, tail=buf.split(b'\r\n\r\n',1)
            mask=os.urandom(4)
            n=len(msg)
            hdr=(bytes([0x81,0x80|n]) if n<126 else bytes([0x81,0xfe])+struct.pack('!H',n))
            s.sendall(hdr+mask+bytes(b^mask[i%4] for i,b in enumerate(msg)))
            deadline=time.monotonic()+timeout
            while time.monotonic()<deadline:
                def take(n):
                    nonlocal tail
                    while len(tail)<n:
                        chunk=s.recv(max(4096,n-len(tail)))
                        if not chunk: raise EOFError('socket closed')
                        tail+=chunk
                    result,tail=tail[:n],tail[n:]
                    return result
                h=take(2)
                op=h[0]&15; length=h[1]&127
                if length==126: length=struct.unpack('!H',take(2))[0]
                if length==127: length=struct.unpack('!Q',take(8))[0]
                if length>4_000_000: return False,'oversize_response'
                m=take(4) if h[1]&128 else None
                data=take(length)
                if m: data=bytes(b^m[i%4] for i,b in enumerate(data))
                if op==8: return False,'websocket_closed'
                if op!=1: continue
                response=json.loads(data)
                if not isinstance(response,dict): continue
                kind=str(response.get('op',response.get('type',''))).lower()
                if kind in ('pixel_updated','pixel_update','pixel','canvas_pixel','region_update'): continue
                if kind in ('put_ok','put_pixel_ok','put_pixel_ack','put_ack'):
                    return True,kind
                if kind in ('put_error','error','put_fail','put_failed'):
                    return False,kind+':'+str(response.get('error',''))
                if kind in ('put_result','put_pixel_result','put_response'):
                    return response.get('success') is True or response.get('ok') is True,kind
            return False,'ack_timeout'
    except (OSError,EOFError,ValueError,json.JSONDecodeError) as e:
        return False,type(e).__name__+':'+str(e)[:80]


def free_ports(n):
    # Reserve all ports until allocation finished, to avoid duplicate ephemeral selections.
    socks=[]; ports={}
    try:
        for i in range(1,n+1):
            pp=[]
            for _ in range(2):
                s=socket.socket();s.bind(('127.0.0.1',0));socks.append(s);pp.append(s.getsockname()[1])
            ports[i]=pp
        return ports
    finally:
        for s in socks:s.close()


def wait_port(p,proc,timeout=20):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if proc.poll() is not None:raise RuntimeError('peer exited early: '+str(proc.returncode))
        try:
            with socket.create_connection(('127.0.0.1',p),timeout=.2):return
        except OSError:time.sleep(.1)
    raise RuntimeError(f'websocket port {p} never opened')


def kill_process(info):
    if info['proc'].poll() is None:
        try:os.killpg(info['proc'].pid,signal.SIGKILL)
        except ProcessLookupError:pass
        try:info['proc'].wait(timeout=3)
        except subprocess.TimeoutExpired:pass


def launch(args,strategy,out,ports):
    nodes={}
    try:
        for i in range(1,args.nodes+1):
            nd=out/f'node{i}';nd.mkdir(parents=True,exist_ok=True)
            cmd=[str(args.peer),'--id',str(i),'--port',str(ports[i][0]),'--ws-port',str(ports[i][1]),
                 '--replicas','3','--read-quorum','2','--write-quorum','2','--strategy',strategy,
                 '--min-replicas','2','--max-replicas','4',
                 '--policy-interval',str(args.policy_interval),'--policy-cooldown',str(args.policy_cooldown),
                 '--write-window',str(args.write_window),'--write-report-interval',str(args.write_report_interval),
                 '--write-threshold-mid',str(args.write_threshold_mid),'--write-threshold-high',str(args.write_threshold_high)]
            if i!=1:cmd+=['--peers',f'1:127.0.0.1:{ports[1][0]}']
            cmd+=args.peer_arg
            fh=(nd/'node.log').open('a',buffering=1)
            proc=subprocess.Popen(cmd,cwd=nd,stdout=fh,stderr=subprocess.STDOUT,start_new_session=True)
            nodes[i]={'proc':proc,'log':fh,'cmd':cmd,'dir':nd}
            wait_port(ports[i][1],proc)
            time.sleep(args.join_spacing)
        return nodes
    except Exception:
        teardown(nodes)
        raise


def teardown(nodes):
    for info in nodes.values():
        try:kill_process(info)
        except (OSError,ProcessLookupError):pass
    for info in nodes.values():
        try:info['log'].close()
        except OSError:pass


def restart(nodes,i,ports):
    info=nodes[i]
    info['log'].close()
    fh=(info['dir']/'node.log').open('a',buffering=1)
    proc=subprocess.Popen(info['cmd'],cwd=info['dir'],stdout=fh,stderr=subprocess.STDOUT,start_new_session=True)
    nodes[i]=dict(info,log=fh,proc=proc)
    wait_port(ports[i][1],proc)


def pixel(region,sequence,seed):
    x=(region%16)*16+(sequence%16)
    y=(region//16)*16+((sequence//16)%16)
    d=hashlib.sha256(f'{seed}:{sequence}'.encode()).digest()
    return x,y,f'#{d[0]:02X}{d[1]:02X}{d[2]:02X}'


def quorum_records(log,region):
    if not log.exists():return []
    text=log.read_text(errors='replace')
    out=[]
    for m in QUORUM.finditer(text):
        if int(m.group(1))==region:
            out.append((int(m.group(2)),tuple(int(v) for v in m.group(3).strip(',').split(',') if v)))
    return out


def find_selection(args,nodes,ports,region_constraint=None,owners_constraint=None):
    candidates=([region_constraint] if region_constraint is not None else
                random.Random(args.seed).sample(range(256),256))
    for region in candidates:
        x,y,c=pixel(region,-1,args.seed)
        ws_put(ports[1][1],x,y,c,args.ack_timeout)
        time.sleep(.1)
        records=quorum_records(nodes[1]['dir']/'node.log',region)
        baseline=next((targets[:2] for n,targets in reversed(records) if n==2 and len(targets)>=2),None)
        if baseline and len(set(baseline))==2 and 1 not in baseline:
            if owners_constraint is None or list(baseline)==list(owners_constraint):
                return region,list(baseline)
    raise RuntimeError('No matching N=2 region and original owners excluding gateway 1; inspect node1/node.log')


def wait_full_cluster(nodes,expected,timeout=90):
    """Require the authority's committed placement to contain all started nodes."""
    log=nodes[1]['dir']/'node.log'
    pattern=re.compile(r'\[placement\] committed generation=(\d+)[^\n]* owners=(\d+)')
    until=time.monotonic()+timeout
    while time.monotonic()<until:
        if nodes[1]['proc'].poll() is not None:
            raise RuntimeError('authority exited before cluster reached full membership')
        text=log.read_text(errors='replace') if log.exists() else ''
        matches=list(pattern.finditer(text))
        if matches and int(matches[-1].group(2))==expected:
            return int(matches[-1].group(1))
        time.sleep(.5)
    raise RuntimeError(f'cluster did not reach {expected} committed members within {timeout}s')


def latest_policy(node_dir,region):
    paths=list(node_dir.rglob('policy_node1.csv'))
    if not paths:return None
    last=None
    for p in paths:
        try:
            with p.open(newline='') as f:
                for row in csv.DictReader(f):
                    if row.get('region')==str(region):last=row
        except (OSError,csv.Error):pass
    if not last:return None
    try:
        return {'n':int(last.get('assigned_n') or 0),'epoch':int(last['policy_epoch']),
                'gen':int(last['placement_gen'])}
    except (ValueError,KeyError):return None


def complete_backfills(out,region,gen):
    events={}
    for p in out.glob('node*/**/replica_repair_events.csv'):
        try:
            with p.open(newline='') as f:
                for r in csv.DictReader(f):
                    if (r.get('region')==str(region) and r.get('placement_gen')==str(gen)
                        and r.get('event')=='backfill_completed' and r.get('classification')=='complete'):
                        node=int(r['node_id']);epoch=int(r['policy_epoch'])
                        events.setdefault(node,[]).append(epoch)
        except (OSError,ValueError,csv.Error):continue
    return events


def readiness(args,strategy,out,nodes,region,owners):
    """Evaluate readiness against actual PUT preference targets and completion evidence."""
    policy=latest_policy(nodes[1]['dir'],region)
    records=quorum_records(nodes[1]['dir']/'node.log',region)
    if not policy or not records:return False,{'reason':'policy_or_quorum_missing','policy':policy}
    n,targets=records[-1]
    result={'policy':policy,'last_put_n':n,'last_put_targets':list(targets),'original_owners':owners}
    if strategy=='B':
        return policy['n']==2 and n==2 and list(targets[:2])==owners,result
    if policy['n']!=4 or n!=4 or len(targets)<4 or list(targets[:2])!=owners:
        return False,dict(result,reason='not_N4_or_owner_order_changed')
    extras=list(targets[2:4]);events=complete_backfills(out,region,policy['gen'])
    # Must find complete backfill for BOTH newly added nodes at or after policy epoch
    # at which each became a target. We infer join epochs from policy changes, not
    # simply from the latest N=4 row.
    joins={}
    path=next(iter(nodes[1]['dir'].rglob('policy_node1.csv')),None)
    if path:
        try:
            with path.open(newline='') as f:
                for row in csv.DictReader(f):
                    if row.get('region')!=str(region) or int(row['placement_gen'])!=policy['gen']:continue
                    if int(row['assigned_n']) in (3,4):
                        epoch=int(row['policy_epoch'])
                        # The first transition to N=3 adds preference index 2,
                        # transition to N=4 adds preference index 3.
                        if int(row['assigned_n'])==3:joins.setdefault(2,epoch)
                        if int(row['assigned_n'])==4:joins.setdefault(3,epoch)
        except (OSError,ValueError,csv.Error):pass
    checks=[]
    for index,node in enumerate(extras,start=2):
        join_epoch=joins.get(index)
        valid=join_epoch is not None and any(e>=join_epoch for e in events.get(node,[]))
        checks.append({'node':node,'join_epoch':join_epoch,'complete_epochs':events.get(node,[]),'ready':valid})
    result.update({'additional_replicas':checks,'placement_gen':policy['gen']})
    return all(c['ready'] for c in checks) and len(checks)==2,result


def main():
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--strategy',required=True,choices=['B','C'])
    p.add_argument('--seed',type=int,default=712367)
    p.add_argument('--out-root',type=Path,default=Path('experiments/results/bc_adversarial'))
    p.add_argument('--peer',type=Path,default=Path('build/bin/p2place-peer'))
    p.add_argument('--peer-arg',action='append',default=[],help='extra peer argument; repeat as needed')
    p.add_argument('--nodes',type=int,default=10)
    p.add_argument('--seconds',type=float,default=300)
    p.add_argument('--rate',type=float,default=6)
    p.add_argument('--kill-at',type=float,default=48)
    p.add_argument('--rejoin-at',type=float,default=78)
    p.add_argument('--heat-until',type=float,default=180)
    p.add_argument('--probes',type=int,default=8)
    p.add_argument('--ack-timeout',type=float,default=.8)
    p.add_argument('--join-spacing',type=float,default=.3)
    p.add_argument('--stabilize',type=float,default=15)
    p.add_argument('--policy-interval',type=int,default=2)
    p.add_argument('--policy-cooldown',type=int,default=3)
    p.add_argument('--write-window',type=int,default=60)
    p.add_argument('--write-report-interval',type=int,default=2)
    p.add_argument('--write-threshold-mid',type=float,default=.2)
    p.add_argument('--write-threshold-high',type=float,default=1.)
    args=p.parse_args()
    if args.nodes!=10:p.error('this targeted experiment requires exactly 10 nodes')
    if not (0<args.kill_at<args.rejoin_at<args.heat_until<args.seconds):p.error('invalid timeline')
    if args.rate<=0 or args.probes<1:p.error('invalid rate/probes')
    args.peer=args.peer.resolve()
    if not args.peer.is_file():p.error('peer binary missing: '+str(args.peer))
    root=args.out_root.resolve()/f'seed{args.seed}'
    root.mkdir(parents=True,exist_ok=True)
    manifest=root/'scenario.json'
    scenario=json.loads(manifest.read_text()) if manifest.exists() else None
    if scenario and scenario.get('seed')!=args.seed:raise RuntimeError('scenario seed mismatch')
    out=root/args.strategy
    if out.exists():raise RuntimeError(f'{out} exists; choose a new --out-root or move the previous run')
    out.mkdir()
    (out/'config.json').write_text(json.dumps({k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},indent=2))
    ports=free_ports(args.nodes)
    nodes={}
    summary={'strategy':args.strategy,'seed':args.seed,'status':'INCONCLUSIVE','ports':ports}
    try:
        nodes=launch(args,args.strategy,out,ports)
        summary['initial_placement_gen']=wait_full_cluster(nodes,args.nodes)
        time.sleep(args.stabilize)
        reg,owners=find_selection(args,nodes,ports,scenario['region'] if scenario else None,
                                  scenario['owners'] if scenario else None)
        if scenario is None:
            manifest.write_text(json.dumps({'seed':args.seed,'region':reg,'owners':owners,'nodes':args.nodes},indent=2))
        summary.update(region=reg,original_owners=owners)
        print(f'[{args.strategy}] SHARED SCENARIO region={reg} baseline_owners={owners}',flush=True)
        start=time.monotonic();next_write=start
        killed=False;rejoined=False;readiness_at_fault=None
        rows=[];probes=[]
        with (out/'puts.csv').open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=['time_s','phase','sequence','region','x','y','color','ack','reason']);w.writeheader()
            def put(sequence,phase):
                x,y,color=pixel(reg,sequence,args.seed)
                ok,reason=ws_put(ports[1][1],x,y,color,args.ack_timeout)
                row={'time_s':round(time.monotonic()-start,3),'phase':phase,'sequence':sequence,
                     'region':reg,'x':x,'y':y,'color':color,'ack':int(ok),'reason':reason}
                w.writerow(row);f.flush();rows.append(row)
                return row
            seq=1
            while time.monotonic()-start<args.seconds:
                now=time.monotonic()-start
                if not killed and now>=args.kill_at:
                    ready,detail=readiness(args,args.strategy,out,nodes,reg,owners)
                    summary['pre_fault_readiness']=detail
                    if not ready:
                        raise RuntimeError('INCONCLUSIVE: pre-fault readiness not certified; crash NOT executed. Details: '+json.dumps(detail))
                    readiness_at_fault=detail
                    print(f'[{args.strategy} {now:.1f}s] pre-fault readiness PASS: {detail}',flush=True)
                    # Send kills back-to-back, before membership reconfiguration.
                    for owner in owners:
                        os.killpg(nodes[owner]['proc'].pid,signal.SIGKILL)
                    for owner in owners:nodes[owner]['proc'].wait(timeout=3)
                    killed=True
                    summary['kill_time_s']=round(time.monotonic()-start,3)
                    print(f'[{args.strategy}] SIMULTANEOUS CRASH {owners}',flush=True)
                    for j in range(args.probes):
                        probes.append(put(10000+j,'immediate_fault_probe'))
                    summary['probe_ack_count']=sum(v['ack'] for v in probes)
                    summary['probe_attempt_count']=len(probes)
                    print(f'[{args.strategy}] IMMEDIATE PROBES {summary["probe_ack_count"]}/{len(probes)} ACK',flush=True)
                    next_write=time.monotonic()
                if killed and not rejoined and time.monotonic()-start>=args.rejoin_at:
                    for owner in owners:restart(nodes,owner,ports)
                    rejoined=True
                    summary['rejoin_time_s']=round(time.monotonic()-start,3)
                    print(f'[{args.strategy}] OWNERS REJOINED',flush=True)
                now=time.monotonic()-start
                if now<args.heat_until and time.monotonic()>=next_write:
                    phase='heat' if now<args.kill_at else 'fault_window' if now<args.rejoin_at else 'recovery'
                    put(seq,phase);seq+=1
                    next_write+=1/args.rate
                    if next_write<time.monotonic()-1:next_write=time.monotonic()
                time.sleep(.01 if now<args.heat_until else .25)
        counts={}
        for r in rows:
            x=counts.setdefault(r['phase'],{'attempts':0,'acks':0})
            x['attempts']+=1;x['acks']+=r['ack']
        for x in counts.values():x['success_pct']=round(100*x['acks']/x['attempts'],2) if x['attempts'] else 0
        summary.update(status='COMPLETED',phase_counts=counts,final_policy=latest_policy(nodes[1]['dir'],reg),
                       runtime_s=round(time.monotonic()-start,2),
                       design='deliberately targeted dual-original-owner crash; not general random-churn performance')
        print(f'[{args.strategy}] COMPLETE: {json.dumps(counts)}',flush=True)
    except Exception as e:
        summary['error']=repr(e)
        print(f'[{args.strategy}] {e}',flush=True)
        raise
    finally:
        (out/'summary.json').write_text(json.dumps(summary,indent=2))
        teardown(nodes)
        print('RESULTS:',out,flush=True)

if __name__=='__main__':main()
