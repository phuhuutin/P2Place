#!/usr/bin/env python3
"""Deep P2Place Strategy-B diagnostic: offline forensics, read-only live probes,
   and an isolated destructive churn scenario. stdlib, plus repository WS helpers.

Install in tests/. Run --help for subcommands. This tests externally observable
behavior and evidence; it CANNOT directly inspect per-replica RAM or prove loss.
"""
from __future__ import annotations
import argparse
import collections
import csv
import importlib.util
import json
import pathlib
import random
import re
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'tests'))
from test_lab04_canvas_sync import snapshot, open_ws, receive_json  # noqa


def region(x,y): return (y//16)*16+x//16

def load_csv(p):
    if not p.exists(): return []
    with p.open(newline='') as f: return list(csv.DictReader(f))


def load_expected(result, sample=None):
    """Reconstruct deterministic sample from ACK rows, in COMPLETION order."""
    conf = json.loads((result/'config.json').read_text())
    rows = load_csv(result/'runner_operations.csv')
    ack = [r for r in rows if str(r['success']).lower() in ('true','1')]
    count = min(sample or int(conf.get('verify_pixels', 1500)), len(ack))
    selected = random.Random(int(conf['seed']) + 10007).sample(ack,count)
    return {(int(r['x']),int(r['y'])): str(r['color']).upper() for r in selected}, conf


def classify(actual, expected):
    return 'correct' if actual==expected else ('absent' if actual is None else 'wrong_color')


def analyze(result, out):
    expected, conf = load_expected(result)
    summ = json.loads((result/'summary.json').read_text())
    out.mkdir(parents=True,exist_ok=True)
    policy = load_csv(result/'region_replication_events.csv')
    backfills = []
    for p in (result/'metrics').glob('node*_boot*/replica_backfill_events.csv'):
        try: backfills.extend([{**r, 'source_file':str(p.relative_to(result))} for r in load_csv(p)])
        except (OSError, ValueError): pass
    for p in [result/'replica_backfill_events.csv']:
        if p.exists(): backfills.extend([{**r,'source_file':str(p.relative_to(result))} for r in load_csv(p)])
    n_changed = collections.Counter()
    for e in policy:
        if e.get('event_type')=='n_change':
            try: n_changed[int(e['region'])]+=1
            except (ValueError, KeyError): pass
    examples=[]
    for checkpoint in ('verification_immediate','verification_after_settle','verification'):
        v=summ.get(checkpoint, {})
        for x in v.get('examples',[]):
            k=(int(x['x']),int(x['y']))
            examples.append({'checkpoint':checkpoint,'reader':x.get('reader'),
                             'x':k[0],'y':k[1],'region':region(*k),
                             'expected':x.get('expected'), 'actual':x.get('actual'),
                             'type':classify(x.get('actual'),x.get('expected')),
                             'region_policy_changes':n_changed[region(*k)]})
    with (out/'verification_examples_enriched.csv').open('w',newline='') as f:
        cols=['checkpoint','reader','x','y','region','expected','actual','type','region_policy_changes']
        w=csv.DictWriter(f,fieldnames=cols); w.writeheader(); w.writerows(examples)
    phases={}
    for chk in ('verification_immediate','verification_after_settle'):
        v=summ.get(chk,{})
        phases[chk]={'sampled_pixels':v.get('sampled_pixels'),
                     'reader_nodes':v.get('checked_nodes'),
                     'pixel_node_mismatches':v.get('missing'),
                     'read_failures':v.get('read_failures')}
    hotspots=collections.Counter(r['region'] for r in examples if r['checkpoint']=='verification_after_settle')
    errors=collections.Counter()
    for path in result.glob('node*_boot*.log'):
        for line in path.open(errors='replace'):
            low=line.lower()
            for word in ('[backfill]','[read-repair]','anti-entropy','timeout','policy-ignore','[policy-apply]'):
                if word in low: errors[word]+=1
    report={'strategy':summ.get('strategy'),'seed':summ.get('seed'),
            'acknowledged_sample_reconstructed':len(expected),
            'checkpoints':phases,'known_policy_changed_regions':len(n_changed),
            'policy_change_events':sum(n_changed.values()),
            'backfill_event_count':len(backfills), 'log_keyword_counts':dict(errors),
            'example_hotspots':hotspots.most_common(20),
            'IMPORTANT':'verification examples are capped (often 20): hotspots are NOT complete missing-pixel totals. CSV shows only reported examples. Offline evidence cannot establish replica contents.'}
    (out/'offline_report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
    return expected


def probe(expected, port_map, host, timeout, rounds, interval, out):
    """Read only: repeated complete canvas snapshots from each gateway."""
    out.mkdir(parents=True,exist_ok=True)
    rows=[]; captures=[]
    for tick in range(rounds):
        snapshots={}
        for node,port in sorted(port_map.items()):
            try:
                pixels=snapshot(host,port,timeout)
                snapshots[node]=pixels
                print(f'[snapshot] round={tick} node={node} pixels={len(pixels)}')
            except Exception as e:
                rows.append({'round':tick,'reader':node,'x':'','y':'','region':'',
                             'expected':'','actual':'','status':'snapshot_error','error':str(e)})
        for (x,y), color in expected.items():
            seen=[]
            for node,pixels in snapshots.items():
                actual=pixels.get((x,y)); status=classify(actual,color)
                seen.append((node,status))
                rows.append({'round':tick,'reader':node,'x':x,'y':y,
                             'region':region(x,y),'expected':color,'actual':actual or '',
                             'status':status,'error':''})
            captures.append({'round':tick,'x':x,'y':y,'region':region(x,y),
                             'all_correct':bool(seen) and all(s=='correct' for _,s in seen),
                             'any_correct':any(s=='correct' for _,s in seen),
                             'all_missing':bool(seen) and all(s!='correct' for _,s in seen),
                             'readers_checked':len(seen)})
        mismatches=sum(r['status'] not in ('correct','snapshot_error') for r in rows if r['round']==tick)
        all_bad=sum(c['all_missing'] for c in captures if c['round']==tick)
        partially_bad=sum(c['any_correct'] and not c['all_correct'] for c in captures if c['round']==tick)
        print(f'[probe] round={tick} comparisons_mismatched={mismatches} '
              f'no_gateway_matched={all_bad} gateway_disagreement={partially_bad}')
        if tick+1<rounds: time.sleep(interval)
    for name,data,cols in [
        ('snapshot_comparisons.csv',rows,['round','reader','x','y','region','expected','actual','status','error']),
        ('pixel_gateway_classification.csv',captures,['round','x','y','region','all_correct','any_correct','all_missing','readers_checked'])]:
        with (out/name).open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=cols);w.writeheader();w.writerows(data)
    errors=sum(r['status']=='snapshot_error' for r in rows)
    wrong=sum(r['status'] not in ('correct','snapshot_error') for r in rows if r['round']==rounds-1)
    per_round=[]
    for tick in range(rounds):
        rr=[r for r in rows if r['round']==tick]
        per_round.append({'round':tick,
                          'comparisons':len(rr),
                          'mismatches':sum(r['status'] not in ('correct','snapshot_error') for r in rr),
                          'absent':sum(r['status']=='absent' for r in rr),
                          'wrong_color':sum(r['status']=='wrong_color' for r in rr),
                          'read_errors':sum(r['status']=='snapshot_error' for r in rr)})
    (out/'probe_summary.json').write_text(json.dumps({
        'rounds':rounds,'read_errors':errors,'last_round_mismatches':wrong,
        'last_round_unmatched_on_all_readers':sum(c['all_missing'] for c in captures if c['round']==rounds-1),
        'per_round':per_round,
        'note':'Gateway snapshots are not direct per-replica storage inspection'
    },indent=2)+'\n')
    return rows,captures


def put(port,x,y,color,timeout=8):
    ws=open_ws('127.0.0.1',port,timeout)
    try:
        ws.send_frame(1,json.dumps({'op':'put','x':x,'y':y,'color':color}))
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            ws.sock.settimeout(max(.05,deadline-time.monotonic()))
            try: m=receive_json(ws)
            except socket.timeout: continue
            if m.get('op')=='error': return False,str(m.get('message'))
            if (m.get('op')=='pixel' and m.get('x')==x and m.get('y')==y
                and str(m.get('color')).upper()==color): return True,''
        return False,'no matching pixel ACK'
    except Exception as exc: return False,str(exc)
    finally: ws.close()


def members(path):
    try:
        fields=path.read_text().split();count=int(fields[2]);
        return tuple(int(x) for x in fields[3:3+count])
    except (OSError,IndexError,ValueError): return ()


def pick_port(excluded):
    for i in range(1000):
        with socket.socket() as s:
            s.bind(('127.0.0.1',0)); p=s.getsockname()[1]
        if p not in excluded:excluded.add(p);return p
    raise RuntimeError('No unused port')


@dataclass
class Node:
    node:int; tcp:int; ws:int; process:object=None; fh:object=None; boot:int=0


def isolated(args):
    """Destructive only to own isolated nodes and unique pixels; no other processes touched."""
    args.out.mkdir(parents=True,exist_ok=False)
    used=set();nodes={n:Node(n,pick_port(used),pick_port(used)) for n in range(1,11)}
    peer=pathlib.Path(args.peer).resolve()
    if not peer.exists():raise FileNotFoundError(peer)
    expected={}; events=[]; failures=[]
    def start(n):
        node=nodes[n];node.boot+=1
        fh=(args.out/f'node{n}_boot{node.boot}.log').open('w');node.fh=fh
        cmd=[str(peer),'--id',str(n),'--port',str(node.tcp),'--ws-port',str(node.ws),
             '--state-file',str(args.out/f'node{n}.state'),'--replicas','3',
             '--read-quorum','2','--write-quorum','2','--strategy','B',
             '--min-replicas',str(args.min_replicas),
             '--max-replicas',str(args.max_replicas),
             '--reliability-window',str(args.window),
             '--policy-interval',str(args.policy_interval),
             '--policy-cooldown',str(args.cooldown),
             '--suspect-after','2','--unavailable-after','15',
             '--recovery-stabilization','2',
             '--metrics-dir',str(args.out/f'metrics/node{n}_boot{node.boot}')]
        (args.out/f'metrics/node{n}_boot{node.boot}').mkdir(parents=True)
        if n>1:
            seed=1 if nodes[1].process and nodes[1].process.poll() is None else 2
            cmd+=['--peers',f'{seed}:127.0.0.1:{nodes[seed].tcp}']
        elif node.boot>1:
            cmd+=['--peers',f'2:127.0.0.1:{nodes[2].tcp}']
        node.process=subprocess.Popen(cmd,stdout=fh,stderr=subprocess.STDOUT,cwd=str(ROOT))
        events.append({'t':time.time(),'type':'start','node':n,'boot':node.boot})
        print(f'[start] {n} tcp={node.tcp} ws={node.ws}')
    def stop(n):
        node=nodes[n]
        if node.process and node.process.poll() is None:
            node.process.kill();node.process.wait(timeout=5)
        if node.fh and not node.fh.closed:node.fh.close()
        events.append({'t':time.time(),'type':'kill','node':n,'boot':node.boot})
        print(f'[kill] {n}')
    def all_joined(count,timeout=120):
        goal=tuple(range(1,count+1));end=time.monotonic()+timeout
        while time.monotonic()<end:
            if all(members(args.out/f'node{i}.state')==goal for i in goal):return True
            for i in goal:
                if nodes[i].process.poll() is not None:
                    raise RuntimeError(f'Node {i} exited during join; inspect log')
            time.sleep(.4)
        raise RuntimeError(f'Admission timed out at {count} nodes')
    def writes(stage, regions, per_region, entry):
        # Produce disjoint unique positions per stage and distribute regions evenly.
        good=bad=0
        for rid in regions:
            for j in range(per_region):
                for k in range(256):
                    off=(j*29+k*37+len(expected)*11)%256
                    x=(rid%16)*16+off%16; y=(rid//16)*16+off//16
                    if (x,y) not in expected:break
                else:raise RuntimeError(f'pixel capacity exhausted region={rid}')
                color=f'#{random.Random(args.seed+x*257+y).randrange(0x1000000):06X}'
                ok,why=put(nodes[entry].ws,x,y,color)
                if ok:expected[(x,y)]=color;good+=1
                else: bad+=1;failures.append({'stage':stage,'region':rid,'x':x,'y':y,'error':why})
        print(f'[writes] stage={stage} acked={good} failed={bad} unique_expected={len(expected)}')
    try:
        for i in nodes:
            start(i);all_joined(i)
        # The actual balanced-placement file defines which regions use both 8 and 9.
        fields=(args.out/'node1.state').read_text().split();count=int(fields[2]);
        member_list=[int(x) for x in fields[3:3+count]]
        offset=3+count*(2 if fields[0].endswith('V2') else 1)
        owner_list=[int(x) for x in fields[offset:offset+256]]
        vulnerable=[r for r in range(256) if 8 in [member_list[(member_list.index(owner_list[r])+j)%count] for j in range(3)]
                    and 9 in [member_list[(member_list.index(owner_list[r])+j)%count] for j in range(3)]]
        if not vulnerable:raise RuntimeError('No regions with node8 and node9 in N3')
        vulnerable=vulnerable[:args.regions]
        (args.out/'target_regions.json').write_text(json.dumps(vulnerable,indent=2))
        writes('baseline',vulnerable,args.pixels_per_region,2)
        probe(expected,{n:nodes[n].ws for n in (1,2,3)},'127.0.0.1',args.timeout,1,0,args.out/'baseline')
        # Warm-up crashes: suspect/unavailable are counted unhealthy in rolling window.
        for i in range(args.training_cycles):
            stop(8);time.sleep(args.training_down);start(8);time.sleep(args.training_up)
        print(f'[backfill] waiting {args.backfill_wait}s before double outage')
        time.sleep(args.backfill_wait)
        policy=[]
        for log in sorted(args.out.glob('node*_boot*.log')):
            policy.extend([ln.strip() for ln in log.open(errors='replace') if '[policy-broadcast]' in ln])
        (args.out/'policy_lines.json').write_text(json.dumps(policy[-100:],indent=2))
        if not any(re.search(r'\bn4=[1-9]\d*',l) for l in policy):
            print('[WARN] No N4 policy observed; stress failure may not test intended adaptation')
        writes('pre_outage',vulnerable,args.pixels_per_region,3)
        stop(8);stop(9)
        writes('during_dual_failure',vulnerable,args.pixels_per_region,2)
        start(8);start(9)
        print('[recovery] waiting for rejoins and anti-entropy')
        time.sleep(args.recovery_wait)
        readings,_=probe(expected,{n:nodes[n].ws for n in range(1,11)},'127.0.0.1',args.timeout,
              args.rounds,args.probe_interval,args.out/'recovery')
        first=[r for r in readings if r['round']==0]
        first_bad=sum(r['status'] not in ('correct','snapshot_error') for r in first)
        regs=sorted({r['region'] for r in first if r['status'] not in ('correct','snapshot_error')})
        print(f'[diagnose] round0 mismatches={first_bad} regions={regs}')
        last=[r for r in readings if r['round']==args.rounds-1]
        failed=[r for r in last if r['status']!='correct']
        print(f'[final] {len(failed)} non-matching/read-error comparisons')
        return 2 if failed or not expected else 0
    finally:
        (args.out/'fault_events.json').write_text(json.dumps(events,indent=2))
        (args.out/'write_failures.json').write_text(json.dumps(failures,indent=2))
        (args.out/'acknowledged_pixels.json').write_text(json.dumps(
            [{'x':x,'y':y,'color':c,'region':region(x,y)} for (x,y),c in expected.items()],indent=2))
        for node in nodes.values():
            if node.process and node.process.poll() is None:
                node.process.terminate()
                try:node.process.wait(timeout=3)
                except subprocess.TimeoutExpired:node.process.kill();node.process.wait()
            if node.fh and not node.fh.closed:node.fh.close()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='mode',required=True)
    a=sub.add_parser('analyze',help='Offline forensics of a completed run; no node access')
    a.add_argument('--result',type=pathlib.Path,required=True)
    a.add_argument('--out',type=pathlib.Path,default=None)
    b=sub.add_parser('probe',help='READ ONLY: compare historical ACK sample on live gateway snapshots')
    b.add_argument('--result',type=pathlib.Path,required=True)
    b.add_argument('--ports',nargs='+',required=True,help='node:ws_port, e.g. 1:35515 2:46211')
    b.add_argument('--host',default='127.0.0.1');b.add_argument('--timeout',type=float,default=45)
    b.add_argument('--rounds',type=int,default=4);b.add_argument('--interval',type=float,default=20)
    b.add_argument('--out',type=pathlib.Path,required=True)
    c=sub.add_parser('isolated',help='DESTRUCTIVE to new own processes: churn/backfill/snapshot test')
    c.add_argument('--peer',type=pathlib.Path,default=ROOT/'build/bin/p2place-peer')
    c.add_argument('--out',type=pathlib.Path,required=True)
    c.add_argument('--seed',type=int,default=43);c.add_argument('--regions',type=int,default=12)
    c.add_argument('--pixels-per-region',type=int,default=3)
    c.add_argument('--min-replicas',type=int,default=3)
    c.add_argument('--max-replicas',type=int,default=4)
    c.add_argument('--window',type=int,default=45);c.add_argument('--policy-interval',type=int,default=3)
    c.add_argument('--cooldown',type=int,default=8)
    c.add_argument('--training-cycles',type=int,default=5);c.add_argument('--training-down',type=float,default=7)
    c.add_argument('--training-up',type=float,default=5)
    c.add_argument('--backfill-wait',type=float,default=25)
    c.add_argument('--recovery-wait',type=float,default=40)
    c.add_argument('--rounds',type=int,default=4);c.add_argument('--probe-interval',type=float,default=15)
    c.add_argument('--timeout',type=float,default=45)
    args=p.parse_args()
    if args.mode=='analyze':
        analyze(args.result,args.out or args.result/'deep_diagnostics');return 0
    if args.mode=='probe':
        port_map={int(k):int(v) for pair in args.ports for k,v in [pair.split(':',1)]}
        expected,_=load_expected(args.result)
        rows,_=probe(expected,port_map,args.host,args.timeout,args.rounds,args.interval,args.out)
        last=[r for r in rows if r['round']==args.rounds-1]
        return 2 if not expected or not last or any(r['status']!='correct' for r in last) else 0
    if args.out.exists():p.error('isolated --out must be a new path (safety guard)')
    return isolated(args)

if __name__=='__main__':raise SystemExit(main())
