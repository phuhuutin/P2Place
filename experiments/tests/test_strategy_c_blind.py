#!/usr/bin/env python3
"""Independent black-box P2Place Strategy C tests.

Requires only Python stdlib, build/bin/p2place-peer and the existing
`test_websocket_manual.py` WebSocket framing helper. No imports of P2Place
Strategy C test harness or policy calculation code.

Launches isolated peers on randomly allocated ports; never touches an
existing cluster. Results/logs retained in --artifacts.
"""
import argparse
import csv
import json
import pathlib
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback
import datetime

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_websocket_manual import WebSocketClient

LOG_POLICY = re.compile(r'\[policy-broadcast\].*\bn2=(\d+)\s+n3=(\d+)\s+n4=(\d+)')


def free_port():
    s = socket.socket(); s.bind(('127.0.0.1', 0)); port = s.getsockname()[1]; s.close()
    return port


def until(check, label, timeout=90, interval=.15):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            x = check()
            if x: return x
        except (OSError, ValueError, KeyError, IndexError) as exc:
            last = str(exc)
        time.sleep(interval)
    raise AssertionError(f'timeout waiting for {label}; last={last}')


def placement(path):
    if not path.is_file(): return None
    f = path.read_text().split()
    if not f or f[0] not in ('P2PLACE_BALANCED_V1','P2PLACE_BALANCED_V2'):return None
    gen, size = int(f[1]), int(f[2]); members = tuple(map(int, f[3:3+size]))
    off = 3 + size * (2 if f[0].endswith('V2') else 1)
    owners = tuple(map(int, f[off:off+256]))
    if len(owners)!=256 or any(o not in members for o in owners):return None
    return gen,members,owners


def preference(p, region, count=4):
    _,members,owners=p
    idx=members.index(owners[region])
    return tuple(members[(idx+j)%len(members)] for j in range(min(count,len(members))))


def region_pixel(region,index):
    assert 0 <= region < 256 and 0 <= index < 256
    return (region%16)*16+index%16,(region//16)*16+index//16


def color_for(seed, index):
    # Unique color per request, with no dependence on the tested implementation.
    return f'#{(seed*101 + index*7919) % 0xFFFFFF:06X}'


def log_text(path):
    return path.read_text(errors='replace') if path.exists() else ''


def latest_n(root):
    matches=LOG_POLICY.findall(log_text(root/'node1.log'))
    return tuple(map(int,matches[-1])) if matches else None


def current_region_n(root,region):
    # Authority's direct, versioned, accepted N-change records, not a sum
    # across all regions; ignore a partially written line.
    path=root/'metrics1'/'region_replication_events.csv'
    if not path.exists():return None
    current=None
    with path.open(newline='') as fh:
        for row in csv.DictReader(fh):
            try:
                if row['event_type'] in ('initial','n_change') and int(row['region'])==region:
                    current=int(row['new_n'])
            except (ValueError,KeyError,TypeError):continue
    return current


def completed_backfill(root, node, region):
    path=root/f'metrics{node}'/'replica_backfill_events.csv'
    if not path.exists():return False
    with path.open(newline='') as fh:
        for r in csv.DictReader(fh):
            try:
                if int(r['region'])==region and r['result'].lower() in ('ok','true','1','success') and 'classification=complete_partial' not in r.get('reason','') and 'empty_partial_accepted' not in r.get('reason',''):
                    return True
            except (KeyError, ValueError):pass
    # A replica can already have the data from normal successful PUTs and
    # need no backfill event. The test never treats absence as proof of readiness.
    return False


def backfill_diagnostics(root, nodes, region):
    """Summarize source evidence; never interpret missing metrics as success."""
    lines=[f'authority target N={current_region_n(root,region)}; distribution={latest_n(root)}']
    for node in nodes:
        event_path=root/f'metrics{node}'/'replica_backfill_events.csv'
        rows=[]
        if event_path.exists():
            try:
                with event_path.open(newline='') as fh:
                    rows=[r for r in csv.DictReader(fh)
                          if r.get('region')==str(region)]
            except (OSError, csv.Error) as exc:
                lines.append(f'node {node}: metrics parse error: {exc}')
        log=log_text(root/f'node{node}.log')
        related=[line.strip() for line in log.splitlines()
                 if '[backfill]' in line and f'region={region}' in line]
        policy=[line.strip() for line in log.splitlines()
                if '[policy-apply]' in line]
        lines.append(f'node {node}: event_file={event_path.exists()} '
                     f'region_events={len(rows)} backfill_logs={len(related)} '
                     f'last_policy={policy[-1] if policy else "NONE"}')
        for row in rows[-3:]:
            lines.append(f'  event result={row.get("result")} reason={row.get("reason")} '
                         f'gen={row.get("placement_gen")} epoch={row.get("policy_epoch")}')
        for line in related[-2:]: lines.append(f'  log {line[-260:]}')
    return '\n'.join(lines)


def wait_for_verified_backfill(root, nodes, region, timeout):
    """No false PASS: require reported full backfill on BOTH new holders."""
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if all(completed_backfill(root,n,region) for n in nodes):
            return
        time.sleep(.25)
    raise AssertionError(
        f'Backfill readiness was NOT proven within {timeout}s for region {region}.\n'
        +backfill_diagnostics(root,nodes,region)
        +'\nThis is an inconclusive fault test, not proof that Strategy C failed. '
        'Inspect node logs and completed/partial backfill classifications before changing C.')


def ws_put(port,x,y,color,timeout=3):
    """One isolated WS per request to prevent stale broadcasts from false ACKs.

    P2Place live `pixel` has no request ID; ACK is attributed only if exact
    coordinates AND color match. An error or timeout is a failed PUT.
    """
    ws=WebSocketClient('127.0.0.1',port)
    try:
        ws.connect(); deadline=time.monotonic()+timeout
        ws.sock.settimeout(min(timeout,3))
        ws.send_frame(0x1,json.dumps({'op':'put','x':x,'y':y,'color':color}))
        while time.monotonic()<deadline:
            try:
                ws.sock.settimeout(max(.1,deadline-time.monotonic()))
                op, raw=ws.recv_frame()
            except (OSError,ConnectionError,socket.timeout):return False
            if op==0x9:ws.send_frame(0xA,raw);continue
            if op==0x8:return False
            if op!=0x1:continue
            msg=json.loads(raw)
            if msg.get('op')=='error':return False
            if (msg.get('op')=='pixel' and int(msg['x'])==x and int(msg['y'])==y
                and str(msg['color']).upper()==color):return True
        return False
    finally:
        ws.close()


def ws_snapshot(port,timeout=20):
    ws=WebSocketClient('127.0.0.1',port)
    try:
        ws.connect(); ws.sock.settimeout(timeout)
        ws.send_frame(0x1,json.dumps({'op':'get_canvas'})); found={}; begun=False
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            ws.sock.settimeout(max(.1,deadline-time.monotonic()))
            op, raw=ws.recv_frame()
            if op==0x9: ws.send_frame(0xA,raw);continue
            if op==0x8:raise RuntimeError('socket closed mid snapshot')
            if op!=0x1:continue
            msg=json.loads(raw)
            if msg.get('op')=='error':raise RuntimeError(str(msg))
            if msg.get('op')=='canvas_begin': begun=True; found={}
            elif msg.get('op')=='canvas_chunk':
                assert begun,'chunk without begin'
                for px in msg.get('pixels',[]): found[(int(px['x']),int(px['y']))]=str(px['color']).upper()
            elif msg.get('op')=='canvas_end':
                assert begun,'end without begin';return found
        raise TimeoutError('snapshot timed out')
    finally:ws.close()


class Cluster:
    def __init__(self,binary,root,nodes,strategy):
        self.binary=binary;self.root=root;self.count=nodes;self.strategy=strategy
        self.procs={};self.logs={};self.tcp={};self.ws={}
        root.mkdir(parents=True,exist_ok=True)
        # Never let a repeated blind run silently resume a previous test cluster.
        stale = sorted(root.glob('node*.state')) + sorted(root.glob('node*.state.pending'))
        if stale:
            raise RuntimeError('Refusing to reuse persistent test placement state in '
                               f'{root}: {stale[:3]}. Use a fresh --artifacts run directory.')

    def start(self,node):
        tcp,ws=free_port(),free_port();self.tcp[node]=tcp;self.ws[node]=ws
        args=[str(self.binary),'--id',str(node),'--port',str(tcp),'--ws-port',str(ws),
            '--state-file',str(self.root/f'node{node}.state'),'--replicas','3',
            '--read-quorum','2','--write-quorum','2','--strategy',self.strategy,
            '--min-replicas','2','--max-replicas','4',
            '--reliability-window','120','--policy-interval','2','--policy-cooldown','3']
        if self.strategy=='C':
            args += ['--write-window','60','--write-report-interval','2']
        args += ['--metrics-dir',str(self.root/f'metrics{node}')]
        if node>1: args += ['--peers',f'1:127.0.0.1:{self.tcp[1]}']
        f=(self.root/f'node{node}.log').open('w');self.logs[node]=f
        if shutil.which('stdbuf'):args=['stdbuf','-oL','-eL']+args
        self.procs[node]=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=f,stderr=subprocess.STDOUT)

    def boot(self):
        self.start(1)
        until(lambda: placement(self.root/'node1.state'),'bootstrap',40)
        # Admission is sequential to avoid overlapping formation and handoffs.
        for node in range(2,self.count+1):
            self.start(node)
            until(lambda n=node: all((p:=placement(self.root/f'node{i}.state')) and len(p[1])>=n
                     for i in range(1,n+1)),f'admit node {node}',90)
        expected=tuple(range(1,self.count+1))
        until(lambda: all((p:=placement(self.root/f'node{i}.state')) and p[1]==expected
                  for i in range(1,self.count+1)), 'fully committed placement',90)
        until(lambda: latest_n(self.root)==(256,0,0), 'healthy N=2 baseline',90)
        time.sleep(3)  # allow full-mesh peer connections to settle after admission
        return placement(self.root/'node1.state')

    def stop(self):
        for p in self.procs.values():
            if p.poll() is None:p.terminate()
        for p in self.procs.values():
            try:p.wait(timeout=3)
            except subprocess.TimeoutExpired:p.kill();p.wait(timeout=3)
        for f in self.logs.values():f.close()

    def kill_pair(self,pair):
        # Signal both before waiting; avoids accidental serial outages.
        for node in pair:self.procs[node].send_signal(signal.SIGKILL)
        stamp=time.monotonic()
        for node in pair:self.procs[node].wait(timeout=3)
        return stamp


def choose_region(place,seed,exclude=(1,)):
    rng=random.Random(seed)
    valid=[r for r in range(256) if all(n not in exclude for n in preference(place,r,2))]
    assert valid,'no regions with two failure targets away from gateway'
    r=rng.choice(valid)
    return r,preference(place,r,4)


def heat(cluster,region,seed,n=90):
    """Make a hot region using *only valid client writes* and check each ACK."""
    accepted=[]
    for i in range(n):
        x,y=region_pixel(region,i);c=color_for(seed,i)
        good=ws_put(cluster.ws[1],x,y,c,timeout=4)
        if not good:raise AssertionError(f'heating PUT failed at index={i}, pixel=({x},{y})')
        accepted.append((x,y,c))
    return accepted


def run_cold(args,output):
    c=Cluster(args.binary,output,args.nodes,'C')
    try:
        place=c.boot();region,_=choose_region(place,args.seed)
        x,y=region_pixel(region,0)
        assert ws_put(c.ws[1],x,y,color_for(args.seed,0),4)
        time.sleep(8)
        assert latest_n(output)==(256,0,0),f'cold C unexpectedly scaled up: {latest_n(output)}'
        assert current_region_n(output,region)==2,'cold region target is not N2'
        print(f'PASS cold: C kept 256 regions at N2 after one write, region={region}',flush=True)
        return {'result':'PASS','region':region,'n_distribution':latest_n(output)}
    finally:c.stop()


def run_hot(args,output):
    c=Cluster(args.binary,output,args.nodes,'C')
    try:
        place=c.boot();region,_=choose_region(place,args.seed+17)
        heat(c,region,args.seed,n=args.heat_writes)
        until(lambda:current_region_n(output,region)==4,f'C region {region} N4 accepted',40)
        assert latest_n(output)[2]>=1,'N4 distribution absent after hot input'
        print(f'PASS hot: C raised region {region} from N2 to N4',flush=True)
        return {'result':'PASS','region':region,'n_distribution':latest_n(output)}
    finally:c.stop()


def trial_fault(args,output,strategy,selected=None):
    c=Cluster(args.binary,output,args.nodes,strategy)
    try:
        place=c.boot()
        region,pref=choose_region(place,args.seed+113)
        if selected is not None and region!=selected:raise AssertionError('B/C placement mismatch')
        assert 1 not in pref[:2], 'the gateway must stay alive'
        accepted=heat(c,region,args.seed,n=args.heat_writes)
        if strategy=='C':
            until(lambda:current_region_n(output,region)==4,'C hot region actually N4',45)
        else:
            # Check B stays N2 while the same write rate is applied.
            time.sleep(5)
            assert current_region_n(output,region)==2,'B unexpectedly N>2 before faults'
        assert placement(output/'node1.state')[0]==place[0],'unexpected pre-fault membership change'
        # Do not confuse a target N4 with synchronized copies: test for
        # completed backfill on at least the two additional preferred targets.
        extra=pref[2:4]
        if strategy=='C':
            wait_for_verified_backfill(output,extra,region,args.backfill_timeout)
        # Confirm the hot region is STILL at N4 at the exact kill instant.
        if strategy=='C':assert current_region_n(output,region)==4,'C target decayed before fault'
        else:assert current_region_n(output,region)==2,'B target changed before fault'
        pair=pref[:2];baseline=placement(output/'node1.state')[0]
        c.kill_pair(pair)
        results=[]
        # Keep probes within short pre-removal window (default auto-removal
        # 15s + stabilization). Separate WS clients isolate stale messages.
        start=time.monotonic()
        for i in range(args.probes):
            x,y=region_pixel(region,160+i);col=color_for(args.seed+713,i)
            ok=ws_put(c.ws[1],x,y,col,timeout=2.5)
            results.append({'x':x,'y':y,'color':col,'acked':ok,'elapsed_s':round(time.monotonic()-start,2)})
        after=placement(output/'node1.state')[0]
        assert baseline==after, f'INCONCLUSIVE: placement reconfigured during probes {baseline}->{after}'
        assert time.monotonic()-start < 15,'INCONCLUSIVE: failure probe exceeded detection window'
        acked=sum(x['acked'] for x in results)
        # Black-box read verification after faults (do not assume an ACK
        # automatically implies same-value GET when R+W<=N).
        observed={}
        if acked:
            try:
                canvas=ws_snapshot(c.ws[1],timeout=12)
                observed={f"{p['x']}:{p['y']}":canvas.get((p['x'],p['y'])) for p in results if p['acked']}
            except Exception as ex:observed={'snapshot_error':str(ex)}
        print(f'{strategy} fault: region={region} pair={pair} pref={pref} ACK={acked}/{len(results)}',flush=True)
        return {'result':'PASS','strategy':strategy,'region':region,'pref':pref,'killed':pair,
                'pre_fault_n':4 if strategy=='C' else 2,'acked':acked,'total':len(results),
                'probe_results':results,'snapshot_observed':observed}
    finally:c.stop()


def run_fault(args,output):
    # Different cluster process instances, same seed, same committed member
    # IDs, same region, same workload and failure targets.
    b=trial_fault(args,output/'B','B')
    c=trial_fault(args,output/'C','C',selected=b['region'])
    assert b['pref']==c['pref'], 'B and C had different owner preference lists'
    assert b['acked']==0, f'B control had unexpected successful PUTs: {b["acked"]}'
    assert c['acked']>=args.probes-1, f'C failed to preserve W2 availability: {c["acked"]}/{args.probes}'
    print(f'PASS blind-fault: B {b["acked"]}/{args.probes}; C {c["acked"]}/{args.probes}',flush=True)
    return {'result':'PASS','B':b,'C':c}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary',type=pathlib.Path,default=pathlib.Path('build/bin/p2place-peer'))
    p.add_argument('--artifacts',type=pathlib.Path,default=pathlib.Path('experiments/results/strategy_c_blind'))
    p.add_argument('--seed',type=int,default=38731)
    p.add_argument('--nodes',type=int,default=6)
    p.add_argument('--heat-writes',type=int,default=90)
    p.add_argument('--probes',type=int,default=4)
    p.add_argument('--backfill-timeout',type=float,default=45,help='Max seconds to verify full new-replica backfill; no inferred success')
    p.add_argument('--cases',nargs='+',choices=['cold','hot','fault'],default=['cold','hot','fault'])
    args=p.parse_args();args.binary=args.binary.resolve();args.artifacts=args.artifacts.resolve()
    if not args.binary.is_file():p.error(f'executable not found: {args.binary} (build project first)')
    if args.nodes<5 or args.nodes>10:p.error('--nodes must be 5..10')
    if not 65<=args.heat_writes<=150:p.error('--heat-writes must be 65..150 for the 60s rate window')
    if not 1<=args.probes<=8:p.error('--probes must be 1..8')
    if not 1<=args.backfill_timeout<=120:p.error('--backfill-timeout must be 1..120 seconds')
    # Every invocation gets an isolated directory. Preserve all prior runs,
    # especially their raw diagnostics; never delete a user's experiment files.
    args.artifacts.mkdir(parents=True,exist_ok=True)
    base_artifacts=args.artifacts
    run_label=datetime.datetime.now(datetime.timezone.utc).strftime('run_%Y%m%dT%H%M%SZ')
    candidate=base_artifacts/run_label
    suffix=1
    while candidate.exists():
        candidate=base_artifacts/f'{run_label}_{suffix}'
        suffix+=1
    candidate.mkdir(parents=True,exist_ok=False)
    args.artifacts=candidate
    print(f'Fresh isolated artifacts: {args.artifacts}',flush=True)
    outcomes={};failed=0
    for case in args.cases:
        print(f'\n== Blind test {case}, seed={args.seed} ==',flush=True)
        out=args.artifacts/case;out.mkdir(parents=True,exist_ok=True)
        try:
            outcome={'cold':run_cold,'hot':run_hot,'fault':run_fault}[case](args,out)
        except Exception as exc:
            failed+=1;traceback.print_exc()
            outcome={'result':'FAIL','error':repr(exc)}
        outcomes[case]=outcome
        (args.artifacts/'blind_results.json').write_text(json.dumps(outcomes,indent=2))
    print('\nRESULTS:',json.dumps({k:v['result'] for k,v in outcomes.items()}),flush=True)
    print('Logs and output:',args.artifacts,flush=True)
    return 1 if failed else 0


if __name__=='__main__':sys.exit(main())
