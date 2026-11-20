#!/usr/bin/env python3
"""Independent black-box P2Place Strategy C tests.

Requires only Python stdlib, build/bin/p2place-peer and the existing
`test_websocket_manual.py` WebSocket framing helper. No imports of P2Place
Strategy C test harness or policy calculation code.

Launches isolated peers on randomly allocated ports; never touches an
existing cluster. Results/logs retained in --artifacts.

Fault-trial readiness contract (fixes the run_20261117 blind failure):
a BACKGROUND heater keeps the selected region's real client-PUT rate above
C's high threshold from warm-up through the fault phase; C is only eligible
for the fault when target N=4 is sustained (2s dwell), every added target
has applied the same committed generation/epoch and has a genuine completed
(not partial) backfill, and a client read confirms known heater pixels.
A drop below N4 while the heater is active fails fast with
POLICY_DECAY_BEFORE_READY. `idle` (legitimate decay) and `negative` (the
readiness guard refuses to certify without evidence) are separate cases.
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
import threading
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
    return region_n_epoch(root,region)[0]


def region_n_epoch(root,region):
    """(target N, policy epoch) of the region's latest accepted policy event."""
    path=root/'metrics1'/'region_replication_events.csv'
    if not path.exists():return None,None
    current=None;epoch=None
    with path.open(newline='') as fh:
        for row in csv.DictReader(fh):
            try:
                if row['event_type'] in ('initial','n_change') and int(row['region'])==region:
                    current=int(row['new_n']);epoch=int(row['policy_epoch'])
            except (ValueError,KeyError,TypeError):continue
    return current,epoch


def last_policy_apply(root,node):
    """(placement_gen, policy_epoch) of a member's last applied snapshot.

    The authority applies its own table without a POLICY_SNAPSHOT round-trip,
    so it has no [policy-apply] line; its [policy-broadcast] line carries the
    same accepted gen/epoch and is equivalent black-box evidence."""
    text=log_text(root/f'node{node}.log')
    matches=re.findall(r'\[policy-apply\] node=(\d+) gen=(\d+) epoch=(\d+)',text)
    if not matches:
        matches=re.findall(r'\[policy-broadcast\] node=(\d+) gen=(\d+) epoch=(\d+)',text)
    if not matches:return None
    _,gen,epoch=matches[-1]
    return int(gen),int(epoch)


def region_events(root,region):
    """Accepted policy events for the region in order: [(new_n, epoch)]."""
    path=root/'metrics1'/'region_replication_events.csv'
    out=[]
    if not path.exists():return out
    with path.open(newline='') as fh:
        for row in csv.DictReader(fh):
            try:
                if row['event_type'] in ('initial','n_change') and int(row['region'])==region:
                    out.append((int(row['new_n']),int(row['policy_epoch'])))
            except (ValueError,KeyError,TypeError):continue
    return out


def join_epoch(events,position):
    """Epoch at which a member at preference `position` became a target
    (the first accepted event whose N exceeds its position), or None."""
    for new_n,epoch in events:
        if new_n>position:return epoch
    return None


def completed_backfill(root, node, region, gen=None, min_epoch=None):
    """Genuine completed backfill for the region, optionally restricted to the
    CURRENT committed generation and at-or-past a policy epoch. Rows from an
    earlier placement/policy (e.g. the boot-time initial sync) must never
    satisfy readiness for the current activation."""
    path=root/f'metrics{node}'/'replica_backfill_events.csv'
    if not path.exists():return False
    with path.open(newline='') as fh:
        for r in csv.DictReader(fh):
            try:
                if int(r['region'])!=region:continue
                if r['result'].lower() not in ('ok','true','1','success'):continue
                reason=r.get('reason','')
                if 'classification=complete_partial' in reason or 'empty_partial_accepted' in reason:continue
                if gen is not None and int(r.get('placement_gen') or 0)!=gen:continue
                if min_epoch is not None and int(r.get('policy_epoch') or 0)<min_epoch:continue
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


def wait_for_verified_backfill(root, nodes, region, timeout, gen=None, min_epoch=None):
    """No false PASS: require reported full backfill on BOTH new holders,
    bound to the current generation and the activation epoch."""
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if all(completed_backfill(root,n,region,gen=gen,min_epoch=min_epoch)
               for n in nodes):
            return
        time.sleep(.25)
    raise AssertionError(
        f'Backfill readiness was NOT proven within {timeout}s for region {region}.\n'
        +backfill_diagnostics(root,nodes,region)
        +'\nThis is an inconclusive fault test, not proof that Strategy C failed. '
        'Inspect node logs and completed/partial backfill classifications before changing C.')


class Heater:
    """Background hot-region writer: keeps the region's cluster-wide PUT rate
    above N_write=4's high threshold from policy warm-up through the fault
    phase. Every write is a valid, distinct client PUT through a living
    gateway using the real WebSocket protocol; ACKs are verified exactly."""

    def __init__(self,ws_port,region,seed,rate=3.0):
        self.ws_port=ws_port;self.region=region;self.seed=seed
        self.interval=1.0/rate;self.stop=threading.Event()
        self.thread=None;self.acks=0;self.fails=0;self.i=0
        self.lock=threading.Lock();self.last={}  # index -> (x, y, color)

    def _run(self):
        nxt=time.monotonic()
        while not self.stop.is_set():
            i=self.i;self.i+=1
            idx=i%128  # keep clear of the probe indexes (160+)
            x,y=region_pixel(self.region,idx)
            c=color_for(self.seed+2000,i)
            ok=ws_put(self.ws_port,x,y,c,timeout=2.5)
            if ok:
                self.acks+=1
                with self.lock:self.last[idx]=(x,y,c)
            else:
                self.fails+=1
            nxt+=self.interval
            delay=nxt-time.monotonic()
            if delay>0:self.stop.wait(delay)
            else:nxt=time.monotonic()

    def start(self):
        self.thread=threading.Thread(target=self._run,daemon=True)
        self.thread.start()

    def halt(self):
        self.stop.set()
        if self.thread:self.thread.join(timeout=6)

    def sample(self,n=12):
        with self.lock:items=list(self.last.values())
        return items[-n:]


def wait_for_activation_ready(root,region,extra,timeout,heater,snapshot_port,
                              pref=None):
    """Wait for a REAL N4 activation: target N=4 sustained, every additional
    target has applied the committed generation at or past THE EPOCH IT
    JOINED the target set, has a GENUINE completed backfill (partial/unknown
    classifications never count) for that epoch or later, and a client read
    confirms known written pixel values. Requires a 2s dwell.
    POLICY_DECAY_BEFORE_READY fails fast."""
    end=time.monotonic()+timeout
    seen4=False
    ready_since=None
    last_reasons=['not evaluated yet']
    while time.monotonic()<end:
        n,epoch=region_n_epoch(root,region)
        if n==4:seen4=True
        if seen4 and n is not None and n<4:
            raise AssertionError(
                f'POLICY_DECAY_BEFORE_READY: region {region} fell from N4 to N{n} '
                f'(epoch {epoch}) before readiness while the heater was active. '
                'This is a policy/rate problem, not a backfill timeout.')
        ok=False;reasons=[]
        if n==4:
            ok=True
            gen=placement(root/'node1.state')[0]
            events=region_events(root,region)
            for node in extra:
                position=pref.index(node) if pref else 3
                need=join_epoch(events,position)
                if need is None:
                    ok=False;reasons.append(f'node {node}: no event makes it a target')
                    continue
                apply=last_policy_apply(root,node)
                if apply is None or apply[0]!=gen or apply[1]<need:
                    ok=False;reasons.append(f'node {node}: not at gen/epoch '
                                            f'(last_apply={apply}, joined_epoch={need})')
                    continue
                if not completed_backfill(root,node,region,gen=gen,min_epoch=need):
                    ok=False;reasons.append(f'node {node}: no genuine completed backfill '
                                            f'for gen={gen} epoch>={need}')
        else:
            reasons.append(f'current N={n}')
        if ok:
            if ready_since is None:ready_since=time.monotonic()
            if time.monotonic()-ready_since>=2.0:
                # Independent client read: known heater pixels must be
                # visible through the gateway before the fault is allowed.
                sample=heater.sample(12)
                try:
                    canvas=ws_snapshot(snapshot_port,timeout=12)
                except Exception as exc:  # noqa: BLE001
                    canvas={};reasons.append(f'snapshot error: {exc}')
                if sample:
                    missing=[(x,y,c,canvas.get((x,y))) for x,y,c in sample
                             if canvas.get((x,y))!=c]
                    if missing:
                        ok=False
                        reasons.append(f'read-back missing {len(missing)}/{len(sample)} '
                                       f'heater pixels, first={missing[0]}')
                if ok:
                    return epoch
                ready_since=None
        else:
            ready_since=None
        last_reasons=reasons
        time.sleep(.25)
    raise AssertionError(
        f'Backfill readiness was NOT proven within {timeout}s for region {region}.\n'
        f'last reasons: {"; ".join(last_reasons)}\n'
        +backfill_diagnostics(root,extra,region)
        +'\nThis is an inconclusive fault test, not proof that Strategy C failed.')


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

    def restart(self,node):
        """Rejoin a killed member on the SAME ports/state (append the log so
        the pre-fault evidence is preserved)."""
        args=[str(self.binary),'--id',str(node),'--port',str(self.tcp[node]),
              '--ws-port',str(self.ws[node]),
              '--state-file',str(self.root/f'node{node}.state'),'--replicas','3',
              '--read-quorum','2','--write-quorum','2','--strategy',self.strategy,
              '--min-replicas','2','--max-replicas','4',
              '--reliability-window','120','--policy-interval','2','--policy-cooldown','3']
        if self.strategy=='C':
            args += ['--write-window','60','--write-report-interval','2']
        args += ['--metrics-dir',str(self.root/f'metrics{node}restart')]
        (self.root/f'metrics{node}restart').mkdir(parents=True,exist_ok=True)
        args += ['--peers',f'1:127.0.0.1:{self.tcp[1]}']
        f=(self.root/f'node{node}.log').open('a');self.logs[node]=f
        if shutil.which('stdbuf'):args=['stdbuf','-oL','-eL']+args
        self.procs[node]=subprocess.Popen(args,stdin=subprocess.DEVNULL,
                                          stdout=f,stderr=subprocess.STDOUT)


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


def trial_fault(args,output,strategy,selected=None,matched_heat=None):
    c=Cluster(args.binary,output,args.nodes,strategy)
    try:
        place=c.boot()
        region,pref=choose_region(place,args.seed+113)
        if selected is not None and region!=selected:raise AssertionError('B/C placement mismatch')
        assert 1 not in pref[:2], 'the gateway must stay alive'
        extra=pref[2:4]
        t_start=time.monotonic()
        # A valid client PUT stream keeps the write window hot through policy
        # warm-up, readiness, and the fault phase (never stop-and-wait).
        heater=Heater(c.ws[1],region,args.seed,rate=args.heater_rate)
        heater.start()
        n4_epoch=None
        try:
            if strategy=='C':
                n4_epoch=wait_for_activation_ready(output,region,extra,
                    args.backfill_timeout,heater,c.ws[1],pref=pref)
            else:
                if matched_heat is not None:
                    # Same heater duration as the C trial's heat-to-fault
                    # interval (recorded, not silently shortened).
                    time.sleep(max(0.0,matched_heat-(time.monotonic()-t_start)))
                else:
                    time.sleep(args.heat_seconds)
                assert current_region_n(output,region)==2,'B unexpectedly N>2 before faults'
            heat_duration=time.monotonic()-t_start
            pre_n,pre_epoch=region_n_epoch(output,region)
            if strategy=='C':
                assert pre_n==4,f'C target decayed before fault (N={pre_n})'
            else:
                assert pre_n==2,f'B target changed before fault (N={pre_n})'
            pair=pref[:2];baseline=placement(output/'node1.state')[0]
            c.kill_pair(pair)
            results=[]
            # Keep probes within the short pre-removal window (default
            # auto-removal 15s + stabilization). Separated WS clients isolate
            # stale messages. The heater stays active through the probes.
            start=time.monotonic()
            for i in range(args.probes):
                x,y=region_pixel(region,160+i);col=color_for(args.seed+713,i)
                ok=ws_put(c.ws[1],x,y,col,timeout=2.5)
                results.append({'x':x,'y':y,'color':col,'acked':ok,
                                'elapsed_s':round(time.monotonic()-start,2)})
            after=placement(output/'node1.state')[0]
            heater.halt()
            assert baseline==after, f'INCONCLUSIVE: placement reconfigured during probes {baseline}->{after}'
            assert time.monotonic()-start < 15,'INCONCLUSIVE: failure probe exceeded detection window'
            acked=sum(x['acked'] for x in results)
            # Acknowledged-write survival: rejoin the killed pair (the two
            # extras keep holding the acknowledged pixels) and verify their
            # values through a client read. The pair must be restarted before
            # the snapshot: a full-canvas read aborts on OTHER regions whose
            # two owners were the same pair and are still down.
            observed={}
            if acked:
                for owner in pair:c.restart(owner)
                # Bounded convergence poll. A single post-rejoin read can
                # merge the two RESTARTED (empty) replicas: R=2 chooses the
                # first two responders, and the surviving extras hold the
                # acknowledged values. The read contract here is eventual
                # (R+W<=N), so poll until the values are readable or the
                # bound expires — never declare loss from one instant, and
                # record how long convergence took.
                t0=time.monotonic()
                deadline=t0+30
                missing=[]
                last_error=None
                while True:
                    try:
                        canvas=ws_snapshot(c.ws[1],timeout=15)
                        observed={}
                        missing=[]
                        for p in results:
                            if not p['acked']:continue
                            got=canvas.get((p['x'],p['y']))
                            observed[f"{p['x']}:{p['y']}"]=got
                            if got!=p['color']:
                                missing.append((p['x'],p['y'],p['color'],got))
                        last_error=None
                        if not missing:break
                    except Exception as ex:  # transient rejoin/snapshot errors
                        last_error=str(ex)
                    if time.monotonic()>=deadline:break
                    time.sleep(2)
                if missing:
                    raise AssertionError(
                        f'acknowledged pixels not readable within 30s of rejoin: '
                        f'{missing} last_error={last_error}')
                observed['readback_s']=round(time.monotonic()-t0,1)
            print(f'{strategy} fault: region={region} pair={pair} pref={pref} '
                  f'ACK={acked}/{len(results)} heat={heat_duration:.1f}s '
                  f'heater_acks={heater.acks} heater_fails={heater.fails} '
                  f'pre_n={pre_n}',flush=True)
            return {'result':'PASS','strategy':strategy,'region':region,'pref':list(pref),
                    'killed':list(pair),'pre_fault_n':pre_n,
                    'pre_fault_epoch':pre_epoch,'n4_epoch':n4_epoch,
                    'heat_s':round(heat_duration,2),
                    'heater':{'acks':heater.acks,'fails':heater.fails,
                              'rate':args.heater_rate},
                    'acked':acked,'total':len(results),
                    'probe_results':results,'snapshot_observed':observed}
        finally:
            heater.halt()
    finally:c.stop()


def run_fault(args,output):
    # Different cluster process instances, same seed, same committed member
    # IDs, same region, same heater rate/duration, same failure targets.
    # C runs FIRST so its heat-to-fault duration can be replayed for B.
    c=trial_fault(args,output/'C','C')
    b=trial_fault(args,output/'B','B',selected=c['region'],matched_heat=c['heat_s'])
    assert b['pref']==c['pref'], 'B and C had different owner preference lists'
    assert b['acked']==0, f'B control had unexpected successful PUTs: {b["acked"]}'
    assert c['acked']>=args.probes-1, f'C failed to preserve W2 availability: {c["acked"]}/{args.probes}'
    print(f'PASS blind-fault: B {b["acked"]}/{args.probes}; C {c["acked"]}/{args.probes} '
          f'(region={c["region"]}, killed {c["killed"]})',flush=True)
    return {'result':'PASS','C':c,'B':b}


def run_idle(args,output):
    """Idle/decay: with writes stopped, N must fall back to N2 (a legitimate
    policy decay, not a backfill timeout — kept as a separate case)."""
    c=Cluster(args.binary,output,args.nodes,'C')
    try:
        place=c.boot();region,_=choose_region(place,args.seed+29)
        heater=Heater(c.ws[1],region,args.seed,rate=args.heater_rate)
        heater.start()
        try:
            until(lambda:current_region_n(output,region)==4,
                  f'region {region} N4 under heater',90)
        finally:
            heater.halt()
        t0=time.monotonic()
        until(lambda:current_region_n(output,region)==2,
              f'region {region} policy decay to N2 after writes stop',150)
        decay_s=time.monotonic()-t0
        print(f'PASS idle: region {region} decayed N4->N2 in {decay_s:.1f}s after the '
              'heater stopped (write-window expiry, cooldown respected)',flush=True)
        return {'result':'PASS','region':region,'decay_s':round(decay_s,2)}
    finally:c.stop()


def run_negative(args,output):
    """Negative control: the harness itself must NOT certify C protection
    without proof. Immediately after N4 (before any urgent backfill can
    complete), the readiness check with a tiny timeout must refuse to pass."""
    c=Cluster(args.binary,output,args.nodes,'C')
    try:
        place=c.boot();region,pref=choose_region(place,args.seed+41)
        heater=Heater(c.ws[1],region,args.seed,rate=args.heater_rate)
        heater.start()
        try:
            until(lambda:current_region_n(output,region)==4,
                  f'region {region} N4',90)
            gen=placement(output/'node1.state')[0]
            _,epoch=region_n_epoch(output,region)
            try:
                wait_for_verified_backfill(output,pref[2:4],region,timeout=0.05,
                                           gen=gen,min_epoch=epoch)
            except AssertionError as exc:
                if 'NEGATIVE CONTROL FAILED' in str(exc):raise
                print('PASS negative: readiness guard refused to certify without '
                      'completed backfill (no false C-protection claim)',flush=True)
                return {'result':'PASS','region':region,
                        'guard_refused':str(exc)[:200]}
            raise AssertionError('NEGATIVE CONTROL FAILED: readiness was certified '
                                 'without completed backfill evidence')
        finally:
            heater.halt()
    finally:c.stop()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary',type=pathlib.Path,default=pathlib.Path('build/bin/p2place-peer'))
    p.add_argument('--artifacts',type=pathlib.Path,default=pathlib.Path('experiments/results/strategy_c_blind'))
    p.add_argument('--seed',type=int,default=38731)
    p.add_argument('--nodes',type=int,default=6)
    p.add_argument('--heat-writes',type=int,default=90)
    p.add_argument('--heater-rate',type=float,default=3.0,
                   help='Background hot-region PUT attempts/sec during the fault trial '
                        '(must exceed the Strategy C high write threshold of 1.0/s)')
    p.add_argument('--heat-seconds',type=float,default=25.0,
                   help='B control: heater duration when no matched C duration is available')
    p.add_argument('--probes',type=int,default=4)
    p.add_argument('--backfill-timeout',type=float,default=75,help='Max seconds to verify N4 activation + full new-replica backfill; no inferred success')
    p.add_argument('--cases',nargs='+',choices=['cold','hot','fault','idle','negative'],default=['cold','hot','fault'])
    args=p.parse_args();args.binary=args.binary.resolve();args.artifacts=args.artifacts.resolve()
    if not args.binary.is_file():p.error(f'executable not found: {args.binary} (build project first)')
    if args.nodes<5 or args.nodes>10:p.error('--nodes must be 5..10')
    if not 65<=args.heat_writes<=150:p.error('--heat-writes must be 65..150 for the 60s rate window')
    if not 1.0<args.heater_rate<=8.0:p.error('--heater-rate must be in (1.0, 8.0] (60s window high threshold is 1.0/s)')
    if not 1<=args.probes<=8:p.error('--probes must be 1..8')
    if not 1<=args.backfill_timeout<=180:p.error('--backfill-timeout must be 1..180 seconds')
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
            outcome={'cold':run_cold,'hot':run_hot,'fault':run_fault,
                     'idle':run_idle,'negative':run_negative}[case](args,out)
        except Exception as exc:
            failed+=1;traceback.print_exc()
            outcome={'result':'FAIL','error':repr(exc)}
        outcomes[case]=outcome
        (args.artifacts/'blind_results.json').write_text(json.dumps(outcomes,indent=2))
    print('\nRESULTS:',json.dumps({k:v['result'] for k,v in outcomes.items()}),flush=True)
    print('Logs and output:',args.artifacts,flush=True)
    return 1 if failed else 0


if __name__=='__main__':sys.exit(main())
