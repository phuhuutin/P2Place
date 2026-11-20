#!/usr/bin/env python3
"""Independent P2Place Strategy C live adversarial test.

Uses only the public p2place-peer CLI, persisted placement metadata, per-node
readiness telemetry, and real browser WebSocket PUT/GET. Does NOT import any
P2Place policy or previous blind test module.

WARNING: it launches and SIGKILLs ITS OWN isolated nodes, never existing PIDs.
The program under test is not modified. Latest urgent-backfill readiness events
are preferred; old/incomplete metrics fail closed (INCONCLUSIVE, not PASS).
"""
import argparse, csv, datetime, json, pathlib, random, re, shutil, signal
import socket, subprocess, sys, threading, time, traceback
from test_websocket_manual import WebSocketClient

POLICY = re.compile(r'\[policy-broadcast\].*?\bn2=(\d+)\s+n3=(\d+)\s+n4=(\d+)')


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0));return s.getsockname()[1]


def wait_for(predicate, timeout, reason):
    deadline=time.monotonic()+timeout; last=None
    while time.monotonic()<deadline:
        try:
            r=predicate()
            if r: return r
        except (OSError, ValueError, KeyError, IndexError, TypeError) as e:last=str(e)
        time.sleep(.15)
    raise RuntimeError(f'TIMEOUT: {reason}; last_error={last}')


def placement(path):
    if not path.exists():return None
    x=path.read_text().split()
    if len(x)<6 or x[0] not in ('P2PLACE_BALANCED_V1','P2PLACE_BALANCED_V2'):return None
    gen,n=int(x[1]),int(x[2]); members=tuple(map(int,x[3:3+n]));offset=3+n*(2 if x[0].endswith('V2') else 1)
    owners=tuple(map(int,x[offset:offset+256]));
    if len(owners)!=256 or any(owner not in members for owner in owners):return None
    return gen,members,owners


def pref(state,region):
    _,members,owners=state;i=members.index(owners[region]);return tuple(members[(i+j)%len(members)] for j in range(min(4,len(members))))


def pixel(region,i):return (region%16)*16+i%16,(region//16)*16+i//16

def color(seed,i):return '#'+format((seed*737+7919*i+0xC0FFEE)%0xFFFFFF,'06X')

def rows(path):
    if not path.exists():return []
    with path.open(newline='') as f:return list(csv.DictReader(f))


def accepted_table(root,region):
    relevant=[]
    for r in rows(root/'metrics1'/'region_replication_events.csv'):
        try:
            if int(r['region'])==region and r['event_type'] in ('initial','n_change'):
                relevant.append((int(r['new_n']),int(r['placement_gen']),int(r['policy_epoch']),int(r.get('monotonic_ns') or 0)))
        except (KeyError,ValueError,TypeError):pass
    return relevant[-1] if relevant else None


def n_counts(root):
    path=root/'node1.log'
    if not path.exists():return None
    matches=POLICY.findall(path.read_text(errors='replace'))
    return tuple(map(int,matches[-1])) if matches else None


def client_put(port,x,y,c,timeout=3):
    ws=WebSocketClient('127.0.0.1',port)
    try:
        ws.connect();ws.send_frame(1,json.dumps({'op':'put','x':x,'y':y,'color':c}));deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            try:
                ws.sock.settimeout(max(.1,deadline-time.monotonic()));op,data=ws.recv_frame()
            except (TimeoutError,OSError,ConnectionError):return False
            if op==9:ws.send_frame(10,data);continue
            if op==8:return False
            if op!=1:continue
            try:m=json.loads(data)
            except ValueError:continue
            if m.get('op')=='error':return False
            if m.get('op')=='pixel' and m.get('x')==x and m.get('y')==y and str(m.get('color','')).upper()==c:return True
        return False
    finally:ws.close()


def client_canvas(port,timeout=18):
    ws=WebSocketClient('127.0.0.1',port)
    try:
        ws.connect();ws.send_frame(1,json.dumps({'op':'get_canvas'}));deadline=time.monotonic()+timeout;seen={};started=False
        while time.monotonic()<deadline:
            ws.sock.settimeout(max(.1,deadline-time.monotonic()));op,data=ws.recv_frame()
            if op==9:ws.send_frame(10,data);continue
            if op==8:raise RuntimeError('socket closed')
            if op!=1:continue
            m=json.loads(data)
            if m.get('op')=='error':raise RuntimeError('GET error '+str(m))
            if m.get('op')=='canvas_begin':seen={};started=True
            elif m.get('op')=='canvas_chunk':
                if not started:raise RuntimeError('chunk without begin')
                for p in m.get('pixels',[]):seen[(int(p['x']),int(p['y']))]=str(p['color']).upper()
            elif m.get('op')=='canvas_end':
                if not started:raise RuntimeError('end without begin')
                return seen
        raise TimeoutError('GET snapshot timeout')
    finally:ws.close()


class Cluster:
    def __init__(self,binary,root,strategy,count):
        if root.exists():raise RuntimeError(f'refusing to reuse state/output {root}')
        root.mkdir(parents=True);self.root=root;self.strategy=strategy;self.binary=binary;self.count=count
        self.proc={};self.log={};self.tcp={};self.ws={};self.argv={}
    def start(self,node,restart=False):
        if not restart:self.tcp[node],self.ws[node]=free_port(),free_port()
        args=[str(self.binary),'--id',str(node),'--port',str(self.tcp[node]),'--ws-port',str(self.ws[node]),
              '--state-file',str(self.root/f'node{node}.state'),'--replicas','3','--read-quorum','2',
              '--write-quorum','2','--strategy',self.strategy,'--min-replicas','2','--max-replicas','4',
              '--reliability-window','120','--policy-interval','2','--policy-cooldown','3',
              '--metrics-dir',str(self.root/f'metrics{node}')]
        if self.strategy=='C':args+=['--write-window','60','--write-report-interval','2']
        if node!=1:args+=['--peers',f'1:127.0.0.1:{self.tcp[1]}']
        self.argv[node]=args
        if shutil.which('stdbuf'):args=['stdbuf','-oL','-eL']+args
        name=f'node{node}.log' if not restart else f'node{node}.restart.log'
        f=(self.root/name).open('w');self.log.setdefault(node,[]).append(f)
        self.proc[node]=subprocess.Popen(args,stdout=f,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL)
    def boot(self):
        self.start(1);wait_for(lambda:placement(self.root/'node1.state'),35,'node1 bootstrap')
        for n in range(2,self.count+1):
            self.start(n)
            wait_for(lambda:all((p:=placement(self.root/f'node{i}.state')) and len(p[1])>=n for i in range(1,n+1)),90,f'admit {n}')
        want=tuple(range(1,self.count+1))
        wait_for(lambda:all((p:=placement(self.root/f'node{i}.state')) and p[1]==want for i in want),60,'all nodes committed')
        wait_for(lambda:n_counts(self.root)==(256,0,0),90,'N2 initial after steady health')
        time.sleep(2)
        return placement(self.root/'node1.state')
    def kill(self,pair):
        for n in pair:
            if self.proc[n].poll() is not None:raise RuntimeError('target already stopped')
            self.proc[n].send_signal(signal.SIGKILL)
        for n in pair:self.proc[n].wait(timeout=3)
    def restart(self,pair):
        for n in pair:self.start(n,restart=True)
    def stop(self):
        for p in self.proc.values():
            if p.poll() is None:p.terminate()
        for p in self.proc.values():
            try:p.wait(timeout=3)
            except subprocess.TimeoutExpired:p.kill();p.wait(timeout=3)
        for ll in self.log.values():
            for f in ll:f.close()


def choose(state,seed):
    candidates=[r for r in range(256) if 1 not in pref(state,r)[:2]]
    if len(candidates)<10:raise RuntimeError('insufficient independent target regions')
    return random.Random(seed).choice(candidates)


def ready(root,region,pair,extra):
    '''All new targets must report successful COMPLETE activation for the generation/epoch when they entered preference list.'''
    changes=[]
    for row in rows(root/'metrics1'/'region_replication_events.csv'):
        try:
            if int(row['region'])==region and row['event_type'] in ('initial','n_change'):
                changes.append((int(row['new_n']),int(row['placement_gen']),int(row['policy_epoch'])))
        except (KeyError,ValueError,TypeError):continue
    if not changes or changes[-1][0]!=4:return False,{'policy':changes[-6:]}
    current_gen=changes[-1][1]; info={'policy':changes[-6:],'targets':{}}
    for pos,node in enumerate(extra,start=3):
        arrival=next(((gen,epoch) for n,gen,epoch in changes if n>=pos and gen==current_gen),None)
        if not arrival:return False,{'missing_arrival_node':node,**info}
        gen,epoch=arrival; candidates=[]
        for file in [root/f'metrics{node}'/'replica_repair_events.csv', root/f'metrics{node}'/'replica_backfill_events.csv']:
            for row in rows(file):
                try:
                    if int(row['region'])!=region or int(row['placement_gen'])!=gen or int(row['policy_epoch'])<epoch:continue
                    # Explicit full-completion metric; partial and empty responses are NOT readiness.
                    event=str(row.get('event','')).lower(); classification=str(row.get('classification','')).lower()
                    result=str(row.get('result','')).lower(); reason=str(row.get('reason','')).lower()
                    full=((event=='backfill_completed' and classification=='complete') or
                          (result in ('ok','true','success','1') and 'complete' in reason and 'partial' not in reason))
                    if full:candidates.append(row)
                except (KeyError,ValueError,TypeError):continue
        info['targets'][str(node)]={'join_epoch':epoch,'complete_events':len(candidates)}
        if not candidates:return False,info
    return True,info


class Heater:
    def __init__(self,cluster,region,seed,rate):
        self.cluster=cluster;self.region=region;self.seed=seed;self.rate=rate;self.ok=0;self.fail=0;self.index=0
        self.stop_flag=threading.Event();self.worker=None;self.samples=[]
    def start(self):
        def loop():
            next_at=time.monotonic()
            while not self.stop_flag.is_set():
                i=self.index;self.index+=1
                # Reuse coordinates but colors change; this is intentional, mimics hot pixel contention.
                x,y=pixel(self.region,i%140);c=color(self.seed,i)
                result=client_put(self.cluster.ws[1],x,y,c,3)
                self.samples.append((i,result));self.ok+=int(result);self.fail+=int(not result)
                next_at+=1/self.rate
                self.stop_flag.wait(max(0,next_at-time.monotonic()))
        self.worker=threading.Thread(target=loop,daemon=True);self.worker.start()
    def stop(self):
        self.stop_flag.set()
        if self.worker:self.worker.join(timeout=5)


def trial(args,root,kind,region=None,heat_seconds=None):
    cluster=Cluster(args.binary,root,kind,args.nodes);heater=None
    try:
        state=cluster.boot();actual=choose(state,args.seed+901)
        if region is None:region=actual
        elif region!=actual:raise AssertionError('inconsistent owner mapping across B and C trials')
        assigned=pref(state,region); pair=assigned[:2]; extra=assigned[2:4];gen=state[0]
        heater=Heater(cluster,region,args.seed+7201,args.heater_rate);heater.start()
        started=time.monotonic(); evidence={}
        if kind=='C':
            wait_for(lambda:accepted_table(root,region) and accepted_table(root,region)[0]==4,60,'C N4 policy')
            def condition():
                nonlocal evidence
                good,evidence=ready(root,region,pair,extra)
                return good
            wait_for(condition,args.backfill_timeout,'C newly assigned replicas full backfill')
            if accepted_table(root,region)[0]!=4:raise RuntimeError('C policy decayed during readiness')
            # Dwell and ensure new owners remain target members (not merely transient N4).
            time.sleep(2)
            good,evidence=ready(root,region,pair,extra)
            if not good:raise RuntimeError('readiness invalidated at fault instant '+repr(evidence))
        else:
            if heat_seconds is None:raise ValueError('B requires C heat duration')
            while time.monotonic()-started<heat_seconds:time.sleep(.2)
            assert accepted_table(root,region)[0]==2,'B unexpectedly changed target from N2'
        before=placement(root/'node1.state')
        if before[0]!=gen or before[1]!=state[1]:raise RuntimeError('INCONCLUSIVE pre-fault placement altered')
        active_seconds=time.monotonic()-started
        if kind=='C' and heater.ok<args.min_heat_ack:raise RuntimeError(f'heater has only {heater.ok} ACKs')
        if kind=='C' and accepted_table(root,region)[0]!=4:raise RuntimeError('C lost N4 at fault instant')
        cluster.kill(pair); killed_at=time.monotonic();results=[]
        for i in range(args.probes):
            x,y=pixel(region,180+i);col=color(args.seed+8000,i)
            got=client_put(cluster.ws[1],x,y,col,args.probe_timeout)
            results.append({'x':x,'y':y,'color':col,'ack':got,'time_since_kill_s':round(time.monotonic()-killed_at,3)})
        probe_seconds=time.monotonic()-killed_at
        after=placement(root/'node1.state')
        if after[0]!=before[0] or after[1]!=before[1]:raise RuntimeError('INCONCLUSIVE placement changed during probes')
        if probe_seconds>=args.pre_remove_limit:raise RuntimeError('INCONCLUSIVE probe window exceeded pre-removal limit')
        heater.stop();heater_stats={'acked':heater.ok,'failed':heater.fail,'attempted':heater.index};heater=None
        answer={'strategy':kind,'region':region,'owners':assigned,'killed':pair,'pre_fault_N':accepted_table(root,region)[0],
                'heat_duration_s':round(active_seconds,2),'heater':heater_stats,
                'probes':results,'acked':sum(p['ack'] for p in results),'readiness':evidence,
                'placement_gen_at_kill':gen,'probe_seconds':round(probe_seconds,2)}
        # Check eventual read visibility via one healthy gateway; don't mistake a first-R stale result for permanent loss.
        if args.check_rejoin and any(p['ack'] for p in results):
            cluster.restart(pair); deadline=time.monotonic()+args.read_timeout;good=False;last=None
            while time.monotonic()<deadline:
                try:
                    values=client_canvas(cluster.ws[1],min(18,max(3,deadline-time.monotonic())))
                    last={str((p['x'],p['y'])):values.get((p['x'],p['y'])) for p in results if p['ack']}
                    if all(values.get((p['x'],p['y']))==p['color'] for p in results if p['ack']):good=True;break
                except Exception as e:last=str(e)
                time.sleep(1)
            answer['eventual_read_visible']=good;answer['last_read']=last
            if not good:raise AssertionError(f'ACKed pixels not visible after {args.read_timeout}s: {last}')
        return answer
    finally:
        if heater:heater.stop()
        cluster.stop()


def cold_trial(args,root,seed):
    c=Cluster(args.binary,root,'C',args.nodes)
    try:
        state=c.boot();region=choose(state,seed+29)
        x,y=pixel(region,7)
        if not client_put(c.ws[1],x,y,color(seed,70)):
            raise AssertionError('cold control PUT did not ACK')
        time.sleep(7)
        table=accepted_table(root,region)
        assert table and table[0]==2, f'cold region N expected 2, got {table}'
        assert n_counts(root)==(256,0,0),f'cold test incorrectly raised target N: {n_counts(root)}'
        return {'region':region,'N':2,'result':'PASS'}
    finally:c.stop()


def decay_trial(args,root,seed):
    c=Cluster(args.binary,root,'C',args.nodes);heater=None
    try:
        state=c.boot();region=choose(state,seed+51)
        heater=Heater(c,region,seed+600,args.heater_rate);heater.start()
        wait_for(lambda:accepted_table(root,region) and accepted_table(root,region)[0]==4,60,'decay control N4 before stopping heater')
        heater.stop();heater=None
        wait_for(lambda:accepted_table(root,region) and accepted_table(root,region)[0]==2,95,'N4 to N2 after write window expires')
        return {'region':region,'N_after_idle':2,'result':'PASS'}
    finally:
        if heater:heater.stop()
        c.stop()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary',type=pathlib.Path,default=pathlib.Path('build/bin/p2place-peer'))
    p.add_argument('--artifacts',type=pathlib.Path,default=pathlib.Path('experiments/results/strategy_c_independent'))
    p.add_argument('--seed',type=int,default=712367)
    p.add_argument('--nodes',type=int,default=6)
    p.add_argument('--heater-rate',type=float,default=3)
    p.add_argument('--min-heat-ack',type=int,default=45)
    p.add_argument('--backfill-timeout',type=float,default=75)
    p.add_argument('--probes',type=int,default=4)
    p.add_argument('--probe-timeout',type=float,default=2.5)
    p.add_argument('--pre-remove-limit',type=float,default=13)
    p.add_argument('--check-rejoin',action=argparse.BooleanOptionalAction,default=True)
    p.add_argument('--read-timeout',type=float,default=40)
    p.add_argument('--seeds',nargs='*',type=int)
    p.add_argument('--cases',nargs='+',choices=['cold','fault','decay'],default=['cold','fault'])
    args=p.parse_args();args.binary=args.binary.resolve();args.artifacts=args.artifacts.resolve()
    if not args.binary.is_file():p.error('binary not found; build first')
    if not 5<=args.nodes<=10:p.error('--nodes must be 5..10')
    if args.heater_rate<=1 or args.probes<1 or args.probes>4:p.error('heater-rate must be >1 and probes 1..4')
    if args.probes*args.probe_timeout>=args.pre_remove_limit:p.error('probe budget exceeds removal window')
    root=args.artifacts;root.mkdir(parents=True,exist_ok=True)
    label=datetime.datetime.now(datetime.timezone.utc).strftime('run_%Y%m%dT%H%M%SZ')
    directory=root/label;suffix=1
    while directory.exists():directory=root/f'{label}_{suffix}';suffix+=1
    directory.mkdir();all_results={};fail=False
    for seed in (args.seeds or [args.seed]):
        args.seed=seed;seed_dir=directory/str(seed);seed_dir.mkdir()
        print(f'\n== independent seed {seed} ==',flush=True)
        record={}
        for case in args.cases:
            try:
                if case=='cold':
                    result=cold_trial(args,seed_dir/'cold',seed)
                elif case=='decay':
                    result=decay_trial(args,seed_dir/'decay',seed)
                else:
                    c=trial(args,seed_dir/'C','C')
                    b=trial(args,seed_dir/'B','B',region=c['region'],heat_seconds=c['heat_duration_s'])
                    if c['owners']!=b['owners']:raise AssertionError('B/C placements differ')
                    if b['acked']!=0:raise AssertionError(f'B negative control must fail before removal: {b["acked"]}')
                    if c['acked']!=args.probes:raise AssertionError(f'C only ACKed {c["acked"]}/{args.probes}')
                    result={'B':b,'C':c}
                record[case]={'status':'PASS','details':result}
                print(f'PASS {case} seed={seed}',flush=True)
            except Exception as e:
                fail=True;traceback.print_exc();record[case]={'status':'FAIL_OR_INCONCLUSIVE','error':str(e)}
            all_results[str(seed)]=record
            (directory/'independent_results.json').write_text(json.dumps(all_results,indent=2))
    print('Logs:',directory,flush=True)
    return int(fail)

if __name__=='__main__':sys.exit(main())
