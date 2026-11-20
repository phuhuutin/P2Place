#!/usr/bin/env python3
"""P2Place B/C targeted dual-owner fault experiment; one strategy per command.

A deliberately adversarial demonstration of write-aware replication, NOT a random
churn benchmark or proof that C generally outperforms B.

Launch B first to write a reproducible scenario.json; run C using the same --out-root
and --seed to reuse exactly the same region and original owner pair.

Assumptions: P2Place --strategy B/C, browser put ACK protocol, [quorum]
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
import shutil
import signal
import socket
import struct
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from collections import Counter
import threading
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ws_client import put_pixel  # verified client-visible pixel-echo ACK semantics
from targeted_bc_dashboard_export import export_dashboard, collect_native_metrics

# Small append-only audit observer; never changes peer behavior or workload timing.
_AUDIT = None

class BootAudit:
    def __init__(self, out):
        self.out=out;self.events=[];self.starts={};self.boots={}
        self.anchor_ns=None

    def launch(self, node, proc, node_dir, log_start=None):
        boot=self.boots.get(node,0)+1
        self.boots[node]=boot
        log=node_dir/'node.log'
        try:pos=log.stat().st_size
        except OSError:pos=0
        self.starts[(node,boot)]=pos if log_start is None else log_start
        self.events.append((time.monotonic_ns(),node,boot,'launch',proc.pid))
        print(f'[node] start n={node} boot={boot} pid={proc.pid}',flush=True)

    def kill(self,node,proc):
        self.events.append((time.monotonic_ns(),node,self.boots[node],'kill',proc.pid))
        print(f'[fault] KILL node={node} boot={self.boots[node]}',flush=True)

    def before_restart(self,node,node_dir):
        boot=self.boots[node]
        self.archive(node,node_dir,boot)

    def archive(self,node,node_dir,boot):
        dest=self.out/'metrics'/f'node{node}_boot{boot}'
        dest.mkdir(parents=True,exist_ok=True)
        for file in node_dir.iterdir():
            if file.is_file() and file.suffix in ('.csv','.jsonl'):
                shutil.copy2(file,dest/file.name)

    def anchor(self):
        self.anchor_ns=time.monotonic_ns()

    def finish(self):
        # A/B ground truth: preserve negative startup times (before the test's anchor).
        with (self.out/'node_process_events.csv').open('w',newline='') as f:
            w=csv.writer(f);w.writerow(('time_s','node_id','boot','action','pid'))
            for ns,n,boot,action,pid in self.events:
                w.writerow((round((ns-self.anchor_ns)/1e9,3) if self.anchor_ns else '',n,boot,action,pid))
        # Capture final boot's native metrics and split existing cumulative logs
        # into clean per-boot slices, preserving native nodeN/node.log unchanged.
        for n,boot_count in self.boots.items():
            nd=self.out/f'node{n}'
            self.archive(n,nd,boot_count)
            path=nd/'node.log'
            if not path.exists():continue
            data=path.read_bytes()
            for b in range(1,boot_count+1):
                begin=self.starts[(n,b)]
                end=self.starts[(n,b+1)] if b<boot_count else len(data)
                (self.out/f'node{n}_boot{b}.log').write_bytes(data[begin:end])


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
    """Client-visible PUT result.

    P2Place has no dedicated ``put_ok`` message.  The browser protocol (and
    ``web/app.js``) treats the quorum-gated ``{"op":"pixel",...}`` echo as the
    confirmation of a successful PUT; a failure is an ``{"op":"error"}`` frame.
    Server ``[quorum-done]`` lines are corroboration, never client ACKs.
    """
    result = put_pixel(port, x, y, color, timeout=timeout)
    return result["result"] == "ack", result["reason"] or result["result"]


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
    # Line-buffer the peers' stdout: readiness and selection parse node logs,
    # and block-buffered C++ iostreams can hide fresh [quorum]/[placement] lines.
    prefix=[shutil.which('stdbuf'),'-oL','-eL'] if shutil.which('stdbuf') else []
    try:
        for i in range(1,args.nodes+1):
            nd=out/f'node{i}';nd.mkdir(parents=True,exist_ok=True)
            cmd=[*prefix,str(args.peer),'--id',str(i),'--port',str(ports[i][0]),'--ws-port',str(ports[i][1]),
                 '--replicas','3','--read-quorum','2','--write-quorum','2','--strategy',strategy,
                 '--min-replicas','2','--max-replicas','4',
                 '--policy-interval',str(args.policy_interval),'--policy-cooldown',str(args.policy_cooldown),
                 '--write-window',str(args.write_window),'--write-report-interval',str(args.write_report_interval),
                 '--write-threshold-mid',str(args.write_threshold_mid),'--write-threshold-high',str(args.write_threshold_high),
                 '--metrics-dir',str(nd)]
            if i!=1:cmd+=['--peers',f'1:127.0.0.1:{ports[1][0]}']
            cmd+=args.peer_arg
            fh=(nd/'node.log').open('a',buffering=1)
            log_start=fh.tell()
            proc=subprocess.Popen(cmd,cwd=nd,stdout=fh,stderr=subprocess.STDOUT,start_new_session=True)
            nodes[i]={'proc':proc,'log':fh,'cmd':cmd,'dir':nd}
            if _AUDIT is not None:_AUDIT.launch(i,proc,nd,log_start)
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
    if _AUDIT is not None:_AUDIT.before_restart(i,info['dir'])
    info['log'].close()
    fh=(info['dir']/'node.log').open('a',buffering=1)
    log_start=fh.tell()
    proc=subprocess.Popen(info['cmd'],cwd=info['dir'],stdout=fh,stderr=subprocess.STDOUT,start_new_session=True)
    nodes[i]=dict(info,log=fh,proc=proc)
    if _AUDIT is not None:_AUDIT.launch(i,proc,info['dir'],log_start)
    wait_port(ports[i][1],proc)


def pixel(region,sequence,seed):
    x=(region%16)*16+(sequence%16)
    y=(region//16)*16+((sequence//16)%16)
    d=hashlib.sha256(f'{seed}:{sequence}'.encode()).digest()
    return x,y,f'#{d[0]:02X}{d[1]:02X}{d[2]:02X}'


def sha256_file(path):
    digest=hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024*1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def kill_owners(nodes,owners):
    """Signal both owners before waiting, so the crash is simultaneous."""
    for node in owners:
        proc=nodes[node]['proc']
        if proc.poll() is None:
            if _AUDIT is not None:_AUDIT.kill(node,proc)
            try: os.killpg(proc.pid,signal.SIGKILL)
            except ProcessLookupError: pass
    for node in owners:
        try: nodes[node]['proc'].wait(timeout=3)
        except subprocess.TimeoutExpired: pass


def probe_burst(port,region,seed,start,base_sequence,count,timeout,wave,scheduled):
    """Issue all immediate fault probes concurrently, outside the workload pool.

    Using the shared ThreadPoolExecutor would let the normal offered load delay
    probes past the fixed-generation window.  A fresh connection per probe and a
    matching pixel echo prevent cross-probe ACK confusion.
    """
    results=[None]*count
    barrier=threading.Barrier(count)
    def worker(index):
        sequence=base_sequence+index
        x,y,color=pixel(region,sequence,seed)
        try: barrier.wait(timeout=5)
        except threading.BrokenBarrierError: pass
        issued=time.monotonic()
        ok,why=ws_put(port,x,y,color,timeout)
        results[index]=dict(
            time_s=round(time.monotonic()-start,3),phase='immediate_fault_probe',
            sequence=sequence,region=region,x=x,y=y,color=color,ack=int(ok),reason=why,
            latency_ms=round((time.monotonic()-issued)*1000,3),wave=wave,gateway=1,
            scheduled_time_s=round(scheduled,3))
    threads=[threading.Thread(target=worker,args=(i,),daemon=True) for i in range(count)]
    for thread in threads: thread.start()
    for thread in threads: thread.join(timeout=timeout+3)
    return [result for result in results if result]


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
    raise RuntimeError('No eligible N=2 region found (check actual PUT quorum logs and browser command/ACK protocol in node1/node.log)')


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
                'gen':int(last['placement_gen']),
                'confirmed':int(last.get('confirmed_live_copies') or 0)}
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


def completed_put_for_region(log,region):
    """Latest [quorum] PUT for a region that already has a [quorum-done] line.

    Avoids a readiness race with the continuously running workload: the newest
    [quorum] line may still be in flight, so walk backwards to the last request
    whose client-independent server outcome is already known.
    """
    pattern=re.compile(r'\[quorum\] node=1 op=PUT request=(\d+)[^\n]*?region='+str(region)
                       +r'\b[^\n]*?N=(\d+)[^\n]*?W=(\d+)[^\n]*?targets=([\d,]*)')
    for match in reversed(list(pattern.finditer(log))):
        request=match.group(1)
        done=re.search(r'\[quorum-done\] node=1 op=PUT request='+request
                       +r' ok=([01]) ack=(\d+)/(\d+) error=([^\n]*)',log[match.start():])
        if not done:continue
        return dict(request=int(request),n=int(match.group(2)),w=int(match.group(3)),
                    targets=[int(v) for v in match.group(4).split(',') if v],
                    ok=done.group(1)=='1',ack=int(done.group(2)),
                    required=int(done.group(3)),error=done.group(4).strip())
    return None


def readiness(args,strategy,out,nodes,region,owners):
    """Evaluate readiness against routed targets, quorum outcome and completion evidence."""
    log_path=nodes[1]['dir']/'node.log'
    log=log_path.read_text(errors='replace') if log_path.exists() else ''
    last=completed_put_for_region(log,region)
    if not last:
        return False,{'reason':'no_completed_put','region':region}
    committed=list(re.finditer(r'\[placement\] committed generation=(\d+)[^\n]*?owners=(\d+)',log))
    current_gen=int(committed[-1].group(1)) if committed else None
    current_owners=int(committed[-1].group(2)) if committed else None
    stable=bool(committed and current_owners==args.nodes)
    policy=latest_policy(nodes[1]['dir'],region)
    result={'policy':policy,'last_completed_put':last,'original_owners':owners,
            'placement_gen':current_gen,'placement_stable':stable}
    w2=last['w']==2 and last['ok'] and last['ack']>=2

    if strategy=='B':
        # B uses N=2 as the control. policy_node1.csv is optional, but when
        # present (metrics enabled) it must agree with N=2 and the live gen.
        broadcast=list(re.finditer(r'\[policy-broadcast\] node=1 gen=(\d+) epoch=(\d+) n2=(\d+) n3=(\d+) n4=(\d+)',log))
        n2_broadcast=bool(committed and broadcast
                          and int(broadcast[-1].group(1))==current_gen
                          and int(broadcast[-1].group(3))==256
                          and int(broadcast[-1].group(4))==0
                          and int(broadcast[-1].group(5))==0)
        genuine=(last['n']==2 and last['w']==2 and list(last['targets'])==list(owners) and w2)
        policy_ok=policy is None or (policy['gen']==current_gen and policy['n']==2)
        result.update(n2_policy_broadcast=n2_broadcast,
                      successful_original_owner_quorum=genuine,
                      policy_n=policy['n'] if policy else None)
        return bool(genuine and stable and n2_broadcast and policy_ok),result

    if not policy:
        return False,dict(result,reason='missing_policy_csv_for_C',
             explanation='C needs policy/backfill CSVs; launch peers with --metrics-dir. No crash executed.')
    if not stable or policy['gen']!=current_gen:
        return False,dict(result,reason='C_policy_gen_mismatch')
    if policy['n']!=4 or last['n']!=4 or len(last['targets'])<4 or list(last['targets'][:2])!=list(owners):
        return False,dict(result,reason='not_N4_or_owner_order_changed')
    if not w2:
        return False,dict(result,reason='no_successful_W2_quorum_on_N4_targets')
    extras=list(last['targets'][2:4]);events=complete_backfills(out,region,policy['gen'])
    # Infer each new target's activation epoch from N transitions. A direct
    # 2->4 jump activates BOTH positions at the same epoch; a setdefault() on
    # an N=3 row would leave position 2 missing and wrongly fail readiness.
    joins={2:None,3:None}
    path=next(iter(nodes[1]['dir'].rglob('policy_node1.csv')),None)
    if path:
        previous=0
        try:
            with path.open(newline='') as f:
                for row in csv.DictReader(f):
                    if row.get('region')!=str(region):continue
                    try:
                        if int(row['placement_gen'])!=policy['gen']:continue
                        assigned=int(row['assigned_n']);epoch=int(row['policy_epoch'])
                    except (KeyError,ValueError):continue
                    if assigned>=3 and previous<3:joins[2]=epoch
                    if assigned>=4 and previous<4:joins[3]=epoch
                    previous=assigned
        except (OSError,csv.Error):pass
    checks=[]
    for index,node in enumerate(extras,start=2):
        join_epoch=joins.get(index)
        valid=join_epoch is not None and any(e>=join_epoch for e in events.get(node,[]))
        checks.append({'node':node,'join_epoch':join_epoch,'complete_epochs':events.get(node,[]),'ready':valid})
    expected_confirmed=len(last['targets'])-(1 if 1 in last['targets'] else 0)
    live_ok=policy['confirmed']>=expected_confirmed
    result.update({'additional_replicas':checks,'placement_gen':policy['gen'],
                   'confirmed_live_copies':policy['confirmed'],
                   'expected_confirmed_live_copies':expected_confirmed,
                   'live_copies_ok':live_ok})
    return bool(all(c['ready'] for c in checks) and len(checks)==2 and live_ok),result



# Fault wave schedule is relative to the start of the 900-second measurement.
DEFAULT_WAVES = [(180., 225.), (360., 405.), (540., 585.), (720., 765.)]


def ws_exchange(port, payload, timeout=4.0, max_messages=4096):
    """Return (messages, complete) for one browser request.

    This is a GET inspection helper, not a PUT ACK oracle.  ``complete`` is
    True only when ``canvas_end`` arrived; a partial or timed-out snapshot must
    not be interpreted as missing pixels.
    """
    key = base64.b64encode(os.urandom(16)).decode()
    messages=[];complete=False
    with socket.create_connection(('127.0.0.1',port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall((f'GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\n'
                      f'Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n'
                      'Sec-WebSocket-Version: 13\r\n\r\n').encode())
        pending=b''
        while b'\r\n\r\n' not in pending:
            chunk=sock.recv(4096)
            if not chunk:raise EOFError('closed during handshake')
            pending += chunk
            if len(pending)>16384:raise ValueError('oversize handshake')
        if not pending.startswith(b'HTTP/1.1 101'):
            raise ValueError('handshake unsuccessful')
        pending=pending.split(b'\r\n\r\n',1)[1]
        msg=json.dumps(payload,separators=(',',':')).encode()
        mask=os.urandom(4)
        prefix=(bytes([0x81,0x80|len(msg)]) if len(msg)<126 else
                bytes([0x81,0xfe])+struct.pack('!H',len(msg)))
        sock.sendall(prefix+mask+bytes(b^mask[i%4] for i,b in enumerate(msg)))
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline and len(messages)<max_messages:
            def take(n):
                nonlocal pending
                while len(pending)<n:
                    chunk=sock.recv(max(n-len(pending),4096))
                    if not chunk:raise EOFError('closed during frame')
                    pending+=chunk
                r,pending=pending[:n],pending[n:]
                return r
            try:
                header=take(2)
                op=header[0]&15
                length=header[1]&127
                if length==126:length=struct.unpack('!H',take(2))[0]
                elif length==127:length=struct.unpack('!Q',take(8))[0]
                if length>2_000_000:raise ValueError('oversize frame')
                frame_mask=take(4) if header[1]&128 else None
                data=take(length)
                if frame_mask:data=bytes(b^frame_mask[i%4] for i,b in enumerate(data))
                if op==8:break
                if op==1:
                    try:message=json.loads(data)
                    except json.JSONDecodeError:continue
                    messages.append(message)
                    if isinstance(message,dict) and message.get('op')=='canvas_end':
                        complete=True
                        break
            except (OSError,EOFError):break
        return messages,complete


def flatten_canvas(messages):
    """Decode only unambiguous known structures; otherwise return None (not zero mismatches)."""
    points={}
    recognized=False
    def visit(item):
        nonlocal recognized
        if isinstance(item,list):
            if len(item)==256 and all(isinstance(r,list) and len(r)==256 for r in item):
                recognized=True
                for y,row in enumerate(item):
                    for x,color in enumerate(row):
                        if isinstance(color,str):points[(x,y)]=color.upper()
                return
            for obj in item:visit(obj)
        elif isinstance(item,dict):
            if isinstance(item.get('x'),int) and isinstance(item.get('y'),int) and isinstance(item.get('color'),str):
                recognized=True
                points[(item['x'],item['y'])]=item['color'].upper()
            for name in ('pixels','canvas','data','regions','chunks','updates'):
                if name in item:visit(item[name])
    visit(messages)
    return points if recognized else None


def verify_canvas(out,checkpoint,ports,expected,seed,samples,readers,elapsed):
    """One real get_canvas per reader; sampled `(pixel,reader)` comparisons.

    Expected colors are last *confirmed client-ACKed* writes to unambiguous
    coordinates.  A GET that never reaches ``canvas_end`` is UNREADABLE, not a
    mismatch, and is retried once.  Sampling every available unambiguous pixel
    when the oracle has fewer than ``samples`` is MEASURED, not INCONCLUSIVE.
    """
    rng=random.Random(seed+0xBEEF)
    positions=sorted(expected)
    rng.shuffle(positions)
    available=len(positions)
    chosen=positions[:min(samples,available)]
    details=[];statuses=Counter()
    for reader in readers:
        start=time.monotonic()
        canvas=None;error=''
        for attempt in range(2):
            try:
                responses,complete=ws_exchange(ports[reader][1],{'op':'get_canvas'},timeout=8)
                canvas=flatten_canvas(responses)
                error_seen=any(isinstance(m,dict) and m.get('op')=='error' for m in responses)
                if complete and not error_seen and canvas is not None:
                    error=''
                    break
                if not complete:error='snapshot_incomplete'
                elif error_seen:error='snapshot_error'
                else:error='unsupported_get_canvas_shape'
                canvas=None
            except Exception as e:
                canvas=None;error=type(e).__name__+':'+str(e)[:100]
            if attempt==0:time.sleep(0.5)
        latency=round(1000*(time.monotonic()-start),2)
        for x,y in chosen:
            wanted=expected[(x,y)]
            observed=canvas.get((x,y)) if canvas is not None else None
            status=('unreadable' if canvas is None else 'missing' if observed is None else
                    'match' if observed==wanted else 'mismatch')
            statuses[status]+=1
            details.append(dict(checkpoint=checkpoint,elapsed_s=round(elapsed,3),pixel_x=x,pixel_y=y,
                                reader_node=reader,expected=wanted,observed=observed or '',
                                status=status,latency_ms=latency,error=error))
    path=out/'verification_samples.csv'
    with path.open('a',newline='') as f:
        w=csv.DictWriter(f,fieldnames=['checkpoint','elapsed_s','pixel_x','pixel_y','reader_node',
                          'expected','observed','status','latency_ms','error'])
        if f.tell()==0:w.writeheader()
        w.writerows(details)
    target=min(samples,available)
    return dict(checkpoint=checkpoint,elapsed_s=round(elapsed,3),
                requested_sample_pixels=samples,available_sample_pixels=available,
                sampled_pixels=len(chosen),reader_nodes=readers,comparisons=len(details),
                counts=dict(statuses),mismatch_count=statuses['mismatch']+statuses['missing'],
                status=('MEASURED' if target>0 and not statuses['unreadable']
                        and len(chosen)==target else 'INCONCLUSIVE'),
                note='Compares to last positively client-ACKed value; concurrent writes to the '
                     'same coordinate make the expectation ambiguous. Missing can be a transient '
                     'R=2 read race OR data lost when a killed owner held the only copy; interpret '
                     'by region ownership, not as a universal policy verdict.')


def choose_region_with_owners(args,nodes,ports,scenario):
    """Test candidate regions until initial preference pair is exactly [3,4]."""
    if scenario:
        region=int(scenario['region']);owners=list(map(int,scenario['owners']))
        if owners!=[3,4]:raise RuntimeError('scenario owners must be [3,4]')
        return find_selection(args,nodes,ports,region,owners)
    candidates=random.Random(args.seed).sample(range(256),256)
    for reg in candidates:
        x,y,c=pixel(reg,-1,args.seed)
        ws_put(ports[1][1],x,y,c,args.ack_timeout)
        time.sleep(.05)  # let the line-buffered [quorum] line reach the log
        records=quorum_records(nodes[1]['dir']/'node.log',reg)
        if any(n==2 and list(targets)==[3,4] for n,targets in records):
            return reg,[3,4]
    raise RuntimeError('No stable region with initial owners [3,4]; do not substitute arbitrary owners')


def main():
    global _AUDIT
    p=argparse.ArgumentParser(description='P2Place 900s aggressive fixed-pair B/C repeated fault experiment')
    p.add_argument('--strategy',choices=['B','C'],required=True)
    p.add_argument('--seed',type=int,default=712367)
    p.add_argument('--out-root',type=Path,default=Path('experiments/results/bc_900_aggressive'))
    p.add_argument('--peer',type=Path,default=Path('build/bin/p2place-peer'))
    p.add_argument('--peer-arg',action='append',default=[])
    p.add_argument('--nodes',type=int,default=10)
    p.add_argument('--seconds',type=float,default=900)
    p.add_argument('--rate',type=float,default=20)
    p.add_argument('--heat-until',type=float,default=780)
    p.add_argument('--waves',default='180:225,360:405,540:585,720:765',help='crash:restart pairs; node 3+4 only')
    p.add_argument('--probes',type=int,default=8)
    p.add_argument('--workers',type=int,default=32)
    p.add_argument('--max-pending',type=int,default=128)
    p.add_argument('--ack-timeout',type=float,default=2.0)
    p.add_argument('--join-spacing',type=float,default=.3)
    p.add_argument('--stabilize',type=float,default=15)
    p.add_argument('--verify-pixels',type=int,default=1500)
    p.add_argument('--verify-nodes',type=int,default=3)
    p.add_argument('--policy-interval',type=int,default=2)
    p.add_argument('--policy-cooldown',type=int,default=3)
    p.add_argument('--write-window',type=int,default=60)
    p.add_argument('--write-report-interval',type=int,default=2)
    p.add_argument('--write-threshold-mid',type=float,default=.2)
    p.add_argument('--write-threshold-high',type=float,default=1.0)
    args=p.parse_args()
    if args.nodes!=10:p.error('must have 10 nodes')
    if args.rate<=0 or args.workers<1 or args.max_pending<args.workers:p.error('invalid workload settings')
    if args.probes<1 or args.verify_pixels<1:p.error('invalid probe/verification sample counts')
    if not (1<=args.verify_nodes<=9):p.error('verify-nodes must be 1..9 (owners are excluded)')
    try:
        waves=[tuple(float(x) for x in item.split(':')) for item in args.waves.split(',')]
    except ValueError:p.error('waves must look like 180:225,360:405')
    if not waves or any(not (0<k<r<args.seconds) for k,r in waves) or any(waves[i][1]>=waves[i+1][0] for i in range(len(waves)-1)):
        p.error('invalid crash/rejoin schedule')
    if waves[-1][1]>=args.heat_until or args.heat_until>=args.seconds:p.error('heat-until must follow final rejoin and precede end')
    args.peer=args.peer.resolve()
    if not args.peer.is_file():p.error('binary missing: '+str(args.peer))
    peer_sha=sha256_file(args.peer)
    root=args.out_root.resolve()/f'seed{args.seed}'
    root.mkdir(parents=True,exist_ok=True)
    manifest=root/'scenario.json'
    scenario=json.loads(manifest.read_text()) if manifest.exists() else None
    spec=dict(seed=args.seed,nodes=args.nodes,seconds=args.seconds,rate=args.rate,waves=waves,
              heat_until=args.heat_until,probes=args.probes,ack_timeout=args.ack_timeout,
              workers=args.workers,max_pending=args.max_pending,
              verify_pixels=args.verify_pixels,verify_nodes=args.verify_nodes,
              join_spacing=args.join_spacing,stabilize=args.stabilize,
              policy_interval=args.policy_interval,policy_cooldown=args.policy_cooldown,
              write_window=args.write_window,write_report_interval=args.write_report_interval,
              write_threshold_mid=args.write_threshold_mid,write_threshold_high=args.write_threshold_high,
              peer_sha256=peer_sha)
    if scenario and any(scenario.get(k)!=v for k,v in spec.items() if k!='waves'):
        raise RuntimeError('shared scenario settings mismatch; use same arguments and --out-root')
    if scenario and scenario.get('waves') != [list(x) for x in waves]:
        raise RuntimeError('shared fault schedule mismatch')
    out=root/args.strategy
    if out.exists():raise RuntimeError(f'{out} already exists; use a fresh --out-root')
    out.mkdir()
    _AUDIT=BootAudit(out)
    (out/'config.json').write_text(json.dumps({k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},indent=2))
    ports=free_ports(10)
    summary=dict(strategy=args.strategy,seed=args.seed,status='INCONCLUSIVE',spec=spec,
                 ports=ports,waves=[],verification={},attempts=0,acks=0,skipped=0)
    nodes={};start=None
    try:
        nodes=launch(args,args.strategy,out,ports)
        initial_gen=wait_full_cluster(nodes,10)
        summary['initial_placement_gen']=initial_gen
        summary['peer_sha256']=peer_sha
        if scenario:
            stored=scenario.get('initial_placement_gen')
            if stored is not None and int(stored)!=initial_gen:
                raise RuntimeError(f'initial placement generation mismatch: scenario={stored} '
                                   f'run={initial_gen}; refusing to compare different placements')
        time.sleep(args.stabilize)
        region,owners=choose_region_with_owners(args,nodes,ports,scenario)
        if not scenario:
            manifest.write_text(json.dumps(dict(spec,initial_placement_gen=initial_gen,
                                                region=region,owners=owners),indent=2))
        summary.update(region=region,original_owners=owners)
        print(f'[{args.strategy}] region={region} owners={owners} 900s aggressive repeated fault test',flush=True)

        # Positive control: actual client ACK. If unknown, do not run a meaningless 900s test.
        x,y,c=pixel(region,-1000,args.seed)
        ack,reason=ws_put(ports[1][1],x,y,c,args.ack_timeout)
        summary['positive_control']=dict(ack=ack,reason=reason)
        if not ack:
            raise RuntimeError('Client ACK positive control failed ('+reason+'). Fix WS parser/response before running experiment.')

        _AUDIT.anchor()
        start=time.monotonic()
        summary['time_base']={'workload_start_monotonic_ns':_AUDIT.anchor_ns,'clock':'CLOCK_MONOTONIC'}
        next_slot=start
        pending={};rows=[];expected={};ambiguous=set()
        # Keep a CSV line per scheduled request and every fault probe.
        with (out/'puts.csv').open('w',newline='') as put_file, (out/'fault_events.csv').open('w',newline='') as event_file, (out/'replication_timeline.csv').open('w',newline='') as timeline_file, ThreadPoolExecutor(max_workers=args.workers) as pool:
            fields=['time_s','phase','sequence','region','x','y','color','ack','reason','latency_ms','wave','gateway','scheduled_time_s']
            writer=csv.DictWriter(put_file,fieldnames=fields);writer.writeheader()
            ew=csv.DictWriter(event_file,fieldnames=['time_s','wave','event','nodes','detail']);ew.writeheader()
            nw=csv.DictWriter(timeline_file,fieldnames=['time_s','strategy','region','target_n','placement_generation','policy_epoch','source']);nw.writeheader()
            last_n_sample=-1
            def event(i,kind,detail=''):
                elapsed=time.monotonic()-start
                ew.writerow(dict(time_s=round(elapsed,3),wave=i,event=kind,nodes='3,4',detail=detail));event_file.flush()
                print(f'[{args.strategy} {elapsed:.1f}s] wave {i}: {kind} {detail}',flush=True)
            def perform(seq,phase,scheduled,wave):
                # 90% target region and 10% deterministic background traffic.
                hot=(seq % 10 != 0) or phase=='immediate_fault_probe'
                target=region if hot else (region+1+((seq//10)*17)%255)%256
                xx,yy,color=pixel(target,seq,args.seed)
                t0=time.monotonic()
                ok,why=ws_put(ports[1][1],xx,yy,color,args.ack_timeout)
                return dict(time_s=round(time.monotonic()-start,3),phase=phase,sequence=seq,region=target,
                            x=xx,y=yy,color=color,ack=int(ok),reason=why,latency_ms=round((time.monotonic()-t0)*1000,3),
                            wave=wave,gateway=1,scheduled_time_s=round(scheduled,3))
            def enqueue(seq,phase,scheduled,wave):
                if len(pending)>=args.max_pending:
                    summary['skipped']+=1
                    return False
                fut=pool.submit(perform,seq,phase,scheduled,wave)
                pending[fut]=None
                return True
            def drain():
                for fut in [f for f in pending if f.done()]:
                    del pending[fut]
                    row=fut.result()
                    writer.writerow(row);rows.append(row)
                    if row['ack']:
                        coord=(row['x'],row['y'])
                        # Multiple concurrent ACKed writes to same pixel cannot be totally ordered
                        # from client receipts alone. Exclude those coordinates from oracle.
                        if coord in expected:ambiguous.add(coord)
                        expected[coord]=row['color']
                put_file.flush()
            wave_state=[0]*len(waves)
            immediate_verification=None
            seq=1
            last_report=-1
            while time.monotonic()-start < args.seconds:
                elapsed=time.monotonic()-start
                # Actions precede issuing new scheduled writes.
                for i,(kill_at,rejoin_at) in enumerate(waves):
                    if wave_state[i]==0 and elapsed>=kill_at:
                        drain()
                        latest=readiness(args,args.strategy,out,nodes,region,owners)
                        ready,detail=latest
                        # Require complete extra replicas for C before first fault only.
                        if i==0:summary['first_wave_readiness']=detail
                        if i==0 and not ready:
                            raise RuntimeError('INCONCLUSIVE pre-fault readiness: '+json.dumps(detail))
                        event(i+1,'precrash_state',json.dumps(detail)[:1500])
                        kill_elapsed=round(time.monotonic()-start,3)
                        kill_owners(nodes,owners)
                        wave_state[i]=1
                        event(i+1,'SIGKILL')
                        summary['waves'].append(dict(wave=i+1,kill_time_s=kill_elapsed,
                                                     precrash_state=detail))
                        # Dedicated concurrent burst: the shared pool can be
                        # congested with normal offered load and would dilute
                        # the immediate post-crash window.
                        probe_rows=probe_burst(ports[1][1],region,args.seed,start,
                                               1_000_000+i*100,args.probes,args.ack_timeout,
                                               i+1,kill_elapsed)
                        for row in probe_rows:
                            writer.writerow(row);rows.append(row)
                            if row['ack']:
                                coord=(row['x'],row['y'])
                                if coord in expected:ambiguous.add(coord)
                                expected[coord]=row['color']
                        put_file.flush()
                        event(i+1,'PROBES',f'{sum(r["ack"] for r in probe_rows)}/{len(probe_rows)} ACK')
                    if wave_state[i]==1 and elapsed>=rejoin_at:
                        for node in owners:restart(nodes,node,ports)
                        wave_state[i]=2
                        event(i+1,'RESTART')
                        summary['waves'][i]['rejoin_time_s']=round(time.monotonic()-start,3)
                # Take the first independent verification checkpoint as soon as writes cease.
                if elapsed>=args.heat_until and immediate_verification is None:
                    drain()
                    # Workload may still have pending requests. Finish them before forming the oracle.
                    while pending:
                        drain()
                        if pending:time.sleep(.05)
                    snapshot={k:v for k,v in expected.items() if k not in ambiguous}
                    readers=[j for j in range(1,11) if j not in owners][:args.verify_nodes]
                    immediate_verification=pool.submit(verify_canvas,out,'immediate',ports,snapshot,
                                                         args.seed,args.verify_pixels,readers,time.monotonic()-start)
                # Precise bounded-concurrency offered load. Account for dropped scheduling slots.
                now=time.monotonic()
                while now>=next_slot and next_slot-start<args.heat_until:
                    scheduled=next_slot-start
                    wave=next((i+1 for i,(a,b) in enumerate(waves) if a<=scheduled<b),0)
                    phase='fault_window' if wave else 'heat' if scheduled<waves[0][0] else 'recovery'
                    enqueue(seq,phase,scheduled,wave)
                    seq+=1;next_slot+=1/args.rate
                    if seq>1_000_000:raise RuntimeError('sequence limit exceeded')
                drain()
                whole=int(elapsed)
                if whole!=last_n_sample:
                    last_n_sample=whole
                    pol=latest_policy(nodes[1]['dir'],region)
                    if pol:
                        nw.writerow(dict(time_s=whole,strategy=args.strategy,region=region,
                                         target_n=pol['n'],placement_generation=pol['gen'],policy_epoch=pol['epoch'],source='policy_node1.csv'))
                    else:
                        # Log-only fallback is observable evidence but may be stale.
                        qp=nodes[1]['dir']/'node.log'
                        try:
                            with qp.open('rb') as fh:
                                fh.seek(0,2);sz=fh.tell();fh.seek(max(0,sz-32768));tail=fh.read().decode(errors='replace')
                            rec=[m for m in QUORUM.finditer(tail) if int(m.group(1))==region]
                            if rec:nw.writerow(dict(time_s=whole,strategy=args.strategy,region=region,target_n=int(rec[-1].group(2)),
                                                    placement_generation='',policy_epoch='',source='recent_quorum_log'))
                        except OSError:pass
                    timeline_file.flush()
                if whole%30==0 and whole!=last_report:
                    last_report=whole
                    print(f'[{args.strategy} {whole}s] scheduled={seq-1} completed={len(rows)} '
                          f'acks={sum(r["ack"] for r in rows)} pending={len(pending)} skipped={summary["skipped"]}',flush=True)
                time.sleep(.015 if elapsed<args.heat_until else .1)
            # Wait for remaining PUT futures without extending beyond one ACK timeout.
            while pending:
                drain()
                if pending:time.sleep(.05)
        summary['scheduled_workload_puts']=seq-1
        summary['attempts']=len(rows)
        summary['acks']=sum(r['ack'] for r in rows)
        summary['put_success_pct']=round(100*summary['acks']/len(rows),2) if rows else 0
        summary['phase_counts']={phase:dict(attempts=sum(r['phase']==phase for r in rows),
                                  acks=sum(r['ack'] for r in rows if r['phase']==phase))
                                 for phase in sorted({r['phase'] for r in rows})}
        summary['probe_counts']={str(i+1):dict(attempts=sum(r['phase']=='immediate_fault_probe' and r['wave']==i+1 for r in rows),
                                     acks=sum(r['ack'] for r in rows if r['phase']=='immediate_fault_probe' and r['wave']==i+1))
                                 for i in range(len(waves))}
        summary['final_policy']=latest_policy(nodes[1]['dir'],region)
        # Never claim an ACKed overwrite can establish a unique expected state.
        unambiguous={k:v for k,v in expected.items() if k not in ambiguous}
        readers=[i for i in range(1,11) if i not in owners][:args.verify_nodes]
        # Immediate verification sampled at ~780s; final after 900s recovery.
        if immediate_verification is not None:
            summary['verification']['immediate']=immediate_verification.result()
        result=verify_canvas(out,'after_recovery',ports,unambiguous,args.seed,args.verify_pixels,
                             readers,time.monotonic()-start)
        summary['verification']['after_recovery']=result
        for checkpoint,value in summary['verification'].items():
            print(f'[{args.strategy}] verification {checkpoint}: {value}',flush=True)
        (out/'verification_summary.json').write_text(json.dumps(summary['verification'],indent=2))
        summary['status']='COMPLETED' if all(v['status']=='MEASURED' for v in summary['verification'].values()) else 'COMPLETED_VERIFICATION_INCONCLUSIVE'
        summary['actual_runtime_s']=round(time.monotonic()-start,2)
        print(f'[{args.strategy}] {summary["status"]}: ACK {summary["acks"]}/{summary["attempts"]}, skipped {summary["skipped"]}',flush=True)
    except BaseException as e:
        summary['error']=repr(e)
        print(f'[{args.strategy}] INCONCLUSIVE/FAILED: {e}',flush=True)
        raise
    finally:
        (out/'summary.json').write_text(json.dumps(summary,indent=2))
        teardown(nodes)
        try:
            _AUDIT.finish()
            export_dashboard(out,summary,args,_AUDIT.boots)
        except Exception as exc:
            summary['dashboard_export_error']=str(exc)
            (out/'summary.json').write_text(json.dumps(summary,indent=2))
            print(f'[dashboard] ERROR exporting compatible results: {exc}',flush=True)
            raise
        print('RESULTS:',out,flush=True)

if __name__=='__main__':main()
