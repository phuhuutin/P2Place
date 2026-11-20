#!/usr/bin/env python3
"""Repro T3-C: 10-node C cluster, hot region, check reports + policy."""
import pathlib, shutil, subprocess, sys, time, re
sys.path.insert(0, '/home/phuhuutin/projects/p2place/tests')
sys.path.insert(0, '/home/phuhuutin/projects/p2place/experiments')
from test_strategy_c import start, wait_for, connect_ws, put_retry, hot_region_with_pair
from test_lab06_strategy_b import placement_fields

root = pathlib.Path('/tmp/c_t3_repro')
shutil.rmtree(root, ignore_errors=True)
root.mkdir(parents=True)
procs, logs, pp, wp = {}, {}, {}, {}
procs[1], logs[1], pp[1], wp[1] = start(root, 1, 'C')
wait_for(lambda: placement_fields(root / 'node1.state') is not None, 'bootstrap')
for n in range(2, 11):
    procs[n], logs[n], pp[n], wp[n] = start(root, n, 'C', seed=(1, pp[1]))
wait_for(lambda: all((s := placement_fields(root / f'node{n}.state')) and s[0] >= 10
                     for n in range(1, 11)), 'gen 10')
wait_for(lambda: 'policy-broadcast' in open(logs[1].name).read(), 'initial broadcast')
time.sleep(3)
hot, pref = hot_region_with_pair(root)
print('hot region:', hot, 'pref:', pref)
ws, r = connect_ws(wp[2])
for i in range(30):
    x = (hot % 16) * 16 + (i % 16)
    y = (hot // 16) * 16 + (i // 16)
    put_retry(ws, r, x, y, '#%06X' % i)
r.close()
time.sleep(20)
log1 = open(logs[1].name).read()
print('=== node1 write-report lines (tail):')
print('\n'.join(l for l in log1.splitlines() if 'write-report' in l)[-1500:])
print('=== node1 policy-broadcast lines:')
print('\n'.join(l for l in log1.splitlines() if 'policy-broadcast' in l)[-1200:])
print('=== node2 tracker send lines:')
print('\n'.join(l for l in open(logs[2].name).read().splitlines() if 'write-report' in l)[-800:])
for p in procs.values():
    if p.poll() is None:
        p.terminate()
for p in procs.values():
    try:
        p.wait(timeout=3)
    except subprocess.TimeoutExpired:
        p.kill()
