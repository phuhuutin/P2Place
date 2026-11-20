#!/usr/bin/env python3
"""Manual repro: 3-node C cluster; dump write-report-related log lines."""
import pathlib, shutil, subprocess, sys, time, re
sys.path.insert(0, str(pathlib.Path("/home/phuhuutin/projects/p2place/tests"))) if "/tests" not in ":".join(sys.path) else None
sys.path.insert(0, str(pathlib.Path("/home/phuhuutin/projects/p2place/tests"))) if not any("tests" in p for p in sys.path) else None
from test_strategy_c import start, wait_for, connect_ws, put_retry
from test_lab06_strategy_b import placement_fields

root = pathlib.Path('/tmp/c_repro')
shutil.rmtree(root, ignore_errors=True)
root.mkdir(parents=True)
procs, logs, pp, wp = {}, {}, {}, {}
procs[1], logs[1], pp[1], wp[1] = start(root, 1, 'C')
wait_for(lambda: placement_fields(root / 'node1.state') is not None, 'bootstrap')
for n in (2, 3):
    procs[n], logs[n], pp[n], wp[n] = start(root, n, 'C', seed=(1, pp[1]))
wait_for(lambda: all((s := placement_fields(root / f'node{n}.state')) and s[0] >= 3
                     for n in (1, 2, 3)), 'gen 3')
time.sleep(6)
# a few writes through gateway 2
ws, r = connect_ws(wp[2])
for i in range(6):
    x, y = 48 + i, 160
    put_retry(ws, r, x, y, '#112233')
r.close()
time.sleep(8)
print('=== node2 log: write-report / REGION_WRITE lines:')
print('\n'.join(l for l in open(logs[2].name).read().splitlines() if 'write-report' in l or 'REGION_WRITE' in l)[:2000])
print('=== node1 log: write-report lines:')
print('\n'.join(l for l in open(logs[1].name).read().splitlines() if 'write-report' in l)[:2000])
print('=== node1 log: policy-broadcast tail:')
print('\n'.join(l for l in open(logs[1].name).read().splitlines() if 'policy-broadcast' in l)[-3:][:600])
for p in procs.values():
    if p.poll() is None:
        p.terminate()
for p in procs.values():
    try:
        p.wait(timeout=3)
    except subprocess.TimeoutExpired:
        p.kill()
