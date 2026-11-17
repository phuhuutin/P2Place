#!/bin/bash
cd ~/projects/p2place/experiments/investigations/n2_regression/B_10_seed43_stress50_300s_N=2 || exit 1
python3 - <<'PYEOF'
import csv, json, random, collections
rows=list(csv.DictReader(open('region_replication_events.csv')))
post600=sorted({int(r['region']) for r in rows if r['event_type']=='n_change' and float(r['time_s'])>600})
print('post-600 shrunken regions:', post600)

# example pixels from both checkpoints
s=json.load(open('summary.json'))
ex_imm=[(e['x'],e['y']) for e in s['verification_immediate'].get('examples',[])]
ex_set=[(e['x'],e['y']) for e in s['verification_after_settle'].get('examples',[])]
def reg(x,y): return (y//16)*16+x//16
print('after-settle examples in shrunken regions:')
for (x,y) in ex_set:
    if reg(int(x),int(y)) in post600:
        print(f'  ({x},{y}) region={reg(int(x),int(y))}')
print('immediate examples in shrunken regions:')
for (x,y) in ex_imm:
    if reg(int(x),int(y)) in post600:
        print(f'  ({x},{y}) region={reg(int(x),int(y))}')

# write records for the after-settle example pixels
ops=list(csv.DictReader(open('runner_operations.csv')))
ack=[r for r in ops if str(r['success']).lower() in ('true','1')]
print('ack rows:', len(ack))
want={(int(x),int(y)) for (x,y) in ex_set}
for r in ack:
    if (int(r['x']),int(r['y'])) in want:
        print(f"  WRITE attempt={r['attempt']} t={r['time_s']} via={r['via_node']} region={r['region']} ({r['x']},{r['y']}) {r['color']}")
PYEOF
