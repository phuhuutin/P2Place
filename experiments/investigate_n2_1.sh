#!/bin/bash
cd ~/projects/p2place/experiments/investigations/n2_regression/B_10_seed43_stress50_300s_N=2 || exit 1
echo "=== config.json (identity-relevant fields):"
python3 - <<'PYEOF'
import json
c=json.load(open('config.json'))
for k in sorted(c):
    if any(s in k.lower() for s in ('ver','sha','git','binary','commit','strategy','min','max','settle','verify','replica')):
        print(f"  {k} = {c[k]}")
PYEOF
echo
echo "=== summary.json verification objects (examples only):"
python3 - <<'PYEOF'
import json
s=json.load(open('summary.json'))
for chk in ('verification_immediate','verification_after_settle','verification'):
    v=s.get(chk,{})
    print(f"--- {chk}: missing={v.get('missing')} read_failures={v.get('read_failures')} sampled={v.get('sampled_pixels')} checked={v.get('checked_nodes')}")
    for e in v.get('examples',[]):
        print(f"    reader={e.get('reader')} x={e.get('x')} y={e.get('y')} expected={e.get('expected')} actual={e.get('actual')}")
PYEOF
echo
echo "=== region_replication_events.csv: time range + post-workload events:"
head -2 region_replication_events.csv
python3 - <<'PYEOF'
import csv
rows=list(csv.DictReader(open('region_replication_events.csv')))
print('rows:', len(rows), 'columns:', list(rows[0].keys()))
# find the time column
tk=[k for k in rows[0] if 'time' in k.lower() or 't_' in k.lower()]
print('time cols:', tk)
if tk:
    tcol=tk[0]
    times=sorted(float(r[tcol]) for r in rows)
    print('time min/max:', times[0], times[-1])
    post=[r for r in rows if float(r[tcol])>600]
    print('events after t=600s:', len(post))
    for r in post[:25]:
        print('  ', {k:v for k,v in r.items() if k in (tcol,'event_type','region','from_n','to_n') or k in (tcol,)})
PYEOF
echo
echo "=== replication_summary.csv time range:"
head -2 replication_summary.csv
python3 - <<'PYEOF'
import csv
rows=list(csv.DictReader(open('replication_summary.csv')))
print('rows:', len(rows), 'cols:', list(rows[0].keys()))
PYEOF
