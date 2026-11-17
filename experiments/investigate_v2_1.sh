#!/bin/bash
cd ~/projects/p2place/experiments/results/B_10_seed43_stress50_N2_regression_v2 || exit 1
echo "=== summary verification numbers:"
python3 - <<'PYEOF'
import json
s=json.load(open('summary.json'))
for chk in ('verification_immediate','verification_after_settle'):
    v=s.get(chk,{})
    print(f"{chk}: missing={v.get('missing')} read_failures={v.get('read_failures')}")
    for e in v.get('examples',[])[:12]:
        print(f"  reader={e.get('reader')} x={e.get('x')} y={e.get('y')} expected={e.get('expected')} actual={e.get('actual')}")
PYEOF
echo
echo "=== placement gens + post-600 events in THIS run:"
python3 - <<'PYEOF'
import csv, collections
rows=list(csv.DictReader(open('region_replication_events.csv')))
gens=sorted({int(r['placement_gen']) for r in rows})
print('placement gens:', gens)
post=[r for r in rows if r['event_type']=='n_change' and float(r['time_s'])>600]
print('post-600 n_change:', len(post), dict(collections.Counter((r['old_n'],r['new_n']) for r in post)))
pre=[r for r in rows if r['event_type']=='n_change' and float(r['time_s'])<=600]
print('workload n_change:', len(pre), dict(collections.Counter((r['old_n'],r['new_n']) for r in pre)))
pc=[r for r in rows if r['event_type']=='placement_change']
print('placement_change events:', [(float(r['time_s']), r['placement_gen']) for r in pc])
PYEOF
