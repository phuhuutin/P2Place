#!/bin/bash
cd ~/projects/p2place/experiments/investigations/n2_regression/B_10_seed43_stress50_300s_N=2 || exit 1
echo "=== files available (verification CSVs?):"
ls | grep -iE 'verif|snapshot|comparison' || echo "(no verification CSVs)"
echo
echo "=== post-600s n_change transitions (old->new distribution + timing):"
python3 - <<'PYEOF'
import csv, collections
rows=list(csv.DictReader(open('region_replication_events.csv')))
post=[r for r in rows if r['event_type']=='n_change' and float(r['time_s'])>600]
trans=collections.Counter((r['old_n'],r['new_n']) for r in post)
print('post-600 transitions:', dict(trans))
times=collections.Counter(round(float(r['time_s'])/30)*30 for r in post)
print('post-600 burst timing (30s buckets):', dict(sorted(times.items())))
print('post-600 regions (sorted):', sorted({int(r['region']) for r in post})[:40], '...')
work=[r for r in rows if r['event_type']=='n_change' and float(r['time_s'])<=600]
print('workload transitions:', dict(collections.Counter((r['old_n'],r['new_n']) for r in work)))
PYEOF
echo
echo "=== replication_summary.csv time range:"
python3 - <<'PYEOF'
import csv
rows=list(csv.DictReader(open('replication_summary.csv')))
print('rows:', len(rows), 'first time:', rows[0].get('time_s'), 'last time:', rows[-1].get('time_s'))
print('cols:', list(rows[0].keys())[:8])
PYEOF
echo
echo "=== fault_plan.json: last 6 events + total:"
python3 - <<'PYEOF'
import json
fp=json.load(open('fault_plan.json'))
print('total events:', len(fp))
for ev in fp[-6:]: print(' ', ev)
PYEOF
