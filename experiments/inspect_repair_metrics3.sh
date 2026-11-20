#!/bin/bash
cd ~/projects/p2place/experiments/results/strategy_c_blind/run_20261117T094546Z || exit 1
echo "=== metrics5 repair events (first 14):"
head -14 negative/metrics5/replica_repair_events.csv
echo
echo "=== urgent_enqueued across nodes:"
grep -h 'urgent_enqueued' negative/metrics*/replica_repair_events.csv | head -8
echo
echo "=== backfill_completed (first 8):"
grep -h 'backfill_completed' negative/metrics*/replica_repair_events.csv | head -8
echo
echo "=== how quickly did urgent completions follow enqueues (region-wise monotonic ns delta):"
python3 - <<'PYEOF'
import csv, glob, collections
enq={}; done={}
for f in glob.glob('negative/metrics*/replica_repair_events.csv'):
    node=f.split('metrics')[1].split('/')[0]
    for r in csv.DictReader(open(f)):
        key=(node,r['region'])
        if r['event']=='urgent_enqueued': enq[key]=int(r['monotonic_ns'])
        elif r['event']=='backfill_completed': done[key]=int(r['monotonic_ns'])
lat=[]
for k in done:
    if k in enq:
        lat.append((done[k]-enq[k])/1e6)
lat.sort()
print('matched urgent->complete pairs:', len(lat))
if lat:
    print('latency ms: min=%.0f median=%.0f max=%.0f' % (lat[0], lat[len(lat)//2], lat[-1]))
PYEOF
