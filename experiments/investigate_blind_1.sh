#!/bin/bash
cd ~/projects/p2place/experiments/results/strategy_c_blind/run_20261117T092443Z/fault/C || exit 1
echo "=== blind_results (fault outcome):"
python3 - <<'PYEOF'
import json
s=json.load(open('blind_results.json'))
print(json.dumps(s.get('fault',{}),indent=1)[:1500])
PYEOF
echo
echo "=== region 192 events:"
grep ',192,' metrics1/region_replication_events.csv | head -8
echo
echo "=== region_write_frequency for region 192 (rate column):"
python3 - <<'PYEOF'
import csv
rows=list(csv.DictReader(open('metrics1/region_write_frequency.csv')))
r192=[r for r in rows if r.get('region')=='192']
print('rows for region 192:', len(r192))
for r in r192[:6]+r192[-4:]:
    print(' t=%s rate=%s n_churn=%s n_write=%s target=%s epoch=%s obs=%s' % (
        r.get('time_s'), r.get('write_rate_per_sec'), r.get('n_churn'),
        r.get('n_write'), r.get('target_n'), r.get('policy_epoch'),
        r.get('observation_complete')))
PYEOF
echo
echo "=== backfill events for region 192 in node 1/2 metrics:"
for n in 1 2; do
  echo "--- metrics$n/replica_backfill_events.csv:"
  grep ',192,' metrics$n/replica_backfill_events.csv 2>/dev/null | head -4
done
echo
echo "=== node1/2 backfill log lines for region 192:"
for n in 1 2; do
  echo "--- node$n.log:"
  grep '\[backfill\].*region=192 ' node$n.log 2>/dev/null | head -4
done
echo
echo "=== how far their backfill queues got (max region id backfilled):"
for n in 1 2; do
  echo -n "node$n max backfilled region: "
  grep -oE '\[backfill\] node='$n' region=[0-9]+' node$n.log 2>/dev/null | grep -oE '[0-9]+$' | sort -n | tail -1
  echo -n "node$n backfill count: "
  grep -c '\[backfill\]' node$n.log 2>/dev/null
done
