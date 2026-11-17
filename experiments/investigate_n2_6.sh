#!/bin/bash
cd ~/projects/p2place/experiments/investigations/n2_regression/B_10_seed43_stress50_300s_N=2 || exit 1
echo "=== all placement_change events (time, gen, fp, reason):"
python3 - <<'PYEOF'
import csv
rows=list(csv.DictReader(open('region_replication_events.csv')))
for r in rows:
    if r['event_type']=='placement_change':
        print(f"  t={float(r['time_s']):8.1f} gen={r['placement_gen']} fp={r['placement_fp'][:12]} epoch={r['policy_epoch']} reason={r['reason']}")
PYEOF
echo
echo "=== n_change events around 678/708 with gen/epoch:"
python3 - <<'PYEOF'
import csv
rows=list(csv.DictReader(open('region_replication_events.csv')))
for r in rows:
    if r['event_type']=='n_change' and r['region']=='133':
        print(f"  t={float(r['time_s']):8.1f} {r['old_n']}->{r['new_n']} gen={r['placement_gen']} epoch={r['policy_epoch']}")
PYEOF
echo
echo "=== node1 (authority) policy-broadcast lines with epochs (last 8):"
grep -hE '\[policy-broadcast\]' node1_boot*.log | tail -8
echo
echo "=== node8 boot9: admission/membership evidence (first 12 lines after commit):"
grep -hE 'HELLO|admit|ADMISSION|membership|election|authority' node8_boot9.log | head -12
echo
echo "=== node8 boot9: any control messages from authority (POLICY_SNAPSHOT type 31)? raw count of 'policy':"
grep -hcE 'policy' node8_boot9.log
