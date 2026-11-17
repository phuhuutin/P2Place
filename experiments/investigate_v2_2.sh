#!/bin/bash
cd ~/projects/p2place/experiments/results/B_10_seed43_stress50_N2_regression_v2 || exit 1
echo "=== write record for (110,136) region 134:"
grep -E ",134,110,136," runner_operations.csv
echo
echo "=== region 134 n_change timeline:"
python3 - <<'PYEOF'
import csv
rows=list(csv.DictReader(open('region_replication_events.csv')))
for r in rows:
    if r['region']=='134' and r['event_type']=='n_change':
        print(f"  t={float(r['time_s']):8.1f} {r['old_n']}->{r['new_n']} epoch={r['policy_epoch']}")
PYEOF
echo
echo "=== N2 pair / N4 set for region 134:"
python3 - <<'PYEOF'
fields=open('node1.state').read().split()
count=int(fields[2]); members=[int(x) for x in fields[3:3+count]]
offset=3+count*(2 if fields[0].endswith('V2') else 1)
owners=[int(x) for x in fields[offset:offset+256]]
o=owners[134]
pair=[members[(members.index(o)+j)%count] for j in range(2)]
quad=[members[(members.index(o)+j)%count] for j in range(4)]
print('N2 pair:', pair, ' N4 set:', quad)
PYEOF
echo
echo "=== replica-put applies for region=134 (index for (110,136): x%16=14,y%16=8 -> 8*16+14=142):"
grep -hE '\[replica-put\].*region=134 index=142 ' node*_boot*.log | sort
echo
echo "=== node8/9 final-boot backfill of region 134:"
for n in 8 9; do f=$(ls node${n}_boot*.log | tail -1); echo "--- $f:"; grep -hE '\[backfill\].*region=134 |\[quorum\].*op=GET.*region=134 ' "$f" | head -4; done
