#!/bin/bash
cd ~/projects/p2place/experiments/investigations/n2_regression/B_10_seed43_stress50_300s_N=2 || exit 1
echo "=== region 133 N timeline:"
python3 - <<'PYEOF'
import csv
rows=list(csv.DictReader(open('region_replication_events.csv')))
for r in rows:
    if r['region']=='133' and r['event_type']=='n_change':
        print(f"  t={float(r['time_s']):8.1f}  {r['old_n']} -> {r['new_n']}  epoch={r['policy_epoch']}")
PYEOF
echo
echo "=== write attempt 5157 (86,140) region=133 index=198: coordinator node5 + applies:"
grep -hE '\[quorum\] node=5 op=PUT request=.* region=133 index=198 ' node5_boot*.log | head -2
grep -hE 'request=[0-9]+ region=133 index=198 ' node5_boot*.log | grep -E 'quorum-ack|quorum-failure|ws-put' | head -8
echo "--- replica applies of region=133 index=198 (all nodes):"
grep -hE '\[replica-put\].*region=133 index=198 ' node*_boot*.log | sort
echo
echo "=== N2 pair for region 133 under the FINAL placement (from node1.state):"
python3 - <<'PYEOF'
fields=open('node1.state').read().split()
count=int(fields[2]); members=[int(x) for x in fields[3:3+count]]
offset=3+count*(2 if fields[0].endswith('V2') else 1)
owners=[int(x) for x in fields[offset:offset+256]]
o=owners[133]
pair=[members[(members.index(o)+j)%count] for j in range(2)]
quad=[members[(members.index(o)+j)%count] for j in range(4)]
print('members:', members)
print('region 133 owner idx -> N2 pair:', pair, ' N4 set:', quad)
PYEOF
echo
echo "=== node5 PUT for region 133 index=198 with targets (first quorum line before ack):"
grep -hE '\[quorum\] node=5 op=PUT request=[0-9]+ region=133 index=198 ' node5_boot*.log
