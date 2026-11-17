#!/bin/bash
cd ~/projects/p2place/experiments/results/B_10_seed43_stress50 || exit 1
echo "=== fault plan for nodes 8 and 9 (window around t=287):"
python3 - <<'PYEOF'
import json
plan=json.load(open('fault_plan.json'))
for ev in plan:
    if ev['node'] in (8,9) and 240 <= ev['at_s'] <= 340:
        print(ev)
PYEOF
echo
echo "=== node5 log: PUT quorum for region=129 index=103 (pixel 23,134):"
grep -hE '\[quorum\].*PUT.*region=129 index=103 |\[quorum-ack\].*PUT.*index=103|\[quorum-done\].*PUT' node5_boot*.log | head -8
echo
echo "=== replica-put lines for region=129 index=103 (which replicas applied it):"
grep -hE '\[replica-put\].*region=129 index=103 ' node*_boot*.log | sort | head -8
