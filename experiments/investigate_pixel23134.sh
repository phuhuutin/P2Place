#!/bin/bash
cd ~/projects/p2place/experiments/results/B_10_seed43_stress50 || exit 1
echo "=== operations CSV rows for x=23,y=134:"
grep -E '^[^,]*,23,134,' runner_operations.csv | head -6
echo
echo "=== fault windows (fault_plan.json):"
python3 - <<'PYEOF'
import json
fp=json.load(open('fault_plan.json'))
print(json.dumps(fp,indent=1)[:1200])
PYEOF
echo
echo "=== PUT quorum-ack lines mentioning pixel 23,134 region (replica-put index):"
echo "region 8 index for (23,134): x%16=7, y%16=6 -> index=6*16+7=103"
grep -hE '\[replica-put\].*region=8 index=103 ' node*_boot*.log | sort | uniq -c | sort -rn | head -8
