#!/bin/bash
cd ~/projects/p2place/experiments/results/strategy_c_blind || exit 1
latest=$(ls -d run_* | sort | tail -1)
echo "run: $latest"
python3 - "$latest" <<'PYEOF'
import json, sys
d = sys.argv[1]
s = json.load(open(f'{d}/blind_results.json'))
f = s['fault']
c, b = f['C'], f['B']
print(json.dumps({
    'C': {k: c.get(k) for k in ('acked','total','pre_fault_n','n4_epoch','heat_s','heater','snapshot_observed')},
    'B': {k: b.get(k) for k in ('acked','total','pre_fault_n','heat_s','heater')},
}, indent=1))
PYEOF
