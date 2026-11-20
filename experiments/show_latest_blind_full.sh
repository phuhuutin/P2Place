#!/bin/bash
cd ~/projects/p2place/experiments/results/strategy_c_blind || exit 1
latest=$(ls -d run_* | sort | tail -1)
echo "run: $latest"
python3 - "$latest" <<'PYEOF'
import json, sys
d = sys.argv[1]
s = json.load(open(f'{d}/blind_results.json'))
print(json.dumps(s, indent=1)[:4000])
PYEOF
