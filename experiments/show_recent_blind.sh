#!/bin/bash
cd ~/projects/p2place/experiments/results/strategy_c_blind || exit 1
for d in $(ls -d run_* | sort | tail -4); do
  [ -f "$d/blind_results.json" ] || continue
  echo "== $d"
  python3 - "$d" <<'PYEOF'
import json, sys
d = sys.argv[1]
try:
    s = json.load(open(f'{d}/blind_results.json'))
except Exception as e:
    print('  unreadable:', e); raise SystemExit
print('  results:', {k: v.get('result') for k, v in s.items()})
if 'fault' in s and s['fault'].get('result') == 'PASS':
    c, b = s['fault']['C'], s['fault']['B']
    print(f"  fault: region={c['region']} killed={c['killed']} "
          f"B ACK={b['acked']}/{b['total']} C ACK={c['acked']}/{c['total']} "
          f"heat B={b['heat_s']}s C={c['heat_s']}s")
    print(f"  C read-back: {c.get('snapshot_observed')}")
    print(f"  C heater: {c.get('heater')}  node2 n4_epoch={c.get('n4_epoch')}")
for k in ('cold','hot','idle'):
    if k in s and s[k].get('result') == 'PASS':
        print(f"  {k}: {s[k]}")
PYEOF
done
