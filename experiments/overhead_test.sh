#!/bin/bash
# Overhead comparison: identical stable A runs with timeline on vs off.
set -u
cd ~/projects/p2place
rm -f experiments/debug_merge*.sh experiments/syntax_check.sh experiments/smoke_test.sh
rm -rf experiments/results/TL_overhead_on experiments/results/TL_overhead_off
python3 experiments/run_experiment.py --strategy A --seed 11 --seconds 40 \
  --churn-profile none --out experiments/results/TL_overhead_on 2>&1 | grep -E '\[run\] finished'
python3 experiments/run_experiment.py --strategy A --seed 11 --seconds 40 \
  --churn-profile none --no-replication-timeline \
  --out experiments/results/TL_overhead_off 2>&1 | grep -E '\[run\] finished'
python3 - <<'EOF'
import json
for name in ('TL_overhead_on', 'TL_overhead_off'):
    d = json.load(open(f'experiments/results/{name}/summary.json'))
    print(f'{name}: acked={d["acked"]} failed={d["failed"]} '
          f'success={d.get("success_pct")}% avg_ack_latency={d.get("latency_ms_acked_avg")}ms')
EOF
