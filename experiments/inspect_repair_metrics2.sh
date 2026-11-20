#!/bin/bash
cd ~/projects/p2place/experiments/results/strategy_c_blind || exit 1
run=$(ls -d run_* | sort | tail -2 | head -1)   # the completed negative run
echo "run: $run"
find "$run" -maxdepth 2 -name 'replica_repair_events.csv' | head -4
echo
f=$(find "$run" -maxdepth 2 -name 'replica_repair_events.csv' | head -1)
if [ -n "$f" ]; then
  echo "=== sample from $f:"
  head -12 "$f"
fi
echo
echo "=== urgent_enqueued anywhere in the run:"
find "$run" -maxdepth 2 -name 'replica_repair_events.csv' -exec grep -h 'urgent_enqueued' {} \; | head -8
echo
echo "=== backfill_completed anywhere:"
find "$run" -maxdepth 2 -name 'replica_repair_events.csv' -exec grep -h 'backfill_completed' {} \; | head -8
