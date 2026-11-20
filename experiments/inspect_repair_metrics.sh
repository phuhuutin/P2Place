#!/bin/bash
latest=$(ls -d ~/projects/p2place/experiments/results/strategy_c_blind/run_* | tail -1)
echo "run: $latest"
echo "=== negative case files:"
ls "$latest/negative" 2>/dev/null | head
echo
echo "=== replica_repair_events.csv sample (first node with data):"
for d in "$latest"/negative/metrics*/; do
  f="$d/replica_repair_events.csv"
  [ -s "$f" ] || continue
  echo "--- $f"
  head -12 "$f"
  break
done
echo
echo "=== urgent_enqueued events across nodes:"
grep -h 'urgent_enqueued' "$latest"/negative/metrics*/replica_repair_events.csv 2>/dev/null | head -8
echo
echo "=== backfill_completed classifications around them:"
grep -h 'backfill_completed' "$latest"/negative/metrics*/replica_repair_events.csv 2>/dev/null | head -8
