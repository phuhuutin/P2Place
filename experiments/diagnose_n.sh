#!/bin/bash
cd ~/projects/p2place
echo '=== binary + object + source timestamps:'
ls -la --time-style=full-iso build/bin/p2place-peer build/CMakeFiles/p2place.dir/src/core/replication_policy.cpp.o src/core/replication_policy.cpp | awk '{print $6, $7, $NF}'
echo
echo '=== recent experiment result dirs:'
ls -dt experiments/results/*/ 2>/dev/null | head -n 8
echo
echo '=== latest run: first policy rows from the per-incarnation policy CSV (C++ view):'
LATEST=$(ls -dt experiments/results/*/ 2>/dev/null | head -n 1)
echo "dir: $LATEST"
for f in "$LATEST"/metrics/node1_boot*/policy_node1.csv; do
  [ -f "$f" ] && { echo "--- $f"; awk -F, 'NR>1 {print $2, "gen="$3, "ep="$4, "region="$5, "desired="$8, "assigned="$9, "confirmed="$10}' "$f" | head -n 4; }
done
echo
echo '=== initial rows from the C++ region_replication_events (per-incarnation):'
for f in "$LATEST"/metrics/node1_boot*/region_replication_events.csv; do
  [ -f "$f" ] && { echo "--- $f"; awk -F, 'NR>1 && $3=="initial" {print $2, "region="$4, "new_n="$6; n++; if(n>=3) exit}' "$f"; }
done
