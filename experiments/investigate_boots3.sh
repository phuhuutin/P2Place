#!/bin/bash
cd ~/projects/p2place/experiments/results/B_deep_isolated_02 || exit 1
echo "=== targeted regions:"; cat target_regions.json
echo
echo "=== latest boots backfill lines for targeted regions:"
for f in node8_boot*.log node9_boot*.log; do
  last=$(ls ${f%_boot*}_boot*.log | tail -1)
  [ "$f" = "$last" ] || continue
  echo "--- $f:"
  grep -E '\[backfill\]|\[quorum\].*GET.*complete_all' "$f" | head -20
done
echo
echo "=== any version=0 backfill lines at all (all nodes, latest boots):"
for n in $(seq 1 10); do
  f=$(ls node${n}_boot*.log 2>/dev/null | tail -1)
  grep -hE '\[backfill\]' "$f" 2>/dev/null | grep -c 'version=0' | xargs echo "node$n version=0 count:"
done
