#!/bin/bash
cd ~/projects/p2place/experiments/results/B_deep_isolated_N2 || exit 1
echo "=== all distinct policy-broadcast epochs on authority (dedup):"
grep -hE '\[policy-broadcast\]' node1_boot1.log | awk '!seen[$0]++' | head -8
echo
echo "=== node8/9 final-boot backfill lines:"
for n in 8 9; do
  f=$(ls node${n}_boot*.log | tail -1)
  echo "--- $f:"
  grep -hE '\[backfill\]' "$f" | head -14
done
echo
echo "=== node8 final-boot superset GET example (one region):"
f8=$(ls node8_boot*.log | tail -1)
grep -hE 'op=GET.*fanout' "$f8" | head -4
echo
echo "=== targeted regions list:"
cat target_regions.json
