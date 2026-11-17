#!/bin/bash
cd ~/projects/p2place/experiments/results/B_10_seed43_stress50_N2_regression_v2 || exit 1
for n in 8 9; do
  f=$(ls node${n}_boot*.log | tail -1)
  echo "=== $f: $(wc -l < $f) lines"
  echo "--- policy lines:"
  grep -hE '\[policy-apply\]|\[policy-ignore\]' "$f" | head -6
  echo "--- backfill count: $(grep -hcE '\[backfill\]' "$f")"
  echo "--- backfill sample (first 6):"
  grep -hE '\[backfill\]' "$f" | head -6
  echo "--- any region=134/95/92 lines:"
  grep -hcE 'region=(134|95|92) ' "$f"
  echo
done
echo "=== node8 boot9 first 10 lines:"
head -10 node8_boot9.log
