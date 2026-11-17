#!/bin/bash
cd ~/projects/p2place/experiments/results/B_10_seed43_stress50 || exit 1
for n in 8 9; do
  total=$(cat node${n}_boot*.log | grep -c '\[backfill\]')
  r129=$(cat node${n}_boot*.log | grep -c 'backfill.*region=129 ')
  echo "node$n: $total backfill lines total, region=129: $r129"
done
echo "--- node8 final boot backfill sample:"
grep -hE '\[backfill\]' node8_boot17.log 2>/dev/null | head -4
echo "--- node8 final boot region=129 replica-get lines (serving snapshots):"
grep -hE 'replica-get.*region=129' node8_boot17.log 2>/dev/null | head -3
