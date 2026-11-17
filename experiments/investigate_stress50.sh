#!/bin/bash
cd ~/projects/p2place/experiments/results/B_10_seed43_stress50 || exit 1
echo "=== which nodes backfilled region=8 and what classification/version did they get (final boots first):"
for n in 1 2 3 8 9 10; do
  echo "--- node$n (last 2 boots):"
  for f in $(ls node${n}_boot*.log | tail -2); do
    grep -hE '\[backfill\].*region=8 ' "$f" | tail -4 | sed "s/^/  ${f##*/}: /"
  done
done
echo
echo "=== region=8 version=0 backfills anywhere in the run:"
grep -l '\[backfill\] node=.* region=8 success=true version=0' node*_boot*.log 2>/dev/null | head
echo
echo "=== late-populated-discard signature for region=8 (quorum-ignore after a region=8 GET):"
grep -hE '\[quorum\].*GET.*region=8 |\[quorum-ignore\].*request=' node*_boot*.log 2>/dev/null | grep -B1 -A0 'region=8 ' | head -6
