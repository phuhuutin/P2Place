#!/bin/bash
cd ~/projects/p2place/experiments/results/B_deep_isolated_01 || exit 1
for f in node8_boot*.log; do
  echo "--- $f:"
  grep -hE '\[backfill\].*region=2[34]|\[quorum\].*GET.*region=2[34]|\[quorum-ack\].*GET.*region=2[34]|\[quorum-done\].*GET.*region=2[34]' "$f"
done
echo "=== node9:"
for f in node9_boot*.log; do
  echo "--- $f:"
  grep -hE '\[backfill\].*region=2[34]|\[quorum\].*GET.*region=2[34]|\[quorum-ack\].*GET.*region=2[34]|\[quorum-done\].*GET.*region=2[34]' "$f"
done
