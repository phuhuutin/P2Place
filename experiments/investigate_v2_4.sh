#!/bin/bash
cd ~/projects/p2place/experiments/results/B_10_seed43_stress50_N2_regression_v2 || exit 1
for n in 8 9; do
  f=$(ls -v node${n}_boot*.log | tail -1)
  echo "=== FINAL boot $f: $(wc -l < "$f") lines"
  echo "--- policy-apply lines:"
  grep -hE '\[policy-apply\]' "$f" | head -8
  echo "--- backfill count: $(grep -hcE '\[backfill\]' "$f")"
  echo "--- region 134 backfill + GET lines:"
  grep -hE '\[backfill\].*region=134 |\[quorum\].*op=GET.*region=134 ' "$f" | head -6
  echo "--- region 95 backfill + GET lines:"
  grep -hE '\[backfill\].*region=95 |\[quorum\].*op=GET.*region=95 ' "$f" | head -6
  echo "--- replica-get region=134 count (serving snapshot reads):"
  grep -hcE '\[replica-get\].*region=134 ' "$f"
  echo
done
echo "=== region 134 holders re-check (all boots, numeric):"
grep -hE '\[replica-put\].*region=134 index=142 ' node*_boot*.log | sort -u
