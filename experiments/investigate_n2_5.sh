#!/bin/bash
cd ~/projects/p2place/experiments/investigations/n2_regression/B_10_seed43_stress50_300s_N=2 || exit 1
echo "=== anti-entropy pushes of region 133 from holders (nodes 1, 10):"
grep -hE '\[anti-entropy\] node=(1|10) region=133 ' node1_boot*.log node10_boot*.log | head -8
echo "count node1 pushes region133: $(grep -hcE '\[anti-entropy\] node=1 region=133 ' node1_boot*.log)"
echo "count node10 pushes region133: $(grep -hcE '\[anti-entropy\] node=10 region=133 ' node10_boot*.log)"
echo
echo "=== region 133 received on 8/9 (replica-repair / backfill) in FINAL boots:"
for n in 8 9; do
  f=$(ls node${n}_boot*.log | tail -1)
  echo "--- $f:"
  grep -hE '\[replica-repair\].*region=133 |\[backfill\].*region=133 |\[quorum\] node='$n' op=GET.*region=133 ' "$f" | head -10
done
echo
echo "=== quorum-ack for the write request 1073742734 (who ACKed the write):"
grep -hE 'request=1073742734' node5_boot*.log | grep -E 'quorum-ack|quorum-failure|quorum-done' | head -6
echo
echo "=== reader 1 snapshot GET for region=133 near t=900 (last 4 GET lines for region 133):"
grep -hE '\[quorum\] node=1 op=GET.*region=133 ' node1_boot*.log | tail -4
echo "--- acks/ignores right after the LAST region=133 GET on node1:"
lastreq=$(grep -hE '\[quorum\] node=1 op=GET.*region=133 ' node1_boot*.log | tail -1 | sed 's/.*request=\([0-9]*\).*/\1/')
echo "last GET request=$lastreq"
grep -hE "request=$lastreq" node1_boot*.log | head -10
