#!/bin/bash
cd ~/projects/p2place/experiments/results/B_10_seed43_stress50_N2_regression_v2 || exit 1
echo "=== authority (node1) distinct broadcasts:"
grep -hoE '\[policy-broadcast\] node=1 gen=[0-9]+ epoch=[0-9]+' node1_boot1.log | sort | uniq -c
echo
echo "=== node8 boot34 ALL policy lines (apply+ignore) with epoch counts:"
grep -hoE '\[policy-(apply|ignore)\][^(]*' node8_boot34.log | sort | uniq -c
echo
echo "=== node8 boot34: first apply line number + log position of epoch5/6 events:"
grep -nE 'policy' node8_boot34.log | head -4
echo
echo "=== node8 boot34 backfill lines (all 8):"
grep -hE '\[backfill\]' node8_boot34.log
echo
echo "=== node1 broadcast epochs in time order (last 10):"
grep -hE '\[policy-broadcast\]' node1_boot1.log | tail -10
