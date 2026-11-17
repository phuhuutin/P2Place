#!/bin/bash
cd ~/projects/p2place
D=experiments/results/B_10_seed43_timeline
echo '=== first policy rows for gen=10 (C++ view, node1_boot1):'
awk -F, 'NR>1 && $3==10 {print $2, "gen="$3, "ep="$4, "region="$5, "desired="$8, "assigned="$9; n++; if(n>=4) exit}' "$D/metrics/node1_boot1/policy_node1.csv"
echo
echo '=== region_replication_events: rows mentioning gen=10 (first 6):'
awk -F, 'NR>1 && $8==10 {print "t_ns="$2, "type="$3, "region="$4, "old="$5, "new="$6, "ep="$9; n++; if(n>=6) exit}' "$D/metrics/node1_boot1/region_replication_events.csv"
echo
echo '=== consolidated timeline: first few rows at/after gen 10 (sorted by time_s):'
awk -F, 'NR>1 && $7==10 {print $2, $3, $4, $5, $6; n++; if(n>=6) exit}' "$D/region_replication_events.csv" 2>/dev/null || echo '(no consolidated file)'
echo
echo '=== file mtimes:'
ls -la --time-style=full-iso "$D"/summary.json "$D"/metrics/node1_boot1/policy_node1.csv 2>/dev/null | awk '{print $6, $7, $NF}'
