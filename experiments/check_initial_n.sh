#!/bin/bash
# Verify: Strategy B's FIRST policy broadcast is the N3 baseline (needs >=3 members).
set -u
cd ~/projects/p2place
D=$(mktemp -d)
BIN=./build/bin/p2place-peer
nohup "$BIN" --id 1 --port 9700 --ws-port 9701 --state-file "$D/n1.state" \
  --strategy B --replicas 3 --read-quorum 2 --write-quorum 2 \
  --reliability-window 30 --policy-interval 3 --policy-cooldown 5 \
  --metrics-dir "$D/metrics" > "$D/n1.log" 2>&1 &
P1=$!
sleep 1
nohup "$BIN" --id 2 --port 9710 --ws-port 9711 --state-file "$D/n2.state" \
  --peers 1:127.0.0.1:9700 --strategy B --replicas 3 --read-quorum 2 --write-quorum 2 \
  --reliability-window 30 --policy-interval 3 --policy-cooldown 5 \
  --metrics-dir "$D/metrics" > "$D/n2.log" 2>&1 &
P2=$!
sleep 2
nohup "$BIN" --id 3 --port 9720 --ws-port 9721 --state-file "$D/n3.state" \
  --peers 1:127.0.0.1:9700 --strategy B --replicas 3 --read-quorum 2 --write-quorum 2 \
  --reliability-window 30 --policy-interval 3 --policy-cooldown 5 \
  --metrics-dir "$D/metrics" > "$D/n3.log" 2>&1 &
P3=$!
sleep 15
echo '=== first policy broadcasts on node 1 (initial must be n3=256):'
grep -a 'policy-broadcast' "$D/n1.log" | head -n 3
kill $P1 $P2 $P3 2>/dev/null
rm -rf "$D"
