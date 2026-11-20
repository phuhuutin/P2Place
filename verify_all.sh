#!/bin/bash
# Full verification: build, unit tests, and every live integration test.
set -u
cd ~/projects/p2place

echo "==================== BUILD ===================="
cmake --build build 2>&1 | grep -E 'error|Error' && { echo "BUILD FAILED"; exit 1; }
echo "build OK"

echo "==================== CTEST ===================="
ctest --test-dir build --output-on-failure 2>&1 | tail -n 6

echo "==================== live: lab06 join regression (A+B+C) ===================="
python3 tests/test_lab06_join_seed_independent.py 2>&1 | grep -vE 'WebSocket handshake|PASS: HTTP' | tail -n 15

echo "==================== live: lab06 churn stress (multi-node recovery) ===================="
python3 tests/test_lab06_churn_stress.py --peer ./build/bin/p2place-peer --pixels 32 --rounds 1 2>&1 | tail -n 12

echo "==================== live: lab06 stale first-restart rejoin (A/B/C/D) ===================="
python3 tests/test_lab06_stale_rejoin.py 2>&1 | grep -vE 'WebSocket handshake|PASS: HTTP' | tail -n 15

echo "==================== live: lab06 stale AUTO_REMOVE revalidation ===================="
python3 tests/test_lab06_auto_remove_stale.py 2>&1 | grep -vE 'WebSocket handshake|PASS: HTTP' | tail -n 8

echo "==================== offline: replication-N timeline reconstruction ===================="
python3 tests/test_timeline_reconstruct.py 2>&1 | tail -n 8

echo "==================== live: lab06 strategy B (stable/churn/recovery + A regression) ===================="
python3 tests/test_lab06_strategy_b.py 2>&1 | grep -vE 'WebSocket handshake|PASS: HTTP' | tail -n 15

echo "==================== live: strategy C (aggregation + B contrast + two-owner survival) ===================="
python3 tests/test_strategy_c.py 2>&1 | grep -vE 'WebSocket handshake|PASS: HTTP' | tail -n 15

echo "==================== live: join via seed (existing) ===================="
python3 tests/test_join_via_seed.py 2>&1 | tail -n 5

echo "==================== live: ring election ===================="
python3 tests/test_ring_election_live.py --peer ./build/bin/p2place-peer 2>&1 | tail -n 6

echo "==================== live: membership ===================="
python3 tests/test_live_membership.py --peer ./build/bin/p2place-peer 2>&1 | tail -n 6

echo "==================== live: lab05 quorum ===================="
python3 tests/test_lab05_live_quorum.py --peer ./build/bin/p2place-peer 2>&1 | tail -n 8

echo "==================== live: lab06 browser sync ===================="
python3 tests/test_lab06_live_browser_sync.py --peer ./build/bin/p2place-peer 2>&1 | tail -n 8

echo "==================== ALL VERIFICATION DONE ===================="
