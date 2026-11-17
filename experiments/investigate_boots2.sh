#!/bin/bash
cd ~/projects/p2place/experiments/results/B_deep_isolated_01 || exit 1
echo "=== node8 boot7 requests 1073741828/29 (backfill region 23/24):"
grep -E 'request=1073741828|request=1073741829' node8_boot7.log | head -20
echo
echo "=== node9 boot2 requests 1073741835/36 (backfill region 23/24):"
grep -E 'request=1073741835|request=1073741836' node9_boot2.log | head -20
echo
echo "=== node8 boot7: first snapshot round GET region=23 (request 1073741918) acks:"
grep -E 'request=1073741918|request=1073741919' node8_boot7.log | head -12
echo
echo "=== who later repaired node8 (replica-repair region 23/24 in boot7):"
grep -E '\[replica-repair\]|\[anti-entropy\]' node8_boot7.log | grep -E 'region=2[34]|23 ' | head -12
echo
echo "=== node10 boot: did node10 push region 23/24 via anti-entropy?"
grep -E '\[anti-entropy\]' node10_boot1.log | grep -E 'region=2[34]' | head -8
echo "=== node8 boot7 region_store clear + reset evidence:"
grep -E 'reset_to_joiner|clearing region|region_store|stale-generation' node8_boot7.log | head -6
