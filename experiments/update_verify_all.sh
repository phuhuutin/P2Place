#!/bin/bash
cd ~/projects/p2place || exit 1
python3 - <<'PYEOF'
p = "verify_all.sh"
s = open(p).read()
anchor = '''echo "==================== live: lab06 strategy B (stable/churn/recovery + A regression) ===================="
python3 tests/test_lab06_strategy_b.py 2>&1 | grep -vE 'WebSocket handshake|PASS: HTTP' | tail -n 15
'''
add = anchor + '''
echo "==================== live: strategy C (aggregation + B contrast + two-owner survival) ===================="
python3 tests/test_strategy_c.py 2>&1 | grep -vE 'WebSocket handshake|PASS: HTTP' | tail -n 15
'''
assert anchor in s, "anchor not found"
open(p, "w").write(s.replace(anchor, add, 1))
print("verify_all.sh updated")
PYEOF
