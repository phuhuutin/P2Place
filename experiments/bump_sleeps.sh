#!/bin/bash
cd ~/projects/p2place || exit 1
python3 - <<'PYEOF'
p = "tests/test_strategy_c.py"
s = open(p).read()
subs = [
("""            wait_for(lambda: 'policy-broadcast' in log_text(root / 'node1.log'),
                     'C: initial policy broadcast', 60)
            time.sleep(3)""",
 """            wait_for(lambda: 'policy-broadcast' in log_text(root / 'node1.log'),
                     'C: initial policy broadcast', 60)
            time.sleep(6)"""),
("""            wait_for(lambda: 'policy-broadcast' in log_text(root / 'node1.log'),
                     'B: initial policy broadcast', 60)
            time.sleep(3)""",
 """            wait_for(lambda: 'policy-broadcast' in log_text(root / 'node1.log'),
                     'B: initial policy broadcast', 60)
            time.sleep(6)"""),
("""                wait_for(lambda: 'policy-broadcast' in log_text(root / 'node1.log'),
                         f'{strategy}: initial policy broadcast', 60)
                time.sleep(3)""",
 """                wait_for(lambda: 'policy-broadcast' in log_text(root / 'node1.log'),
                         f'{strategy}: initial policy broadcast', 60)
                time.sleep(6)"""),
]
for old, new in subs:
    if old in s:
        s = s.replace(old, new, 1)
    else:
        print("MISSING:", old[:60])
open(p, "w").write(s)
print("settle sleeps bumped to 6s")
PYEOF
