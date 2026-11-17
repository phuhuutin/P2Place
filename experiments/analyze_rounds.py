#!/usr/bin/env python3
"""Print per-round/per-region mismatch tables for an isolated run."""
import collections
import csv
import json
import pathlib
import sys

result = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else
                       'experiments/results/B_deep_isolated_02')
p = result / 'recovery/snapshot_comparisons.csv'
rows = list(csv.DictReader(open(p)))
per_round = collections.defaultdict(lambda: collections.Counter())
per_region = collections.defaultdict(lambda: collections.Counter())
per_reader = collections.defaultdict(lambda: collections.Counter())
for r in rows:
    rd = int(r['round'])
    per_round[rd]['comparisons'] += 1
    if r['status'] != 'correct':
        per_round[rd]['mismatch'] += 1
        per_round[rd][r['status']] += 1
        per_region[r['region']]['mismatch'] += 1
        per_reader[r['reader']]['mismatch'] += 1
for rd in sorted(per_round):
    c = per_round[rd]
    print('round %d: comparisons=%d mismatches=%d absent=%d wrong=%d errors=%d'
          % (rd, c['comparisons'], c['mismatch'], c['absent'],
             c['wrong_color'], c['snapshot_error']))
bad = sorted((-v['mismatch'], k) for k, v in per_region.items() if v['mismatch'])
print('regions with any mismatch (mismatches, region):', bad)
badr = sorted((-v['mismatch'], k) for k, v in per_reader.items() if v['mismatch'])
print('readers with any mismatch (mismatches, reader):', badr)
summ = json.load(open(result / 'recovery/probe_summary.json'))
print('probe_summary:', {k: v for k, v in summ.items() if k != 'note'})
