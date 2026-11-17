#!/bin/bash
cd ~/projects/p2place
echo '=== git identity:'
git status --short | head -n 20
git rev-parse HEAD
git log -1 --oneline
sha256sum build/bin/p2place-peer experiments/run_experiment.py
echo
echo '=== existing deep/convergence tests:'
ls tests/ | grep -iE 'deep|converg' || echo '(none)'
echo
echo '=== backfill / anti-entropy / version sites:'
rg -n 'backfill|anti_entropy_step|region.version\(\)|merge_region' src include | grep -v Zone | head -n 25
echo
echo '=== Region::version implementation:'
grep -n -A8 'Region::version' src/core/region.cpp
