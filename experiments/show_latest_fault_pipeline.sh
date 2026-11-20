#!/bin/bash
cd ~/projects/p2place || exit 1
latest=$(cd experiments/results/strategy_c_blind && ls -d run_* | sort | tail -1)
echo "latest run: $latest"
python3 experiments/analyze_blind_fault.py "experiments/results/strategy_c_blind/$latest" 2>&1 | head -30
