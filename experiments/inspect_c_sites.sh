#!/bin/bash
cd ~/projects/p2place || exit 1
echo "=== payloads.hpp encoders:"
grep -n "put16\|put32\|put64\|read16\|read32\|read64" include/messages/payloads.hpp | head -10
echo
echo "=== tick loop head (peer_node.cpp 800-830):"
sed -n "800,830p" src/peer/peer_node.cpp
echo
echo "=== register_handlers / send_control decls:"
grep -n "register_handlers\|send_control" src/peer/peer_node.cpp include/peer/peer_node.hpp | head -10
echo
echo "=== PolicyConfig struct:"
grep -n -B2 -A14 "struct PolicyConfig" include/core/replication_policy.hpp
echo
echo "=== process_started_ member:"
grep -n "process_started_" include/peer/peer_node.hpp | head -3
