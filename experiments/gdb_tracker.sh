#!/bin/bash
cd ~/projects/p2place
gdb -batch \
  -ex "break p2place::core::RegionWriteTracker::record_put_attempt" \
  -ex run \
  -ex "p bucket_count_" \
  -ex "p window_" \
  ./build/bin/p2place-test-write-tracker 2>&1 | grep -E "^\\\$[0-9]" | head -4
