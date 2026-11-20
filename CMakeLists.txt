
cmake_minimum_required(VERSION 3.20)
project(P2Place VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)
find_package(nlohmann_json 3.2.0 REQUIRED)

add_library(p2place STATIC
    # Lab 01
    src/net/socket.cpp
    src/net/protocol.cpp
    src/util/logger.cpp

    # Lab 02 - Stage 1
    src/net/protocol_codec.cpp
    src/net/event_loop.cpp
    src/net/tcp_client.cpp
    src/net/tcp_connection.cpp
    src/net/tcp_server.cpp
    src/core/peer_table.cpp
    src/peer/config.cpp
    src/peer/peer_node.cpp
    src/peer/pixel_event.cpp
    src/peer/placement_runtime.cpp
    src/peer/ring_election.cpp
    src/messages/message.cpp
    # Lab 03
    src/core/region.cpp
    src/core/region_store.cpp
    src/ui/websocket_server.cpp
    src/net/websocket_codec.cpp
    src/util/sha1.cpp
    src/util/base64.cpp

    #lab 04
    src/net/timer_queue.cpp
    src/messages/payloads.cpp
    src/core/balanced_placement.cpp
    src/core/automatic_placement.cpp
    src/messages/placement_protocol.cpp
    src/core/consistent_hash_ring.cpp
    src/core/membership_gossip.cpp
    src/replication/replication_manager.cpp
    src/core/failure_detector.cpp
    src/replication/churn_replication_strategy.cpp
    # Strategy B: reliability measurement + shared replication policy
    src/core/peer_reliability.cpp
    src/core/replication_policy.cpp
    # Strategy C: per-region client write-frequency tracking + report aggregation
    src/core/region_write_tracker.cpp
    src/peer/metrics.cpp
)

target_include_directories(p2place PUBLIC
    "${PROJECT_SOURCE_DIR}/include"
)

target_compile_options(p2place PRIVATE
    -Wall -Wextra -Wpedantic
)

target_link_libraries(p2place PUBLIC Threads::Threads)
target_link_libraries(p2place PRIVATE
    nlohmann_json::nlohmann_json
)
# Peer CLI executable (labs 02+): run a single node from the command line.
add_executable(p2place-peer
    src/peer/main.cpp
)

target_link_libraries(p2place-peer PRIVATE
    p2place
)

target_compile_options(p2place-peer PRIVATE
    -Wall -Wextra -Wpedantic
)

# Lab 01 tests only
include(CTest)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)    
    # Lab 01
    add_executable(p2place-test-lab01
        tests/test_lab01.cpp
    )

    target_link_libraries(p2place-test-lab01 PRIVATE
        p2place
        GTest::gtest_main
    )

    add_test(
        NAME lab01
        COMMAND p2place-test-lab01
    )

    set_tests_properties(lab01 PROPERTIES
        LABELS "lab01;through-lab01"
    )

    # Lab 02
    add_executable(p2place-test-lab02
        tests/test_lab02.cpp
    )

    target_link_libraries(p2place-test-lab02 PRIVATE
        p2place
        GTest::gtest_main
    )

    add_test(
        NAME lab02
        COMMAND p2place-test-lab02
    )

    set_tests_properties(lab02 PROPERTIES
        LABELS "lab02;through-lab02"
    )

    # Lab 03
    add_executable(p2place-test-lab03
        tests/test_lab03.cpp
    )

    target_link_libraries(p2place-test-lab03 PRIVATE
        p2place
        GTest::gtest_main
    )
    add_test(
        NAME lab03
        COMMAND p2place-test-lab03
    )
    set_tests_properties(lab03 PROPERTIES
        LABELS "lab03;through-lab03"
    )

        # Lab 04 - TimerQueue
    add_executable(p2place-test-timer-queue
        tests/test_timer_queue.cpp
    )

    target_link_libraries(p2place-test-timer-queue PRIVATE
        p2place
        GTest::gtest_main
    )

    add_test(
        NAME timer-queue
        COMMAND p2place-test-timer-queue
    )

    set_tests_properties(timer-queue PROPERTIES
        LABELS "lab04;through-lab04"
    )

    # Lab 04 - Binary payload tests
    add_executable(p2place-test-payloads-lab04
        tests/test_payloads_lab04.cpp
    )

    target_link_libraries(p2place-test-payloads-lab04 PRIVATE
        p2place
        GTest::gtest_main
    )

    add_test(
        NAME payloads-lab04
        COMMAND p2place-test-payloads-lab04
    )

    set_tests_properties(payloads-lab04 PROPERTIES
        LABELS "lab04;through-lab04"
    )

     

    # Lab 04 - NODELIST membership
    add_executable(p2place-test-membership-lab04
        tests/test_membership_lab04.cpp
    )

    target_link_libraries(p2place-test-membership-lab04 PRIVATE
        p2place
        GTest::gtest_main
    )

    add_test(
        NAME membership-lab04
        COMMAND p2place-test-membership-lab04
    )

    set_tests_properties(membership-lab04 PROPERTIES
        LABELS "lab04;through-lab04"
    )


    # Lab 04 - GET/PUT coordinator routing
    add_executable(p2place-test-routing-lab04
        tests/test_routing_lab04.cpp
    )

    target_link_libraries(p2place-test-routing-lab04 PRIVATE
        p2place
        GTest::gtest_main
    )

    add_test(
        NAME routing-lab04
        COMMAND p2place-test-routing-lab04
    )

    set_tests_properties(routing-lab04 PROPERTIES
        LABELS "lab04;through-lab04"
    )
    
endif()

# Standalone assertions work without GoogleTest; CTest registers them here.
add_executable(p2place-test-balanced tests/test_balanced_placement_standalone.cpp)
target_link_libraries(p2place-test-balanced PRIVATE p2place)
add_test(NAME balanced-placement COMMAND p2place-test-balanced)

# Standalone placement + handoff tests, independent of GoogleTest.
add_executable(p2place-test-dynamic-placement
    tests/test_dynamic_placement_standalone.cpp
)
target_link_libraries(p2place-test-dynamic-placement PRIVATE p2place)
add_test(NAME dynamic-placement COMMAND p2place-test-dynamic-placement)
set_tests_properties(dynamic-placement PROPERTIES LABELS "lab04;through-lab04")

# Oldest-admission Ring Election unit tests (no GoogleTest dependency).
add_executable(p2place-test-oldest-member tests/test_oldest_member_priority.cpp)
target_link_libraries(p2place-test-oldest-member PRIVATE p2place)
add_test(NAME oldest-member-priority COMMAND p2place-test-oldest-member)
set_tests_properties(oldest-member-priority PROPERTIES LABELS "lab04;election")

# Lab 05 standalone quorum tests (no GoogleTest dependency)
add_executable(p2place-test-lab05 tests/test_lab05_standalone.cpp)
target_link_libraries(p2place-test-lab05 PRIVATE p2place)
add_test(NAME lab05-quorum COMMAND p2place-test-lab05)
set_tests_properties(lab05-quorum PROPERTIES LABELS "lab05;through-lab05")
add_executable(p2place-test-lab05-three-nodes tests/test_lab05_three_node_sim.cpp)
target_link_libraries(p2place-test-lab05-three-nodes PRIVATE p2place)
add_test(NAME lab05-three-node-simulation COMMAND p2place-test-lab05-three-nodes)
set_tests_properties(lab05-three-node-simulation PROPERTIES LABELS "lab05;through-lab05")

# Lab 06: independent tests without the networking runtime.
add_executable(p2place-test-lab06-core
    tests/test_lab06_core_standalone.cpp)
target_link_libraries(p2place-test-lab06-core PRIVATE p2place)
add_test(NAME lab06-core COMMAND p2place-test-lab06-core)

add_executable(p2place-test-lab06-anti-entropy
    tests/test_lab06_anti_entropy_sim.cpp)
target_link_libraries(p2place-test-lab06-anti-entropy PRIVATE p2place)
add_test(NAME lab06-anti-entropy COMMAND p2place-test-lab06-anti-entropy)

# Backfill false-complete regression: complete_all+exclude_self backfill merge,
# bounded empty-region retries, stale-epoch callback drop.
add_executable(p2place-test-backfill-completeness
    tests/test_backfill_completeness.cpp)
target_link_libraries(p2place-test-backfill-completeness PRIVATE p2place)
add_test(NAME backfill-completeness COMMAND p2place-test-backfill-completeness)
set_tests_properties(backfill-completeness PROPERTIES LABELS "lab06;strategy-b")

# Policy-activation priority: urgent new-target backfill and fairness.
add_executable(p2place-test-backfill-priority
    tests/test_backfill_priority.cpp)
target_link_libraries(p2place-test-backfill-priority PRIVATE p2place)
add_test(NAME backfill-priority COMMAND p2place-test-backfill-priority)
set_tests_properties(backfill-priority PROPERTIES LABELS "strategy-c;lab06")

# Strategy C: write-frequency tracker + multi-gateway delta aggregation.
add_executable(p2place-test-write-tracker
    tests/test_region_write_tracker.cpp)
target_link_libraries(p2place-test-write-tracker PRIVATE p2place)
add_test(NAME write-tracker COMMAND p2place-test-write-tracker)
set_tests_properties(write-tracker PROPERTIES LABELS "strategy-c")

# Strategy C: churn + write-aware policy composition.
add_executable(p2place-test-strategy-c
    tests/test_strategy_c.cpp)
target_link_libraries(p2place-test-strategy-c PRIVATE p2place)
add_test(NAME strategy-c COMMAND p2place-test-strategy-c)
set_tests_properties(strategy-c PROPERTIES LABELS "strategy-c")

# No GTest/JSON needed for event codec unit test.
if(BUILD_TESTING)
  add_executable(p2place-test-pixel-event tests/test_pixel_event_standalone.cpp)
  target_link_libraries(p2place-test-pixel-event PRIVATE p2place)
  add_test(NAME lab06-pixel-event COMMAND p2place-test-pixel-event)
endif()

# Seed-independent cluster joining: referral payload unit test.
add_executable(p2place-test-join-referral tests/test_join_referral.cpp)
target_include_directories(p2place-test-join-referral PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
add_test(NAME join-referral-wire COMMAND p2place-test-join-referral)
set_tests_properties(join-referral-wire PROPERTIES LABELS "lab06;membership")

# Lab 06 regression: handoff sources may be any old replica (not only the
# primary); destination merges keep the newer per-pixel version.
add_executable(p2place-test-handoff-regression
    tests/test_handoff_regression.cpp)
target_link_libraries(p2place-test-handoff-regression PRIVATE p2place)
add_test(NAME handoff-regression COMMAND p2place-test-handoff-regression)
set_tests_properties(handoff-regression PROPERTIES LABELS "lab06;membership")

# Strategy B: rolling-window peer reliability tracker.
add_executable(p2place-test-peer-reliability
    tests/test_peer_reliability.cpp)
target_link_libraries(p2place-test-peer-reliability PRIVATE p2place)
add_test(NAME peer-reliability COMMAND p2place-test-peer-reliability)
set_tests_properties(peer-reliability PROPERTIES LABELS "lab06;strategy-b")

# Strategy B: deterministic policy mapping, cooldown, snapshot validation.
add_executable(p2place-test-policy-b
    tests/test_policy_b.cpp)
target_link_libraries(p2place-test-policy-b PRIVATE p2place)
add_test(NAME policy-b COMMAND p2place-test-policy-b)
set_tests_properties(policy-b PROPERTIES LABELS "lab06;strategy-b")
