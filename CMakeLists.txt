
cmake_minimum_required(VERSION 3.20)
project(P2Place VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

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
    src/messages/message.cpp
    # Lab 03
    src/core/region.cpp
    src/core/region_store.cpp
    src/ui/websocket_server.cpp
    src/net/websocket_codec.cpp
    src/util/sha1.cpp
    src/util/base64.cpp
)

target_include_directories(p2place PUBLIC
    "${PROJECT_SOURCE_DIR}/include"
)

target_compile_options(p2place PRIVATE
    -Wall -Wextra -Wpedantic
)

target_link_libraries(p2place PUBLIC Threads::Threads)

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
endif()
