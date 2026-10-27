
cmake_minimum_required(VERSION 3.20)
project(P2Place VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

# Lab 01 source files only
add_library(p2place STATIC
    src/net/socket.cpp
    src/net/protocol.cpp
    src/util/logger.cpp
)

target_include_directories(p2place PUBLIC
    "${PROJECT_SOURCE_DIR}/include"
)

target_compile_options(p2place PRIVATE
    -Wall -Wextra -Wpedantic
)

target_link_libraries(p2place PUBLIC Threads::Threads)

# Lab 01 tests only
include(CTest)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)

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
endif()
