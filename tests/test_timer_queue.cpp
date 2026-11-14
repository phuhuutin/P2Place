
#include <gtest/gtest.h>

#include "net/timer_queue.hpp"
#include "net/event_loop.hpp"
#include <chrono>
#include <vector>

using namespace std::chrono_literals;
using p2place::net::TimerQueue;

TEST(Lab04Timer, RunsInDeadlineOrder) {
    TimerQueue timers;

    const auto start =
        TimerQueue::Clock::time_point{};

    std::vector<int> results;

    timers.schedule(start + 300ms, [&] {
        results.push_back(3);
    });

    timers.schedule(start + 100ms, [&] {
        results.push_back(1);
    });

    timers.schedule(start + 200ms, [&] {
        results.push_back(2);
    });

    timers.pop_due(start + 250ms);

    EXPECT_EQ(
        results,
        (std::vector<int>{1, 2})
    );

    timers.pop_due(start + 300ms);

    EXPECT_EQ(
        results,
        (std::vector<int>{1, 2, 3})
    );
}

TEST(Lab04Timer, CancelPreventsExecution) {
    TimerQueue timers;

    const auto start =
        TimerQueue::Clock::time_point{};

    bool executed = false;

    const auto token = timers.schedule(
        start + 100ms,
        [&] {
            executed = true;
        }
    );

    EXPECT_TRUE(timers.cancel(token));
    EXPECT_FALSE(timers.cancel(token));

    timers.pop_due(start + 500ms);

    EXPECT_FALSE(executed);
}

TEST(Lab04Timer, TimeoutCalculation) {
    TimerQueue timers;

    const auto start =
        TimerQueue::Clock::time_point{};

    EXPECT_EQ(
        timers.next_timeout_ms(start),
        -1
    );

    timers.schedule(start + 100ms, [] {});

    EXPECT_EQ(
        timers.next_timeout_ms(start),
        100
    );

    EXPECT_EQ(
        timers.next_timeout_ms(start + 60ms),
        40
    );

    EXPECT_EQ(
        timers.next_timeout_ms(start + 150ms),
        0
    );

    timers.pop_due(start + 150ms);

    EXPECT_EQ(
        timers.next_timeout_ms(start + 150ms),
        -1
    );
}

TEST(Lab04Timer, CallbackCanScheduleAnotherTimer) {
    TimerQueue timers;

    const auto start =
        TimerQueue::Clock::time_point{};

    int count = 0;

    timers.schedule(start + 100ms, [&] {
        ++count;

        timers.schedule(start + 200ms, [&] {
            ++count;
        });
    });

    timers.pop_due(start + 100ms);
    EXPECT_EQ(count, 1);

    timers.pop_due(start + 200ms);
    EXPECT_EQ(count, 2);
}

TEST(Lab04Timer, EventLoopExecutesTimer) {
    p2place::net::EventLoop loop;

    bool executed = false;

    loop.schedule_timer(
        TimerQueue::Clock::now() + 20ms,
        [&] {
            executed = true;
            loop.stop();
        }
    );

    // Safety timeout in case the timer doesn't fire.
    // The regular on_tick callback runs every 500ms.
    loop.run([&] {
        loop.stop();
    });

    EXPECT_TRUE(executed);
}
