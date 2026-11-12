
#include "net/timer_queue.hpp"

#include <algorithm>
#include <climits>
#include <stdexcept>
#include <utility>

namespace p2place::net {

// --------------------------------------------------
// L4.1: Schedule a callback
// --------------------------------------------------
TimerQueue::Token TimerQueue::schedule(
    Clock::time_point due,
    std::function<void()> fn
) {
    if (!fn) {
        throw std::invalid_argument(
            "TimerQueue: callback must not be empty"
        );
    }

    if (next_token_ == 0) {
        throw std::overflow_error(
            "TimerQueue: token space exhausted"
        );
    }

    const Token token = next_token_++;

    jobs_.emplace(
        token,
        Entry{
            due,
            std::move(fn)
        }
    );

    return token;
}

// --------------------------------------------------
// L4.2: Cancel a pending callback
// --------------------------------------------------
bool TimerQueue::cancel(Token id) {
    return jobs_.erase(id) != 0;
}

// --------------------------------------------------
// L4.5: Execute expired callbacks
// --------------------------------------------------
void TimerQueue::pop_due(
    Clock::time_point now
) {
    while (true) {
        auto earliest = jobs_.end();

        // Find the earliest timer that is due.
        for (auto it = jobs_.begin();
             it != jobs_.end();
             ++it) {

            if (it->second.due > now) {
                continue;
            }

            if (earliest == jobs_.end() ||
                it->second.due < earliest->second.due ||
                (it->second.due == earliest->second.due &&
                 it->first < earliest->first)) {

                earliest = it;
            }
        }

        // No more callbacks are due.
        if (earliest == jobs_.end()) {
            break;
        }

        // Move the callback out before executing.
        auto fn = std::move(earliest->second.fn);

        // Remove the timer before calling it.
        jobs_.erase(earliest);

        // This callback may schedule or cancel timers.
        fn();
    }
}

// --------------------------------------------------
// L4.6: Calculate next epoll timeout
// --------------------------------------------------
int TimerQueue::next_timeout_ms(
    Clock::time_point now
) const {
    if (jobs_.empty()) {
        return -1;
    }

    auto earliest = jobs_.begin()->second.due;

    for (const auto& [token, job] : jobs_) {
        (void)token;
        earliest = std::min(earliest, job.due);
    }

    if (earliest <= now) {
        return 0;
    }

    // Round up so epoll does not wake before
    // the requested deadline due to truncation.
    const auto remaining = earliest - now;

    const auto whole_ms =
        std::chrono::duration_cast<
            std::chrono::milliseconds
        >(remaining);

    if (whole_ms.count() >= INT_MAX) {
        return INT_MAX;
    }

    int timeout = static_cast<int>(
        whole_ms.count()
    );

    if (whole_ms < remaining) {
        ++timeout;
    }

    return timeout;
}

} // namespace p2place::net
