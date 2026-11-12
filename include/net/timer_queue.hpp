
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>

namespace p2place::net {

class TimerQueue final {
public:
    using Clock = std::chrono::steady_clock;
    using Token = std::uint64_t;

    // Register a callback at an absolute deadline.
    // Returns a unique token for cancellation.
    Token schedule(
        Clock::time_point due,
        std::function<void()> fn
    );

    // Cancel a timer if it is still pending.
    bool cancel(Token id);

    // Execute callbacks due at or before now.
    void pop_due(Clock::time_point now);

    // -1: no timers
    //  0: timer due or overdue
    // >0: milliseconds until earliest deadline
    int next_timeout_ms(Clock::time_point now) const;

private:
    struct Entry {
        Clock::time_point due;
        std::function<void()> fn;
    };

    // Token -> scheduled callback
    std::map<Token, Entry> jobs_;

    Token next_token_ = 1;
};

} // namespace p2place::net
