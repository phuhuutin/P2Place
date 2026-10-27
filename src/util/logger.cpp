#include "util/logger.hpp"
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
namespace p2place::util {
void Logger::log(LogLevel level, std::string_view text) {
  static std::mutex mu;
  std::lock_guard lock(mu);
  const char *names[] = {"DEBUG", "INFO", "WARN", "ERROR"};
  const auto now =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::cerr << std::put_time(std::localtime(&now), "%F %T") << " ["
            << names[static_cast<int>(level)] << "] " << text << '\n';
  // TODO(L1.3): Optional: add peer ID prefix and configurable minimum log
  // level.
}
} // namespace p2place::util
