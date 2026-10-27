#pragma once
#include <string_view>
namespace p2place::util {
enum class LogLevel { Debug, Info, Warning, Error };
class Logger final {
public:
  static void log(LogLevel level, std::string_view message);
};
} // namespace p2place::util
