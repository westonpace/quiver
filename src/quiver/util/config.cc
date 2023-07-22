#include "quiver/util/config.h"

#include <spdlog/spdlog.h>

namespace quiver::util::config {

bool IsDebug() {
#ifdef NDEBUG
  return false;
#else
  return true;
#endif
}

void SetLogLevel(LogLevel log_level) {
  switch (log_level) {
    case LogLevel::kTrace:
      spdlog::set_level(spdlog::level::trace);
      break;
    case LogLevel::kDebug:
      spdlog::set_level(spdlog::level::debug);
      break;
    case LogLevel::kInfo:
      spdlog::set_level(spdlog::level::info);
      break;
    case LogLevel::kWarn:
      spdlog::set_level(spdlog::level::warn);
      break;
  }
}

}  // namespace quiver::util::config
