#include "quiver/util/config.h"

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace quiver::util::config {

bool IsDebug() {
#ifdef NDEBUG
  return false;
#else
  return true;
#endif
}

bool HasTraceLogging() {
#if SPDLOG_ACTIVE_LEVEL <= SPDLOG_LEVEL_TRACE
  return true;
#else
  return false;
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

void UseMultithreadedLogger() {
  spdlog::set_default_logger(spdlog::stdout_color_mt("quiver"));
  spdlog::flush_on(spdlog::level::debug);
}

}  // namespace quiver::util::config
