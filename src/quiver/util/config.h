#pragma once

namespace quiver::util::config {

bool IsDebug();
bool HasTraceLogging();
void UseMultithreadedLogger();

enum class LogLevel { kTrace, kDebug, kInfo, kWarn };
void SetLogLevel(LogLevel level);

}  // namespace quiver::util::config
