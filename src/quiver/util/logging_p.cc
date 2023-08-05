// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#include "quiver/util/logging_p.h"

#include <cstdlib>
#include <iostream>
#include <memory>

namespace quiver::util {

// This code is adapted from
// https://github.com/ray-project/ray/blob/master/src/ray/util/logging.cc.

// This is the default implementation of arrow log,
// which is independent of any libs.
class CerrLog {
 public:
  explicit CerrLog(QuiverLogLevel severity) : severity_(severity) {}

  virtual ~CerrLog() {
    if (has_logged_) {
      std::cerr << std::endl;
    }
    if (severity_ == QuiverLogLevel::kFatal) {
      std::abort();
    }
  }

  std::ostream& Stream() {
    has_logged_ = true;
    return std::cerr;
  }

  template <class T>
  CerrLog& operator<<(const T& value) {
    if (severity_ != QuiverLogLevel::kDebug) {
      has_logged_ = true;
      std::cerr << value;
    }
    return *this;
  }

 private:
  const QuiverLogLevel severity_;
  bool has_logged_ = false;
};

using LoggingProvider = CerrLog;

QuiverLogLevel QuiverLog::severity_threshold_ = QuiverLogLevel::kInfo;

void QuiverLog::StartQuiverLog(const std::string& app_name,
                               QuiverLogLevel severity_threshold) {
  severity_threshold_ = severity_threshold;
  // In InitGoogleLogging, it simply keeps the pointer.
  // We need to make sure the app name passed to InitGoogleLogging exist.
  // We should avoid using static string is a dynamic lib.
  static std::unique_ptr<std::string> app_name_;
  app_name_ = std::make_unique<std::string>(app_name);
}

bool QuiverLog::IsLevelEnabled(QuiverLogLevel log_level) {
  return log_level >= severity_threshold_;
}

QuiverLog::QuiverLog(const char* file_name, int line_number, QuiverLogLevel severity)
    // glog does not have DEBUG level, we can handle it using is_enabled_.
    : is_enabled_(severity >= severity_threshold_) {
  auto* logging_provider = new CerrLog(severity);
  *logging_provider << file_name << ":" << line_number << ": ";
  logging_provider_ = logging_provider;
}

std::ostream& QuiverLog::Stream() {
  auto* logging_provider = reinterpret_cast<LoggingProvider*>(logging_provider_);
  return logging_provider->Stream();
}

bool QuiverLog::IsEnabled() const { return is_enabled_; }

QuiverLog::~QuiverLog() {
  if (logging_provider_ != nullptr) {
    delete reinterpret_cast<LoggingProvider*>(logging_provider_);
    logging_provider_ = nullptr;
  }
}

}  // namespace quiver::util
