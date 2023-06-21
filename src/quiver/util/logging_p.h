// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#pragma once

#include <ostream>
#include <string>

#include "quiver/util/macros.h"

namespace quiver::util {

enum class QuiverLogLevel : int {
  kDebug = -1,
  kInfo = 0,
  kWarning = 1,
  kError = 2,
  kFatal = 3
};

#define QUIVER_LOG_INTERNAL(level) ::quiver::util::QuiverLog(__FILE__, __LINE__, level)
#define QUIVER_LOG(level) QUIVER_LOG_INTERNAL(::quiver::util::QuiverLogLevel::level)

#define QUIVER_IGNORE_EXPR(expr) ((void)(expr))

// NOLINTBEGIN
#define QUIVER_CHECK_OR_LOG(condition, level)                 \
  (condition) ? QUIVER_IGNORE_EXPR(0)                         \
              : ::quiver::util::Voidify() & QUIVER_LOG(level) \
                                                << " Check failed: " #condition " "
// NOLINTEND

#define QUIVER_CHECK(condition) QUIVER_CHECK_OR_LOG(condition, kFatal)

// If 'to_call' returns a bad status, CHECK immediately with a logged message
// of 'msg' followed by the status.
#define QUIVER_CHECK_OK_PREPEND(to_call, msg, level)                 \
  do {                                                               \
    ::quiver::Status _status = (to_call);                            \
    QUIVER_CHECK_OR_LOG(_status.ok(), level)                         \
        << "Operation failed: " << QUIVER_STRINGIFY(to_call) << "\n" \
        << (msg) << ": " << _status.ToString();                      \
  } while (false)

// If the status is bad, CHECK immediately, appending the status to the
// logged message.
#define QUIVER_CHECK_OK(s) QUIVER_CHECK_OK_PREPEND(s, "Bad status", kFatal)

#define QUIVER_CHECK_EQ(val1, val2) QUIVER_CHECK((val1) == (val2))
#define QUIVER_CHECK_NE(val1, val2) QUIVER_CHECK((val1) != (val2))
#define QUIVER_CHECK_LE(val1, val2) QUIVER_CHECK((val1) <= (val2))
#define QUIVER_CHECK_LT(val1, val2) QUIVER_CHECK((val1) < (val2))
#define QUIVER_CHECK_GE(val1, val2) QUIVER_CHECK((val1) >= (val2))
#define QUIVER_CHECK_GT(val1, val2) QUIVER_CHECK((val1) > (val2))

#ifdef NDEBUG
#define QUIVER_DFATAL ::quiver::util::QuiverLogLevel::QUIVER_WARNING

// CAUTION: DCHECK_OK() always evaluates its argument, but other DCHECK*() macros
// only do so in debug mode.

#define QUIVER_DCHECK(condition)               \
  while (false) QUIVER_IGNORE_EXPR(condition); \
  while (false) ::quiver::util::detail::NullLog()
#define QUIVER_DCHECK_OK(s) \
  QUIVER_IGNORE_EXPR(s);    \
  while (false) ::quiver::util::detail::NullLog()
#define QUIVER_DCHECK_EQ(val1, val2)      \
  while (false) QUIVER_IGNORE_EXPR(val1); \
  while (false) QUIVER_IGNORE_EXPR(val2); \
  while (false) ::quiver::util::detail::NullLog()
#define QUIVER_DCHECK_NE(val1, val2)      \
  while (false) QUIVER_IGNORE_EXPR(val1); \
  while (false) QUIVER_IGNORE_EXPR(val2); \
  while (false) ::quiver::util::detail::NullLog()
#define QUIVER_DCHECK_LE(val1, val2)      \
  while (false) QUIVER_IGNORE_EXPR(val1); \
  while (false) QUIVER_IGNORE_EXPR(val2); \
  while (false) ::quiver::util::detail::NullLog()
#define QUIVER_DCHECK_LT(val1, val2)      \
  while (false) QUIVER_IGNORE_EXPR(val1); \
  while (false) QUIVER_IGNORE_EXPR(val2); \
  while (false) ::quiver::util::detail::NullLog()
#define QUIVER_DCHECK_GE(val1, val2)      \
  while (false) QUIVER_IGNORE_EXPR(val1); \
  while (false) QUIVER_IGNORE_EXPR(val2); \
  while (false) ::quiver::util::detail::NullLog()
#define QUIVER_DCHECK_GT(val1, val2)      \
  while (false) QUIVER_IGNORE_EXPR(val1); \
  while (false) QUIVER_IGNORE_EXPR(val2); \
  while (false) ::quiver::util::detail::NullLog()

#else
#define QUIVER_DFATAL ::quiver::util::QuiverLogLevel::QUIVER_FATAL

#define QUIVER_DCHECK QUIVER_CHECK
#define QUIVER_DCHECK_OK QUIVER_CHECK_OK
#define QUIVER_DCHECK_EQ QUIVER_CHECK_EQ
#define QUIVER_DCHECK_NE QUIVER_CHECK_NE
#define QUIVER_DCHECK_LE QUIVER_CHECK_LE
#define QUIVER_DCHECK_LT QUIVER_CHECK_LT
#define QUIVER_DCHECK_GE QUIVER_CHECK_GE
#define QUIVER_DCHECK_GT QUIVER_CHECK_GT

#endif  // NDEBUG

#define DCHECK QUIVER_DCHECK
#define DCHECK_OK QUIVER_DCHECK_OK
#define DCHECK_EQ QUIVER_DCHECK_EQ
#define DCHECK_NE QUIVER_DCHECK_NE
#define DCHECK_LE QUIVER_DCHECK_LE
#define DCHECK_LT QUIVER_DCHECK_LT
#define DCHECK_GE QUIVER_DCHECK_GE
#define DCHECK_GT QUIVER_DCHECK_GT

// This code is adapted from
// https://github.com/ray-project/ray/blob/master/src/ray/util/logging.h.

// To make the logging lib pluggable with other logging libs and make
// the implementation unawared by the user, QuiverLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

// This is also a null log which does not output anything.
class QuiverLogBase {
 public:
  virtual ~QuiverLogBase() = default;

  [[nodiscard]] virtual bool IsEnabled() const { return false; }

  template <typename T>
  QuiverLogBase& operator<<(const T& value) {
    if (IsEnabled()) {
      Stream() << value;
    }
    return *this;
  }

 protected:
  virtual std::ostream& Stream() = 0;
};

class QuiverLog : public QuiverLogBase {
 public:
  QuiverLog(const char* file_name, int line_number, QuiverLogLevel severity);
  ~QuiverLog() override;
  QuiverLog(const QuiverLog&) = delete;
  void operator=(const QuiverLog&) = delete;

  /// Return whether or not current logging instance is enabled.
  ///
  /// \return True if logging is enabled and false otherwise.
  [[nodiscard]] bool IsEnabled() const override;

  /// The init function of arrow log for a program which should be called only once.
  ///
  /// \param app_name The app name which starts the log.
  /// \param severity_threshold Logging threshold for the program.
  static void StartQuiverLog(const std::string& app_name,
                             QuiverLogLevel severity_threshold = QuiverLogLevel::kInfo);

  /// Return whether or not the log level is enabled in current setting.
  ///
  /// \param log_level The input log level to test.
  /// \return True if input log level is not lower than the threshold.
  static bool IsLevelEnabled(QuiverLogLevel log_level);

 private:
  // Hide the implementation of log provider by void *.
  // Otherwise, lib user may define the same macro to use the correct header file.
  void* logging_provider_ = nullptr;
  /// True if log messages should be logged and false if they should be ignored.
  bool is_enabled_;

  static QuiverLogLevel severity_threshold_;

 protected:
  std::ostream& Stream() override;
};

// This class make QUIVER_CHECK compilation pass to change the << operator to void.
// This class is copied from glog.
class Voidify {
 public:
  Voidify() = default;
  // This has to be an operator with a precedence lower than << but
  // higher than ?:
  void operator&([[maybe_unused]] QuiverLogBase& base) {}
};

namespace detail {

/// @brief A helper for the nil log sink.
///
/// Using this helper is analogous to sending log messages to /dev/null:
/// nothing gets logged.
class NullLog {
 public:
  /// The no-op output operator.
  ///
  /// @param [in] value
  ///   The object to send into the nil sink.
  /// @return Reference to the updated object.
  template <class T>
  NullLog& operator<<([[maybe_unused]] const T& value) {
    return *this;
  }
};

}  // namespace detail
}  // namespace quiver::util
