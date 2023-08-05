// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow, Kudu, TensorFlow

#pragma once

#include <memory>
#include <string>
#include <utility>

#include "quiver/util/compare.h"
#include "quiver/util/macros.h"
#include "quiver/util/string_builder.h"
#include "quiver/util/to_string.h"

#define QUIVER_RETURN_IF_(condition, status, expr)  \
  do {                                              \
    if (QUIVER_UNLIKELY(condition)) {               \
      ::quiver::Status _st = (status);              \
      _st.AddContextLine(__FILE__, __LINE__, expr); \
      return _st;                                   \
    }                                               \
  } while (0)

#define QUIVER_RETURN_IF(condition, status) \
  QUIVER_RETURN_IF_(condition, status, QUIVER_STRINGIFY(status))

/// \brief Propagate any non-successful Status to the caller
#define QUIVER_RETURN_NOT_OK(status)                                          \
  do {                                                                        \
    const ::quiver::Status __s = ::quiver::internal::GenericToStatus(status); \
    QUIVER_RETURN_IF_(!__s.ok(), __s, QUIVER_STRINGIFY(status));              \
  } while (false)

namespace quiver {

enum class StatusCode : char {
  OK = 0,
  OutOfMemory = 1,
  KeyError = 2,
  TypeError = 3,
  Invalid = 4,
  IOError = 5,
  CapacityError = 6,
  IndexError = 7,
  Cancelled = 8,
  UnknownError = 9,
  NotImplemented = 10,
  SerializationError = 11,
};

/// \brief An opaque class that allows subsystems to retain
/// additional information inside the Status.
class StatusDetail {
 public:
  virtual ~StatusDetail() = default;
  /// \brief Return a unique id for the type of the StatusDetail
  /// (effectively a poor man's substitute for RTTI).
  [[nodiscard]] virtual const char* type_id() const = 0;
  /// \brief Produce a human-readable description of this status.
  [[nodiscard]] virtual std::string ToString() const = 0;

  bool operator==(const StatusDetail& other) const noexcept {
    return std::string(type_id()) == other.type_id() && ToString() == other.ToString();
  }
};

/// \brief Status outcome object (success or error)
///
/// The Status object is an object holding the outcome of an operation.
/// The outcome is represented as a StatusCode, either success
/// (StatusCode::OK) or an error (any other of the StatusCode enumeration values).
///
/// Additionally, if an error occurred, a specific error message is generally
/// attached.
class [[nodiscard]] Status : public util::EqualityComparable<Status>,
                             public util::ToStringOstreamable<Status> {
 public:
  // Create a success status.
  constexpr Status() noexcept : state_(nullptr) {}
  ~Status() noexcept {
    if (state_ != nullptr) [[unlikely]] {
      DeleteState();
    }
  }

  Status(StatusCode code, const std::string& msg);
  /// \brief Pluggable constructor for use by sub-systems.  detail cannot be null.
  Status(StatusCode code, std::string msg, std::shared_ptr<StatusDetail> detail);

  // Copy the specified status.
  inline Status(const Status& other);
  inline Status& operator=(const Status& other);

  // Move the specified status.
  inline Status(Status&& other) noexcept;
  inline Status& operator=(Status&& other) noexcept;

  [[nodiscard]] inline bool Equals(const Status& other) const;

  // AND the statuses.
  inline Status operator&(const Status& other) const noexcept;
  inline Status operator&(Status&& other) const noexcept;
  inline Status& operator&=(const Status& other) noexcept;
  inline Status& operator&=(Status&& other) noexcept;

  /// Return a success status
  static Status OK() { return {}; }

  template <typename... Args>
  static Status FromArgs(StatusCode code, Args&&... args) {
    return Status(code, util::StringBuilder(std::forward<Args>(args)...));
  }

  template <typename... Args>
  static Status FromDetailAndArgs(StatusCode code, std::shared_ptr<StatusDetail> detail,
                                  Args&&... args) {
    return Status(code, util::StringBuilder(std::forward<Args>(args)...),
                  std::move(detail));
  }

  /// Return an error status for out-of-memory conditions
  template <typename... Args>
  static Status OutOfMemory(Args&&... args) {
    return Status::FromArgs(StatusCode::OutOfMemory, std::forward<Args>(args)...);
  }

  /// Return an error status for failed key lookups (e.g. column name in a table)
  template <typename... Args>
  static Status KeyError(Args&&... args) {
    return Status::FromArgs(StatusCode::KeyError, std::forward<Args>(args)...);
  }

  /// Return an error status for type errors (such as mismatching data types)
  template <typename... Args>
  static Status TypeError(Args&&... args) {
    return Status::FromArgs(StatusCode::TypeError, std::forward<Args>(args)...);
  }

  /// Return an error status for unknown errors
  template <typename... Args>
  static Status UnknownError(Args&&... args) {
    return Status::FromArgs(StatusCode::UnknownError, std::forward<Args>(args)...);
  }

  /// Return an error status when an operation or a combination of operation and
  /// data types is unimplemented
  template <typename... Args>
  static Status NotImplemented(Args&&... args) {
    return Status::FromArgs(StatusCode::NotImplemented, std::forward<Args>(args)...);
  }

  /// Return an error status for invalid data (for example a string that fails parsing)
  template <typename... Args>
  static Status Invalid(Args&&... args) {
    return Status::FromArgs(StatusCode::Invalid, std::forward<Args>(args)...);
  }

  /// Return an error status for cancelled operation
  template <typename... Args>
  static Status Cancelled(Args&&... args) {
    return Status::FromArgs(StatusCode::Cancelled, std::forward<Args>(args)...);
  }

  /// Return an error status when an index is out of bounds
  template <typename... Args>
  static Status IndexError(Args&&... args) {
    return Status::FromArgs(StatusCode::IndexError, std::forward<Args>(args)...);
  }

  /// Return an error status when a container's capacity would exceed its limits
  template <typename... Args>
  static Status CapacityError(Args&&... args) {
    return Status::FromArgs(StatusCode::CapacityError, std::forward<Args>(args)...);
  }

  /// Return an error status when some IO-related operation failed
  template <typename... Args>
  static Status IOError(Args&&... args) {
    return Status::FromArgs(StatusCode::IOError, std::forward<Args>(args)...);
  }

  /// Return an error status when some (de)serialization operation failed
  template <typename... Args>
  static Status SerializationError(Args&&... args) {
    return Status::FromArgs(StatusCode::SerializationError, std::forward<Args>(args)...);
  }

  /// Return true iff the status indicates success.
  [[nodiscard]] constexpr bool ok() const { return (state_ == nullptr); }

  /// Return true iff the status indicates an out-of-memory error.
  [[nodiscard]] constexpr bool IsOutOfMemory() const {
    return code() == StatusCode::OutOfMemory;
  }
  /// Return true iff the status indicates a key lookup error.
  [[nodiscard]] constexpr bool IsKeyError() const {
    return code() == StatusCode::KeyError;
  }
  /// Return true iff the status indicates invalid data.
  [[nodiscard]] constexpr bool IsInvalid() const { return code() == StatusCode::Invalid; }
  /// Return true iff the status indicates a cancelled operation.
  [[nodiscard]] constexpr bool IsCancelled() const {
    return code() == StatusCode::Cancelled;
  }
  /// Return true iff the status indicates an IO-related failure.
  [[nodiscard]] constexpr bool IsIOError() const { return code() == StatusCode::IOError; }
  /// Return true iff the status indicates a container reaching capacity limits.
  [[nodiscard]] constexpr bool IsCapacityError() const {
    return code() == StatusCode::CapacityError;
  }
  /// Return true iff the status indicates an out of bounds index.
  [[nodiscard]] constexpr bool IsIndexError() const {
    return code() == StatusCode::IndexError;
  }
  /// Return true iff the status indicates a type error.
  [[nodiscard]] constexpr bool IsTypeError() const {
    return code() == StatusCode::TypeError;
  }
  /// Return true iff the status indicates an unknown error.
  [[nodiscard]] constexpr bool IsUnknownError() const {
    return code() == StatusCode::UnknownError;
  }
  /// Return true iff the status indicates an unimplemented operation.
  [[nodiscard]] constexpr bool IsNotImplemented() const {
    return code() == StatusCode::NotImplemented;
  }
  /// Return true iff the status indicates a (de)serialization failure
  [[nodiscard]] constexpr bool IsSerializationError() const {
    return code() == StatusCode::SerializationError;
  }

  /// \brief Return a string representation of this status suitable for printing.
  ///
  /// The string "OK" is returned for success.
  [[nodiscard]] std::string ToString() const;

  /// \brief Return a string representation of the status code, without the message
  /// text or POSIX code information.
  [[nodiscard]] std::string CodeAsString() const;
  static std::string CodeAsString(StatusCode code);

  /// \brief Return the StatusCode value attached to this status.
  [[nodiscard]] constexpr StatusCode code() const {
    return ok() ? StatusCode::OK : state_->code;
  }

  /// \brief Return the specific error message attached to this status.
  [[nodiscard]] const std::string& message() const {
    static const std::string no_message;
    return ok() ? no_message : state_->msg;
  }

  /// \brief Return the status detail attached to this message.
  [[nodiscard]] const std::shared_ptr<StatusDetail>& detail() const {
    static const std::shared_ptr<StatusDetail> no_detail = nullptr;
    return (state_ != nullptr) ? state_->detail : no_detail;
  }

  /// \brief Return a new Status copying the existing status, but
  /// updating with the existing detail.
  Status WithDetail(std::shared_ptr<StatusDetail> new_detail) const {
    return {code(), message(), std::move(new_detail)};
  }

  /// \brief Return a new Status with changed message, copying the
  /// existing status code and detail.
  template <typename... Args>
  Status WithMessage(Args&&... args) const {
    return FromArgs(code(), std::forward<Args>(args)...).WithDetail(detail());
  }

  void Warn() const;
  void Warn(const std::string& message) const;

  [[noreturn]] void Abort() const;
  [[noreturn]] void Abort(const std::string& message) const;
  void AbortNotOk() const;

  void AddContextLine(const char* filename, int line, const char* expr);

 private:
  struct State {
    StatusCode code;
    std::string msg;
    std::shared_ptr<StatusDetail> detail;
  };
  // OK status has a `nullptr` state_.  Otherwise, `state_` points to
  // a `State` structure containing the error code and message(s)
  State* state_;

  void DeleteState() {
    delete state_;
    state_ = nullptr;
  }
  void CopyFrom(const Status& other);
  inline void MoveFrom(Status& other);
};

void Status::MoveFrom(Status& other) {
  delete state_;
  state_ = other.state_;
  other.state_ = nullptr;
}

Status::Status(const Status& other)
    : state_((other.state_ == nullptr) ? nullptr : new State(*other.state_)) {}

Status& Status::operator=(const Status& other) {
  // The following condition catches both aliasing (when this == &other),
  // and the common case where both other and *this are ok.
  if (this == &other) {
    return *this;
  }
  if (state_ != other.state_) {
    CopyFrom(other);
  }
  return *this;
}

Status::Status(Status&& other) noexcept : state_(other.state_) { other.state_ = nullptr; }

Status& Status::operator=(Status&& other) noexcept {
  MoveFrom(other);
  return *this;
}

bool Status::Equals(const Status& other) const {
  if (state_ == other.state_) {
    return true;
  }

  if (ok() || other.ok()) {
    return false;
  }

  if (detail() != other.detail()) {
    if ((detail() && !other.detail()) || (!detail() && other.detail())) {
      return false;
    }
    return *detail() == *other.detail();
  }

  return code() == other.code() && message() == other.message();
}

Status Status::operator&(const Status& other) const noexcept {
  if (ok()) {
    return other;
  }
  return *this;
}

Status Status::operator&(Status&& other) const noexcept {
  if (ok()) {
    return std::move(other);
  }
  return *this;
}

Status& Status::operator&=(const Status& other) noexcept {
  if (ok() && !other.ok()) {
    CopyFrom(other);
  }
  return *this;
}

Status& Status::operator&=(Status&& other) noexcept {
  if (ok() && !other.ok()) {
    MoveFrom(other);
  }
  return *this;
}

namespace internal {

inline const Status& GenericToStatus(const Status& status) { return status; }
inline Status GenericToStatus(Status&& status) { return std::move(status); }

}  // namespace internal

}  // namespace quiver
