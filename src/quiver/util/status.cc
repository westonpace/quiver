// SPDX-License-Identifier: Apache-2.0

#include "quiver/util/status.h"

#include <cstdlib>
#include <iostream>
#include <sstream>

#include "quiver/util/logging_p.h"

namespace quiver {

Status::Status(StatusCode code, const std::string& msg)
    : Status::Status(code, msg, nullptr) {}

Status::Status(StatusCode code, std::string msg, std::shared_ptr<StatusDetail> detail) {
  QUIVER_CHECK_NE(code, StatusCode::OK) << "Cannot construct ok status with message";
  state_ = new State;
  state_->code = code;
  state_->msg = std::move(msg);
  if (detail != nullptr) {
    state_->detail = std::move(detail);
  }
}

void Status::CopyFrom(const Status& other) {
  delete state_;
  if (other.state_ == nullptr) {
    state_ = nullptr;
  } else {
    state_ = new State(*other.state_);
  }
}

std::string Status::CodeAsString() const {
  if (state_ == nullptr) {
    return "OK";
  }
  return CodeAsString(code());
}

std::string Status::CodeAsString(StatusCode code) {
  const char* type;
  switch (code) {
    case StatusCode::OK:
      type = "OK";
      break;
    case StatusCode::OutOfMemory:
      type = "Out of memory";
      break;
    case StatusCode::KeyError:
      type = "Key error";
      break;
    case StatusCode::TypeError:
      type = "Type error";
      break;
    case StatusCode::Invalid:
      type = "Invalid";
      break;
    case StatusCode::Cancelled:
      type = "Cancelled";
      break;
    case StatusCode::IOError:
      type = "IOError";
      break;
    case StatusCode::CapacityError:
      type = "Capacity error";
      break;
    case StatusCode::IndexError:
      type = "Index error";
      break;
    case StatusCode::UnknownError:
      type = "Unknown error";
      break;
    case StatusCode::NotImplemented:
      type = "NotImplemented";
      break;
    case StatusCode::SerializationError:
      type = "Serialization error";
      break;
    default:
      type = "Unknown";
      break;
  }
  return {type};
}

std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (state_ == nullptr) {
    return result;
  }
  result += ": ";
  result += state_->msg;
  if (state_->detail != nullptr) {
    result += ". Detail: ";
    result += state_->detail->ToString();
  }

  return result;
}

void Status::Abort() const { Abort(std::string()); }

void Status::AbortNotOk() const {
  if (!ok()) {
    Abort();
  }
}

void Status::Abort(const std::string& message) const {
  std::cerr << "-- Arrow Fatal Error --\n";
  if (!message.empty()) {
    std::cerr << message << "\n";
  }
  std::cerr << ToString() << std::endl;
  std::abort();
}

void Status::Warn() const { QUIVER_LOG(kWarning) << ToString(); }

void Status::Warn(const std::string& message) const {
  QUIVER_LOG(kWarning) << message << ": " << ToString();
}

void Status::AddContextLine(const char* filename, int line, const char* expr) {
  QUIVER_CHECK(!ok()) << "Cannot add context line to ok status";
  std::stringstream sstream;
  sstream << "\n" << filename << ":" << line << "  " << expr;
  state_->msg += sstream.str();
}

}  // namespace quiver
