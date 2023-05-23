// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#pragma once

#include <variant>

#include "quiver/util/logging_p.h"
#include "quiver/util/status.h"

namespace quiver::util {

template <typename T, typename... Types>
Status get_or_raise(const std::variant<Types...>* var, const T* out) {
  T* maybe_val = std::get_if<T>(var);
  if (maybe_val == nullptr) {
    return Status::UnknownError("Expected to be able to get variant of type ",
                                typeid(T).name());
  }
  *out = *maybe_val;
  return Status::OK();
}

template <typename T, typename... Types>
Status get_or_raise(std::variant<Types...>* var, T* out) {
  T* maybe_val = std::get_if<T>(var);
  if (maybe_val == nullptr) {
    return Status::UnknownError("Expected to be able to get variant of type ",
                                typeid(T).name());
  }
  *out = *maybe_val;
  return Status::OK();
}

}  // namespace quiver::util
