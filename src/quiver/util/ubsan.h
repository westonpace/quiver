// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

// Contains utilities for making UBSan happy.

#pragma once

#include <cstring>
#include <memory>
#include <type_traits>

#include "quiver/util/macros.h"

namespace quiver::util {

namespace internal {

constexpr uint8_t kNonNullFiller = 0;

}  // namespace internal

/// \brief Returns maybe_null if not null or a non-null pointer to an arbitrary memory
/// that shouldn't be dereferenced.
///
/// Memset/Memcpy are undefined when a nullptr is passed as an argument use this utility
/// method to wrap locations where this could happen.
///
/// Note: Flatbuffers has UBSan warnings if a zero length vector is passed.
/// https://github.com/google/flatbuffers/pull/5355 is trying to resolve
/// them.
template <typename T>
inline T* MakeNonNull(T* maybe_null = nullptr) {
  if (QUIVER_LIKELY(maybe_null != nullptr)) {
    return maybe_null;
  }

  return const_cast<T*>(reinterpret_cast<const T*>(&internal::kNonNullFiller));
}

template <typename T>
inline std::enable_if_t<std::is_trivially_copyable_v<T>, T> SafeLoadAs(
    const uint8_t* unaligned) {
  std::remove_const_t<T> ret;
  std::memcpy(&ret, unaligned, sizeof(T));
  return ret;
}

template <typename T>
inline std::enable_if_t<std::is_trivially_copyable_v<T>, T> SafeLoad(const T* unaligned) {
  std::remove_const_t<T> ret;
  std::memcpy(&ret, unaligned, sizeof(T));
  return ret;
}

template <typename U, typename T>
inline std::enable_if_t<std::is_trivially_copyable_v<T> &&
                            std::is_trivially_copyable_v<U> && sizeof(T) == sizeof(U),
                        U>
SafeCopy(T value) {
  std::remove_const_t<U> ret;
  std::memcpy(&ret, &value, sizeof(T));
  return ret;
}

template <typename T>
inline std::enable_if_t<std::is_trivially_copyable_v<T>, void> SafeStore(void* unaligned,
                                                                         T value) {
  std::memcpy(unaligned, &value, sizeof(T));
}

}  // namespace quiver::util
