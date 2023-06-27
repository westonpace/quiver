#pragma once

#include <cstdint>

namespace quiver::util {
inline namespace literals {

constexpr int64_t kKiLL = 1024LL;
constexpr int32_t kKi = 1024;
constexpr int64_t kMiLL = kKiLL * kKiLL;
constexpr int32_t kMi = kKi * kKi;

constexpr int64_t operator""_KiLL(unsigned long long value) {
  return kKiLL * static_cast<int64_t>(value);
}
constexpr int32_t operator""_Ki(unsigned long long value) {
  return kKi * static_cast<int32_t>(value);
}
constexpr int64_t operator""_MiLL(unsigned long long value) {
  return kMiLL * static_cast<int64_t>(value);
}
constexpr int32_t operator""_Mi(unsigned long long value) {
  return kMi * static_cast<int32_t>(value);
}

}  // namespace literals
}  // namespace quiver::util
