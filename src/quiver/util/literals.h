#pragma once

#include <cassert>
#include <cstdint>
#include <span>

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

struct BitsLiteral {
  std::span<uint8_t> bytes;
  std::size_t num_bits;
};

constexpr int64_t kMaxBitsLiteralSize = 128;
constexpr BitsLiteral operator""_bits(const char* value, std::size_t len) {
  std::array<uint8_t, kMaxBitsLiteralSize> scratch;

  uint8_t* byte_itr = scratch.data();
  uint8_t mask = 1;
  std::size_t num_bits = 0;
  std::size_t num_bytes = 0;
  for (std::size_t idx = 0; idx < len; idx++) {
    if (value[idx] == '0' || value[idx] == '1') {
      if (mask == 1) {
        num_bytes++;
      }
      if (value[idx] == '1') {
        *byte_itr |= mask;
      }
      mask <<= 1;
      if (mask == 0) {
        mask = 1;
        byte_itr++;
      }
      num_bits++;
    }
  }

  return {{scratch.data(), num_bytes}, num_bits};
}

}  // namespace literals
}  // namespace quiver::util
