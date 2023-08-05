// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#pragma once

#include <array>
#include <cstdint>
#include <type_traits>

#include "quiver/util/logging_p.h"

namespace quiver::bit_util {

//
// Bit-related computations on integer values
//

#if defined(_MSC_VER)
#if defined(_M_AMD64) || defined(_M_X64)
#include <intrin.h>  // IWYU pragma: keep
#include <nmmintrin.h>
#endif

#pragma intrinsic(_BitScanReverse)
#pragma intrinsic(_BitScanForward)
#define QUIVER_POPCOUNT64 __popcnt64
#define QUIVER_POPCOUNT32 __popcnt
#else
#define QUIVER_POPCOUNT64 __builtin_popcountll
#define QUIVER_POPCOUNT32 __builtin_popcount
#endif

inline constexpr bool IsPwr2(int32_t val) {
  if (std::is_constant_evaluated()) {
    return val > 0 && (val & (val - 1)) == 0;
  }
  return QUIVER_POPCOUNT32(val) == 1;
}
inline constexpr bool IsPwr2(uint32_t val) {
  if (std::is_constant_evaluated()) {
    return val > 0 && (val & (val - 1)) == 0;
  }
  return QUIVER_POPCOUNT32(val) == 1;
}
inline constexpr bool IsPwr2(int64_t val) {
  if (std::is_constant_evaluated()) {
    return val > 0 && (val & (val - 1)) == 0;
  }
  return QUIVER_POPCOUNT64(val) == 1;
}
inline constexpr bool IsPwr2(uint64_t val) {
  if (std::is_constant_evaluated()) {
    return val > 0 && (val & (val - 1)) == 0;
  }
  return QUIVER_POPCOUNT64(val) == 1;
}

inline bool IsPwr2OrZero(int32_t val) { return QUIVER_POPCOUNT32(val) <= 1; }
inline bool IsPwr2OrZero(uint32_t val) { return QUIVER_POPCOUNT32(val) <= 1; }
inline bool IsPwr2OrZero(int64_t val) { return QUIVER_POPCOUNT64(val) <= 1; }
inline bool IsPwr2OrZero(uint64_t val) { return QUIVER_POPCOUNT64(val) <= 1; }

// Returns the ceil of value/divisor
// Commonly used to get the number of bytes needed to contain some number
// of bits.  E.g. CeilDiv(X, 8) will tell how many bytes are needed to
// contain X bits.
constexpr int64_t CeilDiv(int64_t value, int64_t divisor) {
  return (value == 0) ? 0 : 1 + (value - 1) / divisor;
}

constexpr int32_t CeilDiv(int32_t value, int32_t divisor) {
  return (value == 0) ? 0 : 1 + (value - 1) / divisor;
}

constexpr int64_t FloorDiv(int64_t value, int64_t divisor) { return value / divisor; }
constexpr int32_t FloorDiv(int32_t value, int32_t divisor) { return value / divisor; }

/// Only valid if pwr_2 is a power of two.  Quickly calculate the number of
/// bytes needed to get from x to the next pwr_2
///
/// For example, PaddingNeededPwr2(5, 16) = 11
static inline int32_t PaddingNeededPwr2(int32_t offset, int pwr_2) {
  QUIVER_DCHECK(IsPwr2(pwr_2));
  return (-offset) & (pwr_2 - 1);
}

// Return the number of bytes needed to fit the given number of bits
constexpr int64_t BytesForBits(int64_t bits) {
  // This formula avoids integer overflow on very large `bits`
  return (bits >> 3) + static_cast<int64_t>((bits & 7) != 0);
}

// Returns a mask for the bit_index lower order bits.
// Only valid for bit_index in the range [0, 64).
constexpr uint64_t LeastSignificantBitMask(int64_t bit_index) {
  return (static_cast<uint64_t>(1) << bit_index) - 1;
}

static constexpr bool GetBit(const uint8_t* bits, uint64_t idx) {
  return ((bits[idx >> 3] >> (idx & 0x07)) & 1) != 0;
}

// Bitmask selecting the k-th bit in a byte
static constexpr std::array<uint8_t, 8> kBitmask = {1, 2, 4, 8, 16, 32, 64, 128};

static inline void SetBit(uint8_t* bits, int64_t idx) {
  bits[idx / 8] |= kBitmask[idx % 8];
}

static inline void SetBitTo(uint8_t* bits, int64_t idx, bool bit_is_set) {
  // https://graphics.stanford.edu/~seander/bithacks.html
  // "Conditionally set or clear bits without branching"
  // NOTE: this seems to confuse Valgrind as it reads from potentially
  // uninitialized memory
  bits[idx / 8] ^=
      static_cast<uint8_t>(-static_cast<uint8_t>(bit_is_set) ^ bits[idx / 8]) &
      kBitmask[idx % 8];
}

/// \brief set or clear a range of bits quickly
void SetBitsTo(uint8_t* bits, int64_t start_offset, int64_t length, bool bits_are_set);

/// \brief Sets all bits in the bitmap to true
void SetBitmap(uint8_t* data, int64_t offset, int64_t length);

/// \brief Clears all bits in the bitmap (set to false)
void ClearBitmap(uint8_t* data, int64_t offset, int64_t length);

// Gets the i-th bit from a byte. Should only be used with i <= 7.
static constexpr bool GetBitFromByte(uint8_t byte, uint8_t idx) {
  return (byte & kBitmask[idx]) != 0;
}

// Returns 'value' rounded up to the nearest multiple of 'factor'
constexpr int64_t RoundUp(int64_t value, int64_t factor) {
  return CeilDiv(value, factor) * factor;
}

// Returns 'value' rounded down to the nearest multiple of 'factor'
constexpr int64_t RoundDown(int64_t value, int64_t factor) {
  return (value / factor) * factor;
}

// Returns 'value' rounded up to the nearest multiple of 'factor' when factor
// is a power of two.
// The result is undefined on overflow, i.e. if `value > 2**64 - factor`,
// since we cannot return the correct result which would be 2**64.
constexpr int64_t RoundUpToPowerOf2(int64_t value, int64_t factor) {
  return (value + (factor - 1)) & ~(factor - 1);
}

static inline int32_t PopCount(uint64_t bitmap) { return QUIVER_POPCOUNT64(bitmap); }
static inline int32_t PopCount(uint32_t bitmap) { return QUIVER_POPCOUNT32(bitmap); }

// Bitmask selecting the (k - 1) preceding bits in a byte
static constexpr std::array<uint8_t, 8> kPrecedingBitmask = {0, 1, 3, 7, 15, 31, 63, 127};
// the bitwise complement version of kPrecedingBitmask
static constexpr std::array<uint8_t, 8> kTrailingBitmask = {255, 254, 252, 248,
                                                            240, 224, 192, 128};

static inline int CountTrailingZeros(uint32_t value) {
#if defined(__clang__) || defined(__GNUC__)
  if (value == 0) {
    return 32;
  }
  return static_cast<int>(__builtin_ctzl(value));
#elif defined(_MSC_VER)
  unsigned long index;  // NOLINT
  if (_BitScanForward(&index, value)) {
    return static_cast<int>(index);
  } else {
    return 32;
  }
#else
  int bitpos = 0;
  if (value) {
    while (value & 1 == 0) {
      value >>= 1;
      ++bitpos;
    }
  } else {
    bitpos = 32;
  }
  return bitpos;
#endif
}

static inline int CountTrailingZeros(uint64_t value) {
#if defined(__clang__) || defined(__GNUC__)
  if (value == 0) {
    return 64;
  }
  return static_cast<int>(__builtin_ctzll(value));
#elif defined(_MSC_VER)
  unsigned long index;  // NOLINT
  if (_BitScanForward64(&index, value)) {
    return static_cast<int>(index);
  } else {
    return 64;
  }
#else
  int bitpos = 0;
  if (value) {
    while (value & 1 == 0) {
      value >>= 1;
      ++bitpos;
    }
  } else {
    bitpos = 64;
  }
  return bitpos;
#endif
}

/// Returns a mask with lower i bits set to 1. If i >= sizeof(Word)*8, all-ones will be
/// returned
/// ex:
/// ref: https://stackoverflow.com/a/59523400
template <typename Word>
constexpr Word PrecedingWordBitmask(unsigned int const i) {
  return (static_cast<Word>(i < sizeof(Word) * 8) << (i & (sizeof(Word) * 8 - 1))) - 1;
}
static_assert(PrecedingWordBitmask<uint8_t>(0) == 0x00);
static_assert(PrecedingWordBitmask<uint8_t>(4) == 0x0f);
static_assert(PrecedingWordBitmask<uint8_t>(8) == 0xff);
static_assert(PrecedingWordBitmask<uint16_t>(8) == 0x00ff);

/// \brief Create a word with low `n` bits from `low` and high `sizeof(Word)-n` bits
/// from `high`.
/// Word ret
/// for (i = 0; i < sizeof(Word)*8; i++){
///     ret[i]= i < n ? low[i]: high[i];
/// }
template <typename Word>
constexpr Word SpliceWord(int n, Word low, Word high) {
  return (high & ~PrecedingWordBitmask<Word>(n)) | (low & PrecedingWordBitmask<Word>(n));
}

}  // namespace quiver::bit_util