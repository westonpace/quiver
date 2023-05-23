// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#pragma once

#include <cstdint>

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

inline bool IsPwr2(int32_t val) { return QUIVER_POPCOUNT32(val) == 1; }
inline bool IsPwr2(uint32_t val) { return QUIVER_POPCOUNT32(val) == 1; }
inline bool IsPwr2(int64_t val) { return QUIVER_POPCOUNT64(val) == 1; }
inline bool IsPwr2(uint64_t val) { return QUIVER_POPCOUNT64(val) == 1; }

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

/// Only valid if pwr_2 is a power of two.  Quickly calculate the number of
/// bytes needed to get from x to the next pwr_2
///
/// For example, PaddingNeededPwr2(5, 16) = 11
static inline int32_t PaddingNeededPwr2(int32_t offset, int pwr_2) {
  QUIVER_DCHECK(IsPwr2(pwr_2));
  return (-offset) & (pwr_2 - 1);
}

}  // namespace quiver::bit_util