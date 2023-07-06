// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#include <cstdint>

#include "quiver/util/bit_util.h"

namespace quiver::util {

struct BitmapWordAlignParams {
  int64_t leading_bits;
  int64_t trailing_bits;
  int64_t trailing_bit_offset;
  const uint8_t* aligned_start;
  int64_t aligned_bits;
  int64_t aligned_words;
};

// Compute parameters for accessing a bitmap using aligned word instructions.
// The returned parameters describe:
// - a leading area of size `leading_bits` before the aligned words
// - a word-aligned area of size `aligned_bits`
// - a trailing area of size `trailing_bits` after the aligned words
template <uint64_t ALIGN_IN_BYTES>
inline BitmapWordAlignParams BitmapWordAlign(const uint8_t* data, int64_t bit_offset,
                                             int64_t length) {
  static_assert(bit_util::IsPwr2(ALIGN_IN_BYTES),
                "ALIGN_IN_BYTES should be a positive power of two");
  constexpr uint64_t ALIGN_IN_BITS = ALIGN_IN_BYTES * 8;

  BitmapWordAlignParams params;

  // Compute a "bit address" that we can align up to ALIGN_IN_BITS.
  // We don't care about losing the upper bits since we are only interested in the
  // difference between both addresses.
  const uint64_t bit_addr =
      reinterpret_cast<size_t>(data) * 8 + static_cast<uint64_t>(bit_offset);
  // TODO: is this static cast safe?
  const uint64_t aligned_bit_addr =
      bit_util::RoundUpToPowerOf2(static_cast<int64_t>(bit_addr), ALIGN_IN_BITS);

  params.leading_bits = std::min<int64_t>(length, aligned_bit_addr - bit_addr);
  params.aligned_words = (length - params.leading_bits) / ALIGN_IN_BITS;
  params.aligned_bits = params.aligned_words * ALIGN_IN_BITS;
  params.trailing_bits = length - params.leading_bits - params.aligned_bits;
  params.trailing_bit_offset = bit_offset + params.leading_bits + params.aligned_bits;

  params.aligned_start = data + (bit_offset + params.leading_bits) / 8;
  return params;
}

}  // namespace quiver::util
