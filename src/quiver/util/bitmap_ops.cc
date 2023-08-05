// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#include "quiver/util/bitmap_ops.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>

#include "quiver/util/align_util.h"
#include "quiver/util/bit_block_counter.h"
#include "quiver/util/bit_util.h"
#include "quiver/util/bitmap_reader.h"
#include "quiver/util/bitmap_writer.h"
#include "quiver/util/logging_p.h"

namespace quiver::util {

int64_t CountSetBits(const uint8_t* data, int64_t bit_offset, int64_t length) {
  constexpr int64_t pop_len = sizeof(uint64_t) * 8;
  DCHECK_GE(bit_offset, 0);
  int64_t count = 0;

  const auto params = BitmapWordAlign<pop_len / 8>(data, bit_offset, length);
  for (int64_t i = bit_offset; i < bit_offset + params.leading_bits; ++i) {
    if (bit_util::GetBit(data, i)) {
      ++count;
    }
  }

  if (params.aligned_words > 0) {
    // popcount as much as possible with the widest possible count
    const auto* u64_data = reinterpret_cast<const uint64_t*>(params.aligned_start);
    DCHECK_EQ(reinterpret_cast<size_t>(u64_data) & 7, 0);
    const uint64_t* end = u64_data + params.aligned_words;

    constexpr int64_t kCountUnrollFactor = 4;
    const int64_t words_rounded =
        bit_util::RoundDown(params.aligned_words, kCountUnrollFactor);
    std::array<int64_t, kCountUnrollFactor> count_unroll = {0};

    // Unroll the loop for better performance
    for (int64_t i = 0; i < words_rounded; i += kCountUnrollFactor) {
      for (int64_t k = 0; k < kCountUnrollFactor; k++) {
        count_unroll[k] += bit_util::PopCount(u64_data[k]);
      }
      u64_data += kCountUnrollFactor;
    }
    for (int64_t k = 0; k < kCountUnrollFactor; k++) {
      count += count_unroll[k];
    }

    // The trailing part
    for (; u64_data < end; ++u64_data) {
      count += bit_util::PopCount(*u64_data);
    }
  }

  // Account for left over bits (in theory we could fall back to smaller
  // versions of popcount but the code complexity is likely not worth it)
  for (int64_t i = params.trailing_bit_offset; i < bit_offset + length; ++i) {
    if (bit_util::GetBit(data, i)) {
      ++count;
    }
  }

  return count;
}

int64_t CountAndSetBits(const uint8_t* left_bitmap, int64_t left_offset,
                        const uint8_t* right_bitmap, int64_t right_offset,
                        int64_t length) {
  BinaryBitBlockCounter bit_counter(left_bitmap, left_offset, right_bitmap, right_offset,
                                    length);
  int64_t count = 0;
  while (true) {
    BitBlockCount block = bit_counter.NextAndWord();
    if (block.length == 0) {
      break;
    }
    count += block.popcount;
  }
  return count;
}

enum class TransferMode : bool { Copy, Invert };

// Reverse all bits from entire byte(uint8)
uint8_t ReverseUint8(uint8_t num) {
  num = ((num & 0xf0) >> 4) | ((num & 0x0f) << 4);
  num = ((num & 0xcc) >> 2) | ((num & 0x33) << 2);
  num = ((num & 0xaa) >> 1) | ((num & 0x55) << 1);
  return num;
}

// Get a reverse block of byte(uint8) using offsets, the result can be
// part of a left block and right block, length indicates the number of bits
// to be taken from the right block
uint8_t GetReversedBlock(uint8_t block_left, uint8_t block_right, uint8_t length) {
  return ReverseUint8(((block_right << 8) + block_left) >> length);
}

template <TransferMode mode>
void TransferBitmap(const uint8_t* data, int64_t offset, int64_t length,
                    int64_t dest_offset, uint8_t* dest) {
  int64_t bit_offset = offset % 8;
  int64_t dest_bit_offset = dest_offset % 8;

  if (bit_offset || dest_bit_offset) {
    auto reader = BitmapWordReader<uint64_t>(data, offset, length);
    auto writer = BitmapWordWriter<uint64_t>(dest, dest_offset, length);

    auto nwords = reader.words();
    while (nwords--) {
      auto word = reader.NextWord();
      writer.PutNextWord(mode == TransferMode::Invert ? ~word : word);
    }
    auto nbytes = reader.trailing_bytes();
    while (nbytes--) {
      int valid_bits;
      auto byte = reader.NextTrailingByte(valid_bits);
      writer.PutNextTrailingByte(mode == TransferMode::Invert ? ~byte : byte, valid_bits);
    }
  } else if (length) {
    int64_t num_bytes = bit_util::BytesForBits(length);

    // Shift by its byte offset
    data += offset / 8;
    dest += dest_offset / 8;

    // Take care of the trailing bits in the last byte
    // E.g., if trailing_bits = 5, last byte should be
    // - low  3 bits: new bits from last byte of data buffer
    // - high 5 bits: old bits from last byte of dest buffer
    int64_t trailing_bits = num_bytes * 8 - length;
    uint8_t trail_mask = (1U << (8 - trailing_bits)) - 1;
    uint8_t last_data;

    if (mode == TransferMode::Invert) {
      for (int64_t i = 0; i < num_bytes - 1; i++) {
        dest[i] = static_cast<uint8_t>(~(data[i]));
      }
      last_data = ~data[num_bytes - 1];
    } else {
      std::memcpy(dest, data, static_cast<size_t>(num_bytes - 1));
      last_data = data[num_bytes - 1];
    }

    // Set last byte
    dest[num_bytes - 1] &= ~trail_mask;
    dest[num_bytes - 1] |= last_data & trail_mask;
  }
}

template <bool kOutputByteAligned, typename IndexType>
void IndexedCopyBitmapHelper(const uint8_t* input_bits, int64_t input_bits_offset,
                             uint8_t* output_bits, int64_t output_bits_offset,
                             std::span<const IndexType> indices) {
  auto num_rows = static_cast<int64_t>(indices.size());
  const IndexType* row_ids = indices.data();
  if (!kOutputByteAligned) {
    QUIVER_DCHECK(output_bits_offset % 8 > 0);
    output_bits[output_bits_offset / 8] &=
        static_cast<uint8_t>((1 << (output_bits_offset % 8)) - 1);
  } else {
    QUIVER_DCHECK(output_bits_offset % 8 == 0);
  }
  constexpr int unroll = 8;
  for (int i = 0; i < num_rows / unroll; ++i) {
    const IndexType* row_ids_base = row_ids + unroll * i;
    uint8_t result;
    result = bit_util::GetBit(input_bits, input_bits_offset + row_ids_base[0]) ? 1 : 0;
    result |= bit_util::GetBit(input_bits, input_bits_offset + row_ids_base[1]) ? 2 : 0;
    result |= bit_util::GetBit(input_bits, input_bits_offset + row_ids_base[2]) ? 4 : 0;
    result |= bit_util::GetBit(input_bits, input_bits_offset + row_ids_base[3]) ? 8 : 0;
    result |= bit_util::GetBit(input_bits, input_bits_offset + row_ids_base[4]) ? 16 : 0;
    result |= bit_util::GetBit(input_bits, input_bits_offset + row_ids_base[5]) ? 32 : 0;
    result |= bit_util::GetBit(input_bits, input_bits_offset + row_ids_base[6]) ? 64 : 0;
    result |= bit_util::GetBit(input_bits, input_bits_offset + row_ids_base[7]) ? 128 : 0;
    if (kOutputByteAligned) {
      output_bits[output_bits_offset / 8 + i] = result;
    } else {
      output_bits[output_bits_offset / 8 + i] |=
          static_cast<uint8_t>(result << (output_bits_offset % 8));
      output_bits[output_bits_offset / 8 + i + 1] =
          static_cast<uint8_t>(result >> (8 - (output_bits_offset % 8)));
    }
  }
  if (num_rows % unroll > 0) {
    for (int64_t i = num_rows - (num_rows % unroll); i < num_rows; ++i) {
      bit_util::SetBitTo(output_bits, output_bits_offset + i,
                         bit_util::GetBit(input_bits, input_bits_offset + row_ids[i]));
    }
  }
}

template <typename IndexType>
void IndexedCopyBitmap(const uint8_t* bitmap, std::span<const IndexType> indices,
                       uint8_t* dest, int64_t dest_offset) {
  bool output_aligned = dest_offset % 8 == 0;
  if (output_aligned) {
    IndexedCopyBitmapHelper<true, IndexType>(bitmap, 0, dest, dest_offset, indices);
  } else {
    IndexedCopyBitmapHelper<false, IndexType>(bitmap, 0, dest, dest_offset, indices);
  }
}

void IndexedCopyBitmap(const uint8_t* bitmap, std::span<const int32_t> indices,
                       uint8_t* dest, int64_t dest_offset) {
  IndexedCopyBitmap<int32_t>(bitmap, indices, dest, dest_offset);
}

void IndexedCopyBitmap(const uint8_t* bitmap, std::span<const int64_t> indices,
                       uint8_t* dest, int64_t dest_offset) {
  IndexedCopyBitmap<int64_t>(bitmap, indices, dest, dest_offset);
}

void ReverseBlockOffsets(const uint8_t* data, int64_t offset, int64_t length,
                         int64_t dest_offset, uint8_t* dest) {
  int64_t num_bytes = bit_util::BytesForBits(offset % 8 + length);
  // Shift by its byte offset
  data += offset / 8;
  dest += dest_offset / 8;

  int64_t j_src = num_bytes - 1;
  int64_t i_dest = 0;

  while (length > 0) {
    uint8_t right_trailing_bits_src = (length + offset) % 8;
    right_trailing_bits_src =
        (right_trailing_bits_src == 0) ? 8 : right_trailing_bits_src;

    uint8_t left_trailing_bits_dest = 8 - dest_offset % 8;
    uint8_t left_trailing_mask_dest = 0xFF << (8 - left_trailing_bits_dest);
    if (length <= 8 && (dest_offset % 8) + length < 8) {
      auto extra_bits = static_cast<uint8_t>(8 - ((dest_offset % 8) + length));
      left_trailing_mask_dest <<= extra_bits;
      left_trailing_mask_dest >>= extra_bits;
    }

    uint8_t right_reversed_block;
    if (j_src == 0) {
      right_reversed_block = static_cast<uint8_t>(
          GetReversedBlock(data[0], data[0], right_trailing_bits_src));
    } else {
      right_reversed_block = static_cast<uint8_t>(
          GetReversedBlock(data[j_src - 1], data[j_src], right_trailing_bits_src));
    }

    dest[i_dest] &= ~left_trailing_mask_dest;
    dest[i_dest] |=
        (right_reversed_block << (8 - left_trailing_bits_dest)) & left_trailing_mask_dest;

    dest_offset += left_trailing_bits_dest;
    length -= left_trailing_bits_dest;

    if (left_trailing_bits_dest >= right_trailing_bits_src) {
      j_src--;
    }
    i_dest++;
  }
}

void CopyBitmap(const uint8_t* bitmap, int64_t offset, int64_t length, uint8_t* dest,
                int64_t dest_offset) {
  TransferBitmap<TransferMode::Copy>(bitmap, offset, length, dest_offset, dest);
}

void InvertBitmap(const uint8_t* bitmap, int64_t offset, int64_t length, uint8_t* dest,
                  int64_t dest_offset) {
  TransferBitmap<TransferMode::Invert>(bitmap, offset, length, dest_offset, dest);
}

void ReverseBitmap(const uint8_t* bitmap, int64_t offset, int64_t length, uint8_t* dest,
                   int64_t dest_offset) {
  ReverseBlockOffsets(bitmap, offset, length, dest_offset, dest);
}

bool BitmapEquals(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                  int64_t right_offset, int64_t length) {
  if (left_offset % 8 == 0 && right_offset % 8 == 0) {
    // byte aligned, can use memcmp
    bool bytes_equal =
        std::memcmp(left + left_offset / 8, right + right_offset / 8, length / 8) == 0;
    if (!bytes_equal) {
      return false;
    }
    for (int64_t i = (length / 8) * 8; i < length; ++i) {
      if (bit_util::GetBit(left, left_offset + i) !=
          bit_util::GetBit(right, right_offset + i)) {
        return false;
      }
    }
    return true;
  }

  // Unaligned slow case
  auto left_reader = BitmapWordReader<uint64_t>(left, left_offset, length);
  auto right_reader = BitmapWordReader<uint64_t>(right, right_offset, length);

  int64_t nwords = left_reader.words();
  while ((nwords--) != 0) {
    if (left_reader.NextWord() != right_reader.NextWord()) {
      return false;
    }
  }
  auto nbytes = left_reader.trailing_bytes();
  while ((nbytes--) != 0) {
    int valid_bits;
    if (left_reader.NextTrailingByte(valid_bits) !=
        right_reader.NextTrailingByte(valid_bits)) {
      return false;
    }
  }
  return true;
}

bool OptionalBitmapEquals(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                          int64_t right_offset, int64_t length) {
  if (left == nullptr && right == nullptr) {
    return true;
  }
  if (left != nullptr && right != nullptr) {
    return BitmapEquals(left, left_offset, right, right_offset, length);
  }
  if (left != nullptr) {
    return CountSetBits(left, left_offset, length) == length;
  }
  return CountSetBits(right, right_offset, length) == length;
}

}  // namespace quiver::util