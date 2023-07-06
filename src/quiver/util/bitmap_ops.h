// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#pragma once

#include <cstdint>
#include <memory>
#include <span>

namespace quiver::util {

// ----------------------------------------------------------------------
// Bitmap utilities

/// Copy a bit range of an existing bitmap into an existing bitmap
///
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
/// \param[in] dest_offset bit offset into the destination
/// \param[out] dest the destination buffer, must have at least space for
/// (offset + length) bits

void CopyBitmap(const uint8_t* bitmap, int64_t offset, int64_t length, uint8_t* dest,
                int64_t dest_offset);

void IndexedCopyBitmap(const uint8_t* bitmap, std::span<const int32_t> indices,
                       uint8_t* dest, int64_t dest_offset);
void IndexedCopyBitmap(const uint8_t* bitmap, std::span<const int64_t> indices,
                       uint8_t* dest, int64_t dest_offset);

/// Invert a bit range of an existing bitmap into an existing bitmap
///
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
/// \param[in] dest_offset bit offset into the destination
/// \param[out] dest the destination buffer, must have at least space for
/// (offset + length) bits

void InvertBitmap(const uint8_t* bitmap, int64_t offset, int64_t length, uint8_t* dest,
                  int64_t dest_offset);

/// Reverse a bit range of an existing bitmap into an existing bitmap
///
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to reverse
/// \param[in] dest_offset bit offset into the destination
/// \param[out] dest the destination buffer, must have at least space for
/// (offset + length) bits

void ReverseBitmap(const uint8_t* bitmap, int64_t offset, int64_t length, uint8_t* dest,
                   int64_t dest_offset);

/// Compute the number of 1's in the given data array
///
/// \param[in] data a packed LSB-ordered bitmap as a byte array
/// \param[in] bit_offset a bitwise offset into the bitmap
/// \param[in] length the number of bits to inspect in the bitmap relative to
/// the offset
///
/// \return The number of set (1) bits in the range

int64_t CountSetBits(const uint8_t* data, int64_t bit_offset, int64_t length);

/// Compute the number of 1's in the result of an "and" (&) of two bitmaps
///
/// \param[in] left_bitmap a packed LSB-ordered bitmap as a byte array
/// \param[in] left_offset a bitwise offset into the left bitmap
/// \param[in] right_bitmap a packed LSB-ordered bitmap as a byte array
/// \param[in] right_offset a bitwise offset into the right bitmap
/// \param[in] length the length of the bitmaps (must be the same)
///
/// \return The number of set (1) bits in the "and" of the two bitmaps

int64_t CountAndSetBits(const uint8_t* left_bitmap, int64_t left_offset,
                        const uint8_t* right_bitmap, int64_t right_offset,
                        int64_t length);

bool BitmapEquals(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                  int64_t right_offset, int64_t length);

// Same as BitmapEquals, but considers a NULL bitmap pointer the same as an
// all-ones bitmap.

bool OptionalBitmapEquals(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                          int64_t right_offset, int64_t length);

}  // namespace quiver::util