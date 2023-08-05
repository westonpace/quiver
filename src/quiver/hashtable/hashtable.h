#include "quiver/core/array.h"

#pragma once

namespace quiver::hashtable {

class HashTable {
 public:
  virtual ~HashTable() = default;

  /// Encodes hashes into the hashtable
  ///
  /// \param hashes The hashes to encode
  /// \param row_ids The row ids to encode
  ///              (must be the same length as hashes)
  virtual void Encode(std::span<const int64_t> hashes,
                      std::span<const int64_t> row_ids) = 0;

  /// Decodes rows from the hashtable
  /// \param hashes The hashes to decode
  /// \param hash_idx_out The index of the hash that was decoded
  /// \param row_ids_out The decoded row ids, must have same size as `hash_idx_out`
  /// \param length_out The number of decoded rows
  /// \param hash_idx_offset The hash index to start at (will be updated)
  /// \param bucket_idx_offset The index in the bucket at hash_idx_offset to start at
  /// (will be updated)
  ///
  /// \return true if all row ids have been returned.  False if further
  /// paging required
  ///
  /// \note A single hash can match 0 or many row ids.  This means that its
  ///       impossible to tell how many row ids will be returned.  As such, it is
  ///       best to page through these results.  The page size will be determined by
  ///       the size of `row_ids_out`.
  ///
  ///       To page, set the initial `hash_idx_offset` and `bucket_idx_offset` to 0 and
  ///       then simply call this method repeatedly until it returns true
  ///
  /// \note When this function returns true then the value of hash_idx_offset and
  ///       bucket_idx_offset will be undefined (and most likely just not set)
  virtual bool Decode(std::span<const int64_t> hashes, std::span<int32_t> hash_idx_out,
                      std::span<int64_t> row_ids_out, int64_t* length_out,
                      int64_t* hash_idx_offset, int64_t* bucket_idx_offset) = 0;

  static std::unique_ptr<HashTable> MakeStl();
};

}  // namespace quiver::hashtable
