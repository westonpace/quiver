// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "quiver/core/array.h"
#include "quiver/core/buffer.h"
#include "quiver/util/bit_util.h"
#include "quiver/util/logging_p.h"
#include "quiver/util/memory_pool.h"
#include "quiver/util/status.h"

namespace quiver::row {

/// Description of the data stored in a RowBatch
///
/// Each row (with N fixed-length columns and M variable-length columns) is stored in the
/// following form:
///
/// | FIX_0 | FIX_1 | . | FIX_N | END_0 | END_1 | . | END_M | VAR_0 | VAR_1 | . | VAR_M |
///
/// FIX_x is a fixed-length column value
/// END_x is an offset from the first byte of the row to the end of VAR_x (one byte after)
/// VAR_x is a variable-length column value
///
/// Each variable-length value is aligned according to a given alignment.
///
/// Validity information is kept in a separate array.
struct RowBatchMetadata {
  /// \brief True if there are no variable length columns in the table
  bool is_fixed_length = true;

  /// For a fixed-length binary row, common size of rows in bytes,
  /// rounded up to the multiple of alignment.
  ///
  /// For a varying-length binary, size of all encoded fixed-length key columns,
  /// including lengths of varying-length columns, rounded up to the multiple of string
  /// alignment.
  int32_t fixed_length = 0;

  /// Offset within a row to the array of 32-bit offsets within a row of
  /// ends of varbinary fields.
  /// Used only when the row is not fixed-length, zero for fixed-length row.
  /// There are N elements for N varbinary fields.
  /// Each element is the offset within a row of the first byte after
  /// the corresponding varbinary field bytes in that row.
  /// If varbinary fields begin at aligned addresses, than the end of the previous
  /// varbinary field needs to be rounded up according to the specified alignment
  /// to obtain the beginning of the next varbinary field.
  /// The first varbinary field starts at offset specified by fixed_length,
  /// which should already be aligned.
  int32_t varbinary_end_array_offset = 0;

  /// Fixed number of bytes per row that are used to encode null masks.
  /// Null masks indicate for a single row which of its columns are null.
  /// Nth bit in the sequence of bytes assigned to a row represents null
  /// information for Nth field according to the order in which they are encoded.
  int32_t null_masks_bytes_per_row = 0;

  /// Power of 2. Every row will start at an offset aligned to that number of bytes.
  int32_t row_alignment;

  /// Power of 2. Must be no greater than row alignment.
  /// Every non-power-of-2 binary field and every varbinary field bytes
  /// will start aligned to that number of bytes.
  int32_t string_alignment;

  /// Metadata of encoded columns in their original order.
  const SimpleSchema* schema = nullptr;

  /// Order in which fields are encoded.
  std::vector<int32_t> column_order;
  std::vector<int32_t> inverse_column_order;

  /// Offsets within a row to fields in their encoding order.
  std::vector<int32_t> column_offsets;

  RowBatchMetadata(int32_t row_alignment, int32_t string_alignment)
      : row_alignment(row_alignment), string_alignment(string_alignment) {}

  Status Initialize(const SimpleSchema& cols);

  /// Rounding up offset to the beginning of next column,
  /// choosing required alignment based on the data type of that column.
  static int32_t PaddingNeeded(int32_t offset, int32_t string_alignment,
                               const FieldDescriptor& field) {
    if (field.layout != LayoutKind::kFlat || !bit_util::IsPwr2(field.data_width_bytes)) {
      return 0;
    } else {
      return bit_util::PaddingNeededPwr2(offset, string_alignment);
    }
  }

  /// Returns a pointer to the "ends" section of a row
  const int32_t* varbinary_end_array(const uint8_t* row) const {
    QUIVER_DCHECK(!is_fixed_length);
    return reinterpret_cast<const int32_t*>(row + varbinary_end_array_offset);
  }
  int32_t* varbinary_end_array(uint8_t* row) const {
    QUIVER_DCHECK(!is_fixed_length);
    return reinterpret_cast<int32_t*>(row + varbinary_end_array_offset);
  }

  // Returns the offset within the row and length of the first varbinary field.
  void first_varbinary_offset_and_length(const uint8_t* row, int32_t* offset,
                                         int32_t* length) const {
    QUIVER_DCHECK(!is_fixed_length);
    *offset = fixed_length;
    *length = varbinary_end_array(row)[0] - fixed_length;
  }

  // Returns the offset within the row and length of the second and further varbinary
  // fields.
  //
  // This is a bit more complicated than finding the first varlen field offset because we
  // need to calculate the end of the previous varlen field and then find the next
  // alignment boundary
  inline void nth_varbinary_offset_and_length(const uint8_t* row, int32_t varbinary_id,
                                              int32_t* out_offset,
                                              int32_t* out_length) const {
    QUIVER_DCHECK(!is_fixed_length);
    QUIVER_DCHECK(varbinary_id > 0);
    const int32_t* varbinary_end = varbinary_end_array(row);
    int32_t offset = varbinary_end[varbinary_id - 1];
    offset += bit_util::PaddingNeededPwr2(offset, string_alignment);
    *out_offset = offset;
    *out_length = varbinary_end[varbinary_id] - offset;
  }

  int32_t encoded_field_order(int32_t icol) const { return column_order[icol]; }

  int32_t pos_after_encoding(int32_t icol) const { return inverse_column_order[icol]; }

  int32_t encoded_field_offset(int32_t icol) const { return column_offsets[icol]; }

  int32_t num_cols() const { return static_cast<int32_t>(column_metadatas.size()); }

  int32_t num_varbinary_cols() const;

  /// \brief True if `other` has the same number of columns
  ///   and each column has the same width (two variable length
  ///   columns are considered to have the same width)
  bool IsCompatible(const RowBatchMetadata& other) const;
};

/// \brief A table of data stored in row-major order
class RowBatchImpl {
 public:
  RowBatchImpl();
  /// \brief Initialize a row array for use
  ///
  /// This must be called before any other method
  Status Init(MemoryPool* pool, const RowBatchMetadata& metadata);
  /// \brief Clear all rows from the table
  ///
  /// Does not shrink buffers
  void Clean();
  /// \brief Add empty rows
  /// \param num_rows_to_append The number of empty rows to append
  /// \param num_extra_bytes_to_append For tables storing variable-length data this
  ///     should be a guess of how many data bytes will be needed to populate the
  ///     data.  This is ignored if there are no variable-length columns
  Status AppendEmpty(int32_t num_rows_to_append, int32_t num_extra_bytes_to_append);
  /// \brief Append rows from a source table
  /// \param from The table to append from
  /// \param num_rows_to_append The number of rows to append
  /// \param source_row_ids Indices (into `from`) of the desired rows
  Status AppendSelectionFrom(const RowBatchImpl& from, int32_t num_rows_to_append,
                             const uint16_t* source_row_ids);
  /// \brief Metadata describing the data stored in this table
  const RowBatchMetadata& metadata() const { return metadata_; }
  /// \brief The number of rows stored in the table
  int64_t length() const { return num_rows_; }
  // Accessors into the table's buffers
  const uint8_t* data(int32_t i) const {
    QUIVER_DCHECK(i >= 0 && i < kMaxBuffers);
    return buffers_[i];
  }
  uint8_t* mutable_data(int32_t i) {
    QUIVER_DCHECK(i >= 0 && i < kMaxBuffers);
    return buffers_[i];
  }
  const int32_t* offsets() const { return reinterpret_cast<const int32_t*>(data(1)); }
  int32_t* mutable_offsets() { return reinterpret_cast<int32_t*>(mutable_data(1)); }
  const Buffer& null_masks() const { return null_masks_; }
  Buffer& null_masks() { return null_masks_; }

  /// \brief True if there is a null value anywhere in the table
  ///
  /// This calculation is memoized based on the number of rows and assumes
  /// that values are only appended (and not modified in place) between
  /// successive calls
  bool has_any_nulls() const;

 private:
  Status ResizeFixedLengthBuffers(int64_t num_extra_rows);
  Status ResizeOptionalVaryingLengthBuffer(int64_t num_extra_bytes);

  // Helper functions to determine the number of bytes needed for each
  // buffer given a number of rows.
  int64_t size_null_masks(int64_t num_rows) const;
  int64_t size_offsets(int64_t num_rows) const;
  int64_t size_rows_fixed_length(int64_t num_rows) const;
  int64_t size_rows_varying_length(int64_t num_bytes) const;

  // Called after resize to fix pointers
  void UpdateBufferPointers();

  // The arrays in `buffers_` need to be padded so that
  // vectorized operations can operate in blocks without
  // worrying about tails
  static constexpr int64_t kPaddingForVectors = 64;
  MemoryPool* pool_;
  RowBatchMetadata metadata_;
  // Buffers can only expand during lifetime and never shrink.
  Buffer null_masks_;
  // Only used if the table has variable-length columns
  // Stores the offsets into the binary data (which is stored
  // after all the fixed-sized fields)
  Buffer offsets_;
  // Stores the fixed-length parts of the rows
  Buffer rows_;
  static constexpr int32_t kMaxBuffers = 3;
  uint8_t* buffers_[kMaxBuffers];
  // The number of rows in the table
  int64_t num_rows_;
  // The number of rows that can be stored in the table without resizing
  int64_t rows_capacity_;
  // The number of bytes that can be stored in the table without resizing
  int64_t bytes_capacity_;

  // Mutable to allow lazy evaluation
  mutable int64_t num_rows_for_has_any_nulls_;
  mutable bool has_any_nulls_;
};

}  // namespace quiver::row
