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
#include "quiver/core/io.h"
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
struct RowSchema {
  /// \brief True if there are no variable length columns in the table
  bool is_fixed_length = true;

  /// The size of the fixed portion of the row
  ///
  /// If this is the entire row then this will be a multiple of row_alignment
  ///
  /// If there are any varlength columns then this will be a multiple of string_alignment
  int32_t fixed_length = 0;

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

  RowSchema(int32_t row_alignment, int32_t string_alignment)
      : row_alignment(row_alignment), string_alignment(string_alignment) {}

  Status Initialize(const SimpleSchema& schema);

  /// Rounding up offset to the beginning of next column,
  /// choosing required alignment based on the data type of that column.
  static int32_t PaddingNeeded(int32_t offset, int32_t string_alignment,
                               const FieldDescriptor& field) {
    if (bit_util::IsPwr2OrZero(field.data_width_bytes)) {
      return 0;
    }
    return bit_util::PaddingNeededPwr2(offset, string_alignment);
  }

  [[nodiscard]] int32_t encoded_field_order(int32_t icol) const {
    return column_order[icol];
  }

  [[nodiscard]] int32_t pos_after_encoding(int32_t icol) const {
    return inverse_column_order[icol];
  }

  [[nodiscard]] int32_t encoded_field_offset(int32_t icol) const {
    return column_offsets[icol];
  }

  [[nodiscard]] int32_t num_cols() const { return schema->num_fields(); }

  [[nodiscard]] int32_t num_varbinary_cols() const;

  /// \brief True if `other` has the same number of columns
  ///   and each column has the same width (two variable length
  ///   columns are considered to have the same width)
  [[nodiscard]] bool IsCompatible(const RowSchema& other) const;
};

class RowQueueAppendingProducer {
 public:
  virtual Status Append(const ReadOnlyBatch& batch) = 0;
};

class RowQueueRandomAccessConsumer {
 private:
};

}  // namespace quiver::row
