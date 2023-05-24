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
/// | FIX_0 | FIX_1 | . | FIX_N | VALID_0 | VALID_LOG_2(N) | VAR_0 | VAR_1 | . | VAR_M |
///
/// FIX_x is the fixed-length portion of the column value
/// VALID_x is the validity-bitmap of the row
/// VAR_x is the variable-length portion of the column value
///
/// Each value is aligned as follows:
///
/// A fixed value, that is a bit, is packed into a byte with no alignment
/// A fixed value, that is a power of 2 # of bytes, has no padding
/// A fixed value, that is a non-power of 2 # of bytes, is padded to string_alignment
/// Variable length values are padded to string_alignment
///
/// TODO: Why pad bytes to string_alignment?  Does it matter?
///
/// Each row is aligned to row_alignment
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

  const SimpleSchema* schema = nullptr;

  /// Offsets within a row to fields in their encoding order.
  std::vector<int32_t> column_offsets;

  RowSchema(int32_t row_alignment, int32_t string_alignment)
      : row_alignment(row_alignment), string_alignment(string_alignment) {}

  Status Initialize(const SimpleSchema& schema);

  [[nodiscard]] int32_t encoded_field_offset(int32_t icol) const {
    return column_offsets[icol];
  }

  [[nodiscard]] int32_t num_cols() const { return schema->num_fields(); }

  [[nodiscard]] int32_t num_varbinary_cols() const;

  /// \brief True if `other` is based on the same schema
  [[nodiscard]] bool IsCompatible(const RowSchema& other) const;
};

class RowQueueAppendingProducer {
 public:
  virtual ~RowQueueAppendingProducer() = default;
  virtual Status Append(const ReadOnlyBatch& batch) = 0;
  static Status Create(const RowSchema* row_schema, Sink* sink,
                       std::unique_ptr<RowQueueAppendingProducer>* out);
};

class RowQueueRandomAccessConsumer {
 private:
};

}  // namespace quiver::row
