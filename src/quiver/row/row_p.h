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

/// Converts from a columnar format to a row based format and appends to storage
///
/// The row encoder both converts data from columnar to row format and inserts
/// data into storage.  It does this in a single pass to prevent making an extra
/// copy of the data.
///
/// The row encoder inserts data in an appending fashion.  Each call inserts additional
/// rows into the storage.
class RowEncoder {
 public:
  virtual ~RowEncoder() = default;
  /// Convert and insert a batch of data
  ///
  /// \param batch The batch to convert
  /// \param out_row_id The id of the first inserted row, used for lookup in the decoder
  virtual Status Append(const ReadOnlyBatch& batch, int64_t* out_row_id) = 0;
  static Status Create(const SimpleSchema* schema, StreamSink* sink,
                       std::unique_ptr<RowEncoder>* out);
};

/// Retrieve and decode rows from a row-based storage
///
/// The row decoder both fetches rows from storage and decodes them back into a columnar
/// format.
///
/// The row decoder requires random access to the source as it is expected that it will
/// need to fetch rows in random access.
class RowDecoder {
 public:
  virtual ~RowDecoder() = default;
  virtual Status Load(std::span<int64_t> indices, Batch* out) = 0;
  static Status Create(const SimpleSchema* schema, RandomAccessSource* source,
                       std::unique_ptr<RowDecoder>* out);
};

}  // namespace quiver::row
