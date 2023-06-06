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

class RowQueueAppendingProducer {
 public:
  virtual ~RowQueueAppendingProducer() = default;
  virtual Status Append(const ReadOnlyBatch& batch) = 0;
  static Status Create(const SimpleSchema* schema, StreamSink* sink,
                       std::unique_ptr<RowQueueAppendingProducer>* out);
};

class RowQueueRandomAccessConsumer {
 public:
  virtual ~RowQueueRandomAccessConsumer() = default;
  virtual Status Load(std::span<int32_t> indices, Batch* out) = 0;
  static Status Create(const SimpleSchema* schema, RandomAccessSource* source,
                       std::unique_ptr<RowQueueRandomAccessConsumer>* out);
};

}  // namespace quiver::row
