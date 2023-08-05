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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "quiver/util/status.h"

namespace quiver {

constexpr int64_t kDefaultBufferAlignment = 64;

/// Base class for memory allocation on the CPU.
///
/// Besides tracking the number of allocated bytes, the allocator also should
/// take care of the required 64-byte alignment.
class MemoryPool {
 public:
  virtual ~MemoryPool() = default;

  /// Allocate a new memory region of at least size bytes.
  ///
  /// The allocated region shall be 64-byte aligned.
  Status Allocate(int64_t size, uint8_t** out) {
    return Allocate(size, kDefaultBufferAlignment, out);
  }

  /// Allocate a new memory region of at least size bytes aligned to alignment.
  virtual Status Allocate(int64_t size, int64_t alignment, uint8_t** out) = 0;

  /// Resize an already allocated memory section.
  ///
  /// As by default most default allocators on a platform don't support aligned
  /// reallocation, this function can involve a copy of the underlying data.
  virtual Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                            uint8_t** ptr) = 0;
  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    return Reallocate(old_size, new_size, kDefaultBufferAlignment, ptr);
  }

  /// Free an allocated region.
  ///
  /// @param buffer Pointer to the start of the allocated memory region
  /// @param size Allocated size located at buffer. An allocator implementation
  ///   may use this for tracking the amount of allocated bytes as well as for
  ///   faster deallocation if supported by its backend.
  /// @param alignment The alignment of the allocation. Defaults to 64 bytes.
  virtual void Free(uint8_t* buffer, int64_t size, int64_t alignment) = 0;
  void Free(uint8_t* buffer, int64_t size) {
    Free(buffer, size, kDefaultBufferAlignment);
  }

  /// The number of bytes that were allocated and not yet free'd through
  /// this allocator.
  virtual int64_t bytes_allocated() const = 0;

  /// Return peak memory allocation in this memory pool
  ///
  /// \return Maximum bytes allocated. If not known (or not implemented),
  /// returns -1
  virtual int64_t max_memory() const;

  /// The number of bytes that have ever been allocated.
  virtual int64_t total_bytes_allocated() const = 0;

  /// The number of allocations or reallocations that were requested.
  virtual int64_t num_allocations() const = 0;

  /// The name of the backend used by this MemoryPool (e.g. "system" or "jemalloc").
  virtual std::string backend_name() const = 0;

 protected:
  MemoryPool() = default;
};

/// \brief Return the default memory pool
MemoryPool* DefaultMemoryPool();

}  // namespace quiver
