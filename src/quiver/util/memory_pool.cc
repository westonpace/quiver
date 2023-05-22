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

#include "quiver/util/memory_pool.h"

#include <algorithm>  // IWYU pragma: keep
#include <atomic>
#include <cstdlib>   // IWYU pragma: keep
#include <cstring>   // IWYU pragma: keep
#include <iostream>  // IWYU pragma: keep
#include <limits>
#include <memory>
#include <mutex>
#include <optional>

#include "quiver/util/logging_p.h"
#include "quiver/util/status.h"

namespace quiver {

namespace {

// A static piece of memory for 0-size allocations, so as to return
// an aligned non-null pointer.  Note the correct value for DebugAllocator
// checks is hardcoded.
static constexpr int64_t kDebugXorSuffix = -0x181fe80e0b464188LL;
alignas(kDefaultBufferAlignment) int64_t zero_size_area[1] = {kDebugXorSuffix};
static uint8_t* const kZeroSizeArea = reinterpret_cast<uint8_t*>(&zero_size_area);

class MemoryPoolStats {
 public:
  MemoryPoolStats() : bytes_allocated_(0), max_memory_(0) {}

  int64_t max_memory() const { return max_memory_.load(); }

  int64_t bytes_allocated() const { return bytes_allocated_.load(); }

  int64_t total_bytes_allocated() const { return total_allocated_bytes_.load(); }

  int64_t num_allocations() const { return num_allocs_.load(); }

  inline void UpdateAllocatedBytes(int64_t diff, bool is_free = false) {
    auto allocated = bytes_allocated_.fetch_add(diff) + diff;
    // "maximum" allocated memory is ill-defined in multi-threaded code,
    // so don't try to be too rigorous here
    if (diff > 0 && allocated > max_memory_) {
      max_memory_ = allocated;
    }

    // Reallocations might just expand/contract the allocation in place or might
    // copy to a new location. We can't really know, so we just represent the
    // optimistic case.
    if (diff > 0) {
      total_allocated_bytes_ += diff;
    }

    // We count any reallocation as a allocation.
    if (!is_free) {
      num_allocs_ += 1;
    }
  }

 protected:
  std::atomic<int64_t> bytes_allocated_ = 0;
  std::atomic<int64_t> max_memory_ = 0;
  std::atomic<int64_t> total_allocated_bytes_ = 0;
  std::atomic<int64_t> num_allocs_ = 0;
};

// Helper class directing allocations to the standard system allocator.
class SystemAllocator {
 public:
  // Allocate memory according to the alignment requirements for Arrow
  // (as of May 2016 64 bytes)
  static Status AllocateAligned(int64_t size, int64_t alignment, uint8_t** out) {
    if (size == 0) {
      *out = kZeroSizeArea;
      return Status::OK();
    }
#ifdef _WIN32
    // Special code path for Windows
    *out = reinterpret_cast<uint8_t*>(
        _aligned_malloc(static_cast<size_t>(size), static_cast<size_t>(alignment)));
    if (!*out) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }
#else
    const int result =
        posix_memalign(reinterpret_cast<void**>(out), static_cast<size_t>(alignment),
                       static_cast<size_t>(size));
    if (result == ENOMEM) {
      return Status::OutOfMemory("malloc of size ", size, " failed");
    }

    if (result == EINVAL) {
      return Status::Invalid("invalid alignment parameter: ",
                             static_cast<size_t>(alignment));
    }
#endif
    return Status::OK();
  }

  static Status ReallocateAligned(int64_t old_size, int64_t new_size, int64_t alignment,
                                  uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == kZeroSizeArea) {
      DCHECK_EQ(old_size, 0);
      return AllocateAligned(new_size, alignment, ptr);
    }
    if (new_size == 0) {
      DeallocateAligned(previous_ptr, old_size, alignment);
      *ptr = kZeroSizeArea;
      return Status::OK();
    }
    // Note: We cannot use realloc() here as it doesn't guarantee alignment.

    // Allocate new chunk
    uint8_t* out = nullptr;
    QUIVER_RETURN_NOT_OK(AllocateAligned(new_size, alignment, &out));
    DCHECK(out);
    // Copy contents and release old memory chunk
    memcpy(out, *ptr, static_cast<size_t>(std::min(new_size, old_size)));
#ifdef _WIN32
    _aligned_free(*ptr);
#else
    free(*ptr);
#endif  // defined(_WIN32)
    *ptr = out;
    return Status::OK();
  }

  static void DeallocateAligned(uint8_t* ptr, int64_t size, int64_t /*alignment*/) {
    if (ptr == kZeroSizeArea) {
      DCHECK_EQ(size, 0);
    } else {
#ifdef _WIN32
      _aligned_free(ptr);
#else
      free(ptr);
#endif
    }
  }
};

}  // namespace

int64_t MemoryPool::max_memory() const { return -1; }

///////////////////////////////////////////////////////////////////////
// MemoryPool implementation that delegates its core duty
// to an Allocator class.

template <typename Allocator>
class BaseMemoryPoolImpl : public MemoryPool {
 public:
  ~BaseMemoryPoolImpl() override {}

  Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override {
    if (size < 0) {
      return Status::Invalid("negative malloc size");
    }
    if (static_cast<uint64_t>(size) >= std::numeric_limits<size_t>::max()) {
      return Status::OutOfMemory("malloc size overflows size_t");
    }
    QUIVER_RETURN_NOT_OK(Allocator::AllocateAligned(size, alignment, out));

    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                    uint8_t** ptr) override {
    if (new_size < 0) {
      return Status::Invalid("negative realloc size");
    }
    if (static_cast<uint64_t>(new_size) >= std::numeric_limits<size_t>::max()) {
      return Status::OutOfMemory("realloc overflows size_t");
    }
    QUIVER_RETURN_NOT_OK(
        Allocator::ReallocateAligned(old_size, new_size, alignment, ptr));

    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size, int64_t alignment) override {
    Allocator::DeallocateAligned(buffer, size, alignment);

    stats_.UpdateAllocatedBytes(-size, /*is_free*/ true);
  }

  int64_t bytes_allocated() const override { return stats_.bytes_allocated(); }

  int64_t max_memory() const override { return stats_.max_memory(); }

  int64_t total_bytes_allocated() const override {
    return stats_.total_bytes_allocated();
  }

  int64_t num_allocations() const override { return stats_.num_allocations(); }

 protected:
  MemoryPoolStats stats_;
};

class SystemMemoryPool : public BaseMemoryPoolImpl<SystemAllocator> {
 public:
  std::string backend_name() const override { return "system"; }
};

MemoryPool* DefaultMemoryPool() {
  static SystemMemoryPool default_pool;
  return &default_pool;
}

}  // namespace quiver
