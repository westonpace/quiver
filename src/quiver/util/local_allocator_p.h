#pragma once

#include <cstdint>

#include "quiver/core/array.h"

namespace quiver::util {

class LocalAllocator;

template <typename T>
class local_ptr {
 public:
  ~local_ptr();
  local_ptr(const local_ptr&) = delete;
  local_ptr& operator=(const local_ptr&) = delete;
  local_ptr(local_ptr&& other) noexcept {
    data_ = other.data_;
    allocator_ = other.allocator_;
    allocation_id_ = other.allocation_id_;
    size_ = other.size_;
    other.allocator_ = nullptr;
  }
  local_ptr& operator=(local_ptr&& other) noexcept {
    data_ = other.data_;
    allocator_ = other.allocator_;
    allocation_id_ = other.allocation_id_;
    size_ = other.size_;
    other.allocator_ = nullptr;
    return *this;
  }

  T& operator*() { return data_; }
  T* operator->() { return &data_; }
  T* get() { return &data_; }

 private:
  local_ptr(T data, LocalAllocator* allocator, int32_t allocation_id, int64_t size)
      : data_(data), allocator_(allocator), allocation_id_(allocation_id), size_(size) {}

  T data_;
  LocalAllocator* allocator_;
  int32_t allocation_id_;
  int64_t size_;
  friend LocalAllocator;
};

class LocalAllocator {
 public:
  static constexpr int64_t kDefaultChunkSize = 1024LL * 1024LL;
  static constexpr int64_t kDefaultInitialSize = 16LL * kDefaultChunkSize;

  LocalAllocator(int64_t initial_size = kDefaultInitialSize,
                 int64_t chunk_size = kDefaultChunkSize)
      : chunk_size_(chunk_size), data_(initial_size) {}

  local_ptr<FlatArray> AllocateFlatArray(int32_t data_width_bytes, int64_t num_elements,
                                         bool allocate_validity = true);

  template <typename T>
  local_ptr<std::span<T>> AllocateSpan(int64_t num_elements) {
    int64_t num_bytes = num_elements * sizeof(T);
    uint8_t* buffer = AllocateBuffer(num_bytes);
    std::span<T> span = {reinterpret_cast<T*>(buffer),
                         static_cast<std::size_t>(num_elements)};
    return local_ptr<std::span<T>>(span, this, allocation_id_counter_++, num_bytes);
  }

  [[nodiscard]] int64_t num_allocations_performed() const {
    return allocations_performed_;
  }

  [[nodiscard]] int64_t capacity() const { return static_cast<int64_t>(data_.size()); }

 private:
  uint8_t* AllocateBuffer(int64_t size_bytes);
  void Free(int32_t allocation_id, int64_t size);

  const int64_t chunk_size_;
  std::vector<uint8_t> data_;
  std::vector<std::vector<uint8_t>> chunks_;
  int32_t allocation_id_counter_ = 0;
  int64_t total_allocated_bytes_ = 0;
  int64_t desired_allocated_bytes_ = -1;
  // Track the # of allocations performed for unit tests & debugging
  int64_t allocations_performed_ = 1;
  template <typename T>
  friend class local_ptr;
};

template <typename T>
local_ptr<T>::~local_ptr() {
  if (allocator_ != nullptr) {
    allocator_->Free(allocation_id_, size_);
  }
}

}  // namespace quiver::util
