#pragma once

#include "quiver/core/array.h"

namespace quiver::util {

class LocalAllocator;

template <typename T>
class local_ptr {
 public:
  ~local_ptr();
  T& operator*() { return data_; }
  T* operator->() { return &data_; }

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
  LocalAllocator();
  local_ptr<FlatArray> AllocateFlatArray(int32_t data_width_bytes, int64_t num_elements,
                                         bool allocate_validity = true);

 private:
  static constexpr int64_t kChunkSize = 1024LL * 1024LL;
  static constexpr int64_t kInitialSize = 16LL * kChunkSize;

  uint8_t* AllocateBuffer(int64_t size_bytes);
  void Free(int32_t allocation_id, int64_t size);

  std::vector<uint8_t> data_;
  int32_t allocation_id_counter_ = 0;
  int64_t total_allocated_bytes_ = 0;
  template <typename T>
  friend class local_ptr;
};

template <typename T>
local_ptr<T>::~local_ptr() {
  allocator_->Free(allocation_id_, size_);
}

}  // namespace quiver::util
