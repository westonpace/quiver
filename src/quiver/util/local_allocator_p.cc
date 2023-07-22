#include "quiver/util/local_allocator_p.h"

#include <cstdint>
#include <iostream>

#include "quiver/util/bit_util.h"

namespace quiver::util {

local_ptr<FlatArray> LocalAllocator::AllocateFlatArray(int32_t data_width_bytes,
                                                       int64_t num_elements,
                                                       bool allocate_validity) {
  int64_t num_validity_bytes = bit_util::CeilDiv(num_elements, 8LL);
  int64_t num_value_bytes = num_elements * data_width_bytes;
  if (data_width_bytes == 0) {
    num_value_bytes = num_validity_bytes;
  }
  uint8_t* values_buf = AllocateBuffer(num_value_bytes);
  std::span<uint8_t> values(values_buf, num_value_bytes);
  int64_t num_allocated_bytes = num_value_bytes;

  std::span<uint8_t> validity;
  if (allocate_validity) {
    uint8_t* validity_buf = AllocateBuffer(num_validity_bytes);
    validity = {validity_buf, static_cast<std::size_t>(num_validity_bytes)};
    num_allocated_bytes += num_validity_bytes;
  }

  return local_ptr<FlatArray>(FlatArray{validity, values, data_width_bytes, num_elements},
                              this, allocation_id_counter_++, num_allocated_bytes);
}

uint8_t* LocalAllocator::AllocateBuffer(int64_t size_bytes) {
  int64_t start = total_allocated_bytes_;
  total_allocated_bytes_ += size_bytes;
  if (total_allocated_bytes_ > static_cast<int64_t>(data_.size())) {
    desired_allocated_bytes_ = total_allocated_bytes_;
    // We cannot immediately resize because that would invalidate all previously returned
    // pointers Instead we enter "on-demand mode" and allocate a new chunk for every
    // request.  Once the local allocator is idle again we can resize.
    allocations_performed_++;
    chunks_.emplace_back(size_bytes);
    return chunks_.back().data();
  }

  return data_.data() + start;
}

void LocalAllocator::Free(int32_t allocation_id, int64_t size) {
  DCHECK_EQ(allocation_id, allocation_id_counter_ - 1);
  if QUIVER_UNLIKELY (!chunks_.empty()) {
    chunks_.pop_back();
  }
  total_allocated_bytes_ -= size;
  if QUIVER_UNLIKELY (--allocation_id_counter_ == 0) {
    DCHECK_EQ(total_allocated_bytes_, 0);
    // We are idle again, resize the buffer if we need to
    if (desired_allocated_bytes_ >= 0) {
      int64_t new_size = bit_util::RoundUp(desired_allocated_bytes_, chunk_size_);
      allocations_performed_++;
      data_.resize(new_size);
      desired_allocated_bytes_ = -1;
    }
  }
}

}  // namespace quiver::util
