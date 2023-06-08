#include "quiver/util/local_allocator_p.h"

#include <cstdint>

#include "quiver/util/bit_util.h"

namespace quiver::util {

LocalAllocator::LocalAllocator() : data_(kInitialSize) {}

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
    int64_t new_size = bit_util::CeilDiv(total_allocated_bytes_, kChunkSize) * kChunkSize;
    data_.resize(new_size);
  }

  return data_.data() + start;
}

void LocalAllocator::Free(int32_t allocation_id, int64_t size) {
  DCHECK_EQ(allocation_id, allocation_id_counter_ - 1);
  allocation_id_counter_--;
  total_allocated_bytes_ -= size;
}

}  // namespace quiver::util
