#include "quiver/util/local_allocator_p.h"

#include <gtest/gtest.h>

#include <cstring>

namespace quiver::util {

template <typename T>
void EnsureSpanValid(std::span<T> span) {
  // There is no way to actually validate a pointer is valid so we just
  // memcpy all the bytes to make sure we can read them.
  std::vector<uint8_t> dest(span.size_bytes());
  std::memcpy(dest.data(), span.data(), span.size_bytes());
}

TEST(LocalAllocator, Resize) {
  LocalAllocator allocator(/*initial_size=*/64, /*chunk_size=*/1);
  ASSERT_EQ(allocator.num_allocations_performed(), 1);
  {
    local_ptr<std::span<uint8_t>> some_bytes = allocator.AllocateSpan<uint8_t>(32);
    EnsureSpanValid(*some_bytes);
    ASSERT_EQ(64, allocator.capacity());
    // This should force an allocation and put us into chunk-per-allocation-mode
    local_ptr<std::span<uint8_t>> more_bytes = allocator.AllocateSpan<uint8_t>(64);
    EnsureSpanValid(*some_bytes);
    EnsureSpanValid(*more_bytes);
    ASSERT_EQ(2, allocator.num_allocations_performed());
    // The reported capacity doesn't actually change until we resize
    ASSERT_EQ(64, allocator.capacity());
    // Even a tiny allocation at this point triggers an allocation
    local_ptr<std::span<uint8_t>> a_few_more = allocator.AllocateSpan<uint8_t>(4);
    ASSERT_EQ(3, allocator.num_allocations_performed());
    ASSERT_EQ(64, allocator.capacity());
    EnsureSpanValid(*some_bytes);
    EnsureSpanValid(*more_bytes);
    EnsureSpanValid(*a_few_more);
  }
  ASSERT_EQ(100, allocator.capacity());
  ASSERT_EQ(4, allocator.num_allocations_performed());
  // Now that we've resized internally we shouldn't trigger an allocation
  local_ptr<std::span<uint8_t>> all_at_once = allocator.AllocateSpan<uint8_t>(100);
  EnsureSpanValid(*all_at_once);
  ASSERT_EQ(4, allocator.num_allocations_performed());
}

}  // namespace quiver::util
