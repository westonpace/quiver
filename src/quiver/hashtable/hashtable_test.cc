#include "quiver/hashtable/hashtable.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <numeric>

#include "quiver/testutil/test_util.h"
#include "quiver/util/literals.h"
#include "quiver/util/local_allocator_p.h"
#include "quiver/util/random.h"

using namespace quiver::util::literals;

namespace quiver::hashtable {

TEST(StlHashTable, Basic) {
  util::LocalAllocator local_alloc;
  util::local_ptr<std::span<int64_t>> hashes =
      LocalSpan<int64_t>(&local_alloc, {1, 5, 17, 17, 3, 0});
  util::local_ptr<std::span<int64_t>> row_ids =
      LocalSpan<int64_t>(&local_alloc, {0, 1, 2, 3, 4, 5});

  std::unique_ptr<HashTable> hashtable = HashTable::MakeStl();

  hashtable->Encode(*hashes, *row_ids);

  // Two matches, one of them matches more than one row
  {
    util::local_ptr<std::span<int64_t>> probe_hashes =
        LocalSpan<int64_t>(&local_alloc, {17, 5});
    util::local_ptr<std::span<int64_t>> row_ids_out =
        local_alloc.AllocateSpan<int64_t>(3);
    int64_t hash_idx_offset = 0;
    int64_t bucket_idx_offset = 0;
    int64_t length = 0;

    ASSERT_TRUE(hashtable->Decode(*probe_hashes, *row_ids_out, &length, &hash_idx_offset,
                                  &bucket_idx_offset));

    // std::multimap is not stable when there are multiple matches for the same key
    util::local_ptr<std::span<int64_t>> possibility_one =
        LocalSpan<int64_t>(&local_alloc, {2, 3, 1});
    util::local_ptr<std::span<int64_t>> possibility_two =
        LocalSpan<int64_t>(&local_alloc, {3, 2, 1});

    bool equals_one = std::equal(row_ids_out->begin(), row_ids_out->end(),
                                 possibility_one->begin(), possibility_one->end());
    bool equals_two = std::equal(row_ids_out->begin(), row_ids_out->end(),
                                 possibility_two->begin(), possibility_two->end());
    ASSERT_TRUE(equals_one | equals_two);
    ASSERT_EQ(length, 3);
  }

  // No values match
  {
    util::local_ptr<std::span<int64_t>> probe_hashes =
        LocalSpan<int64_t>(&local_alloc, {100, 1000});
    util::local_ptr<std::span<int64_t>> row_ids_out =
        local_alloc.AllocateSpan<int64_t>(1);
    int64_t hash_idx_offset = 0;
    int64_t bucket_idx_offset = 0;
    int64_t length = 0;

    ASSERT_TRUE(hashtable->Decode(*probe_hashes, *row_ids_out, &length, &hash_idx_offset,
                                  &bucket_idx_offset));

    ASSERT_EQ(length, 0);
  }

  // Paging
  {
    util::local_ptr<std::span<int64_t>> probe_hashes =
        LocalSpan<int64_t>(&local_alloc, {17, 5});
    util::local_ptr<std::span<int64_t>> row_ids_out =
        local_alloc.AllocateSpan<int64_t>(1);
    int64_t hash_idx_offset = 0;
    int64_t bucket_idx_offset = 0;
    int64_t length = 0;

    ASSERT_FALSE(hashtable->Decode(*probe_hashes, *row_ids_out, &length, &hash_idx_offset,
                                   &bucket_idx_offset));
    ASSERT_EQ(1, length);
    ASSERT_THAT(*row_ids_out->data(), ::testing::AnyOf(2, 3));
    ASSERT_EQ(0, hash_idx_offset);
    ASSERT_EQ(1, bucket_idx_offset);

    ASSERT_FALSE(hashtable->Decode(*probe_hashes, *row_ids_out, &length, &hash_idx_offset,
                                   &bucket_idx_offset));
    ASSERT_EQ(1, length);
    ASSERT_THAT(*row_ids_out->data(), ::testing::AnyOf(2, 3));
    ASSERT_EQ(1, hash_idx_offset);
    ASSERT_EQ(0, bucket_idx_offset);

    ASSERT_TRUE(hashtable->Decode(*probe_hashes, *row_ids_out, &length, &hash_idx_offset,
                                  &bucket_idx_offset));
    ASSERT_EQ(1, length);
    ASSERT_EQ(1, *row_ids_out->data());
  }
}

TEST(StlHashTable, Random) {
  constexpr int64_t kNumRows = 1_MiLL;
  constexpr int64_t kBatchSize = 32_KiLL;
  util::LocalAllocator local_alloc;
  util::local_ptr<std::span<int64_t>> hashes =
      local_alloc.AllocateSpan<int64_t>(kNumRows);
  util::local_ptr<std::span<int64_t>> row_ids =
      local_alloc.AllocateSpan<int64_t>(kNumRows);

  util::RandomLongs(*hashes);
  std::iota(row_ids->begin(), row_ids->end(), 0);

  std::unique_ptr<HashTable> hashtable = HashTable::MakeStl();

  // Encode all the hashes
  uint64_t offset = 0;
  while (offset < kNumRows) {
    std::span<const int64_t> batch_hashes{hashes->data() + offset, kBatchSize};
    std::span<const int64_t> batch_row_ids{row_ids->data() + offset, kBatchSize};
    hashtable->Encode(batch_hashes, batch_row_ids);
    offset += kBatchSize;
  }

  // Decode the hashes in reverse order
  util::local_ptr<std::span<int64_t>> reversed_hashes =
      local_alloc.AllocateSpan<int64_t>(kNumRows);
  std::memcpy(reversed_hashes->data(), hashes->data(), kNumRows * sizeof(int64_t));
  std::reverse(reversed_hashes->begin(), reversed_hashes->end());

  offset = 0;
  util::local_ptr<std::span<int64_t>> out_row_ids =
      local_alloc.AllocateSpan<int64_t>(kBatchSize);
  std::vector<int64_t> decoded_row_ids;
  decoded_row_ids.reserve(kNumRows);
  while (offset < kNumRows) {
    int64_t length = 0;
    int64_t hash_idx_offset = 0;
    int64_t bucket_idx_offset = 0;
    std::span<const int64_t> batch_probe_hashes{reversed_hashes->data() + offset,
                                                kBatchSize};
    while (!hashtable->Decode(batch_probe_hashes, *out_row_ids, &length, &hash_idx_offset,
                              &bucket_idx_offset)) {
      for (int i = 0; i < length; i++) {
        decoded_row_ids.push_back(out_row_ids->data()[i]);
      }
    }
    offset += kBatchSize;
  }

  ASSERT_EQ(kNumRows, decoded_row_ids.size());
}

}  // namespace quiver::hashtable