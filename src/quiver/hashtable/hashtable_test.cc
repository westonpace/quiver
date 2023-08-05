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
    util::local_ptr<std::span<int32_t>> hash_idx_out =
        local_alloc.AllocateSpan<int32_t>(3);
    int64_t hash_idx_offset = 0;
    int64_t bucket_idx_offset = 0;
    int64_t length = 0;

    ASSERT_TRUE(hashtable->Decode(*probe_hashes, *hash_idx_out, *row_ids_out, &length,
                                  &hash_idx_offset, &bucket_idx_offset));

    // std::multimap is not stable when there are multiple matches for the same key
    ASSERT_THAT(*row_ids_out, testing::UnorderedElementsAre(2, 3, 1));
    ASSERT_THAT(*hash_idx_out, testing::ElementsAre(0, 0, 1));

    ASSERT_EQ(length, 3);
  }

  // No values match
  {
    util::local_ptr<std::span<int64_t>> probe_hashes =
        LocalSpan<int64_t>(&local_alloc, {100, 1000});
    util::local_ptr<std::span<int64_t>> row_ids_out =
        local_alloc.AllocateSpan<int64_t>(1);
    util::local_ptr<std::span<int32_t>> hash_idx_out =
        local_alloc.AllocateSpan<int32_t>(1);
    int64_t hash_idx_offset = 0;
    int64_t bucket_idx_offset = 0;
    int64_t length = 0;

    ASSERT_TRUE(hashtable->Decode(*probe_hashes, *hash_idx_out, *row_ids_out, &length,
                                  &hash_idx_offset, &bucket_idx_offset));

    ASSERT_EQ(length, 0);
  }

  // Paging
  {
    util::local_ptr<std::span<int64_t>> probe_hashes =
        LocalSpan<int64_t>(&local_alloc, {17, 5});
    util::local_ptr<std::span<int64_t>> row_ids_out =
        local_alloc.AllocateSpan<int64_t>(1);
    util::local_ptr<std::span<int32_t>> hash_idx_out =
        local_alloc.AllocateSpan<int32_t>(1);
    int64_t hash_idx_offset = 0;
    int64_t bucket_idx_offset = 0;
    int64_t length = 0;

    ASSERT_FALSE(hashtable->Decode(*probe_hashes, *hash_idx_out, *row_ids_out, &length,
                                   &hash_idx_offset, &bucket_idx_offset));
    ASSERT_EQ(1, length);
    ASSERT_THAT(*row_ids_out->data(), ::testing::AnyOf(2, 3));
    ASSERT_EQ(*hash_idx_out->data(), 0);
    ASSERT_EQ(0, hash_idx_offset);
    ASSERT_EQ(1, bucket_idx_offset);

    ASSERT_FALSE(hashtable->Decode(*probe_hashes, *hash_idx_out, *row_ids_out, &length,
                                   &hash_idx_offset, &bucket_idx_offset));
    ASSERT_EQ(1, length);
    ASSERT_THAT(*row_ids_out->data(), ::testing::AnyOf(2, 3));
    ASSERT_EQ(*hash_idx_out->data(), 0);
    ASSERT_EQ(1, hash_idx_offset);
    ASSERT_EQ(0, bucket_idx_offset);

    ASSERT_TRUE(hashtable->Decode(*probe_hashes, *hash_idx_out, *row_ids_out, &length,
                                  &hash_idx_offset, &bucket_idx_offset));
    ASSERT_EQ(1, length);
    ASSERT_EQ(1, *row_ids_out->data());
    ASSERT_EQ(*hash_idx_out->data(), 1);
  }
}

TEST(StlHashTable, Random) {
  constexpr int64_t kBatchSize = 8_KiLL;
  constexpr int64_t kNumRows = kBatchSize * 4;
  util::LocalAllocator local_alloc;
  util::local_ptr<std::span<int64_t>> hashes =
      local_alloc.AllocateSpan<int64_t>(kNumRows);
  util::local_ptr<std::span<int64_t>> row_ids =
      local_alloc.AllocateSpan<int64_t>(kNumRows);

  // Randomly generate some hashes, note there may be duplicates
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
  util::local_ptr<std::span<int32_t>> out_hash_idx =
      local_alloc.AllocateSpan<int32_t>(kBatchSize);
  int64_t num_decoded_rows = 0;
  while (offset < kNumRows) {
    int64_t length = 0;
    int64_t hash_idx_offset = 0;
    int64_t prev_hash_idx_offset = 0;
    int64_t bucket_idx_offset = 0;
    std::span<const int64_t> batch_probe_hashes{reversed_hashes->data() + offset,
                                                kBatchSize};
    auto assert_rows = [&] {
      for (int i = 0; i < length; i++) {
        num_decoded_rows++;
      }
      int64_t last_hash_id = prev_hash_idx_offset;
      // Every hash should match at least one row (in which case out_hash_idx would be
      // strictly monotonic) or mulitple rows (in which case there may be some duplicates)
      for (std::size_t i = 0; i < length; i++) {
        ASSERT_THAT(out_hash_idx->data()[i],
                    ::testing::AnyOf(last_hash_id, last_hash_id + 1));
        last_hash_id = out_hash_idx->data()[i];
      }
    };
    while (!hashtable->Decode(batch_probe_hashes, *out_hash_idx, *out_row_ids, &length,
                              &hash_idx_offset, &bucket_idx_offset)) {
      assert_rows();
      prev_hash_idx_offset = hash_idx_offset;
    }
    assert_rows();
    offset += kBatchSize;
  }

  // If there are duplicate hashes we will actually end up with more than kNumRows rows
  // because we will insert the duplicate multiple times and each time we do we will get
  // all matches
  ASSERT_LE(kNumRows, num_decoded_rows);
}

}  // namespace quiver::hashtable
