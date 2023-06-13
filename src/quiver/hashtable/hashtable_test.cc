#include "quiver/hashtable/hashtable.h"

#include <gtest/gtest.h>

#include <memory>

#include "quiver/testutil/test_util.h"
#include "quiver/util/local_allocator_p.h"

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

    util::local_ptr<std::span<int64_t>> expected_row_ids =
        LocalSpan<int64_t>(&local_alloc, {2, 3, 1});
    ASSERT_TRUE(std::equal(row_ids_out->begin(), row_ids_out->end(),
                           expected_row_ids->begin(), expected_row_ids->end()));
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
    ASSERT_EQ(2, *row_ids_out->data());
    ASSERT_EQ(0, hash_idx_offset);
    ASSERT_EQ(1, bucket_idx_offset);

    ASSERT_FALSE(hashtable->Decode(*probe_hashes, *row_ids_out, &length, &hash_idx_offset,
                                   &bucket_idx_offset));
    ASSERT_EQ(1, length);
    ASSERT_EQ(3, *row_ids_out->data());
    ASSERT_EQ(1, hash_idx_offset);
    ASSERT_EQ(0, bucket_idx_offset);

    ASSERT_TRUE(hashtable->Decode(*probe_hashes, *row_ids_out, &length, &hash_idx_offset,
                                  &bucket_idx_offset));
    ASSERT_EQ(1, length);
    ASSERT_EQ(1, *row_ids_out->data());
  }
}

}  // namespace quiver::hashtable
