// SPDX-License-Identifier: Apache-2.0

#include "quiver/store/mmap_store.h"

#include <gtest/gtest.h>

#include <memory>

#include "quiver/util/gtest_util.h"

namespace quiver::store::mmap {

TEST(MmapStore, Invalid) {
  constexpr int32_t kNegativeBlockSizeBytes = -1;
  MmapStoreOptions bad_opts;
  bad_opts.block_size_bytes = kNegativeBlockSizeBytes;

  std::unique_ptr<MmapStore> out;

  ASSERT_RAISES(Invalid, MmapStore::Create(bad_opts, &out));
  ASSERT_FALSE(out);

  constexpr int32_t kValidBlockSizeBytes = 100;
  constexpr int64_t kNegativeTotalSizeBytes = -1LL;

  bad_opts.block_size_bytes = kValidBlockSizeBytes;
  bad_opts.total_size_bytes = kNegativeTotalSizeBytes;
  ASSERT_RAISES(Invalid, MmapStore::Create(bad_opts, &out));

  constexpr int32_t kValidBlockButLargerThanTotalBytes = 100;
  constexpr int64_t kValidTotalButSmallerThanBlockBytes = 10;
  bad_opts.block_size_bytes = kValidBlockButLargerThanTotalBytes;
  bad_opts.total_size_bytes = kValidTotalButSmallerThanBlockBytes;
  ASSERT_RAISES(Invalid, MmapStore::Create(bad_opts, &out));
}

TEST(MmapStore, Default) {
  std::unique_ptr<MmapStore> mmap_store;
  AssertOk(MmapStore::Create({}, &mmap_store));
  ASSERT_GT(mmap_store->block_size_bytes(), 0);
  ASSERT_GT(mmap_store->total_size_bytes(), 0);
}

}  // namespace quiver::store::mmap
