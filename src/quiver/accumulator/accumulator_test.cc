// SPDX-License-Identifier: Apache-2.0

#include "quiver/accumulator/accumulator.h"

#include <gtest/gtest.h>

#include <cmath>

#include "quiver/datagen/datagen.h"
#include "quiver/testutil/test_util.h"
#include "quiver/util/bit_util.h"
#include "quiver/util/literals.h"
#include "quiver/util/random.h"

using namespace quiver::util::literals;

namespace quiver::accum {

TEST(Accumulator, Random) {
  constexpr int32_t kRowWidthBytes = 64;
  constexpr int64_t kNumRows = 1_MiLL - 1;
  constexpr int32_t kBatchSize = 256_Ki;

  util::Seed(42);

  datagen::GeneratedData data = datagen::Gen()
                                    ->FlatFieldsWithNBytesTotalWidth(kRowWidthBytes, 1, 8)
                                    ->NRows(kNumRows);

  std::vector<std::unique_ptr<ReadOnlyBatch>> chunks;
  auto append_chunk = [&](std::unique_ptr<ReadOnlyBatch> chunk) {
    chunks.push_back(std::move(chunk));
    return Status::OK();
  };

  std::unique_ptr<Accumulator> accumulator =
      Accumulator::FixedMemory(data.schema.get(), kBatchSize, append_chunk);

  int64_t offset = 0;
  while (offset < kNumRows) {
    // 1Ki - 64Ki
    auto next_batch_size = static_cast<int32_t>(std::pow(2, util::RandInt(10, 17)));
    int64_t remaining = kNumRows - offset;
    next_batch_size =
        static_cast<int32_t>(std::min(remaining, static_cast<int64_t>(next_batch_size)));
    BatchView view = BatchView::SliceBatch(data.batch.get(), offset, next_batch_size);
    offset += next_batch_size;

    AssertOk(accumulator->InsertBatch(&view));
  }
  AssertOk(accumulator->Finish());

  ASSERT_EQ(bit_util::CeilDiv(static_cast<int32_t>(kNumRows), kBatchSize), chunks.size());

  for (int chunk_idx = 0; chunk_idx < static_cast<int32_t>(chunks.size()); chunk_idx++) {
    const std::unique_ptr<ReadOnlyBatch>& chunk = chunks[chunk_idx];
    BatchView expected = BatchView::SliceBatch(
        data.batch.get(), static_cast<int64_t>(chunk_idx) * kBatchSize, kBatchSize);
    ASSERT_TRUE(expected.BinaryEquals(*chunk));
  }
}

}  // namespace quiver::accum