#include <benchmark/benchmark.h>

#include <iostream>
#include <memory>
#include <numeric>
#include <random>

#include "quiver/benchutil/main.h"
#include "quiver/datagen/datagen.h"
#include "quiver/hashtable/hashtable.h"
#include "quiver/util/literals.h"

namespace quiver {

using namespace util::literals;

namespace {

std::mt19937& Rng() {
  static std::mt19937 rng;
  return rng;
}

void BM_HashRows(benchmark::State& state) {
  constexpr int64_t kBatchSize = 32_KiLL;
  constexpr int64_t kTotalSize = 1_MiLL;
  constexpr int32_t kHashWidthBytes = sizeof(int64_t);
  static_assert(kTotalSize % kBatchSize == 0,
                "kTotalSize must be a multiple of kBatchSize");

  datagen::GeneratedData data =
      datagen::Gen()->Field(datagen::Flat(kHashWidthBytes))->NRows(kTotalSize);

  ReadOnlyFlatArray hashes_arr = std::get<ReadOnlyFlatArray>(data.batch->array(0));
  std::span<const int64_t> hashes{
      reinterpret_cast<const int64_t*>(hashes_arr.values.data()), kTotalSize};

  std::vector<int64_t> row_ids_vec(kTotalSize);
  std::iota(row_ids_vec.begin(), row_ids_vec.end(), 0);
  std::span<const int64_t> row_ids = row_ids_vec;

  std::vector<int64_t> probe_hashes_vec;
  probe_hashes_vec.assign(hashes.begin(), hashes.end());
  std::sort(probe_hashes_vec.begin(), probe_hashes_vec.end());
  auto last = std::unique(probe_hashes_vec.begin(), probe_hashes_vec.end());
  probe_hashes_vec.erase(last, probe_hashes_vec.end());
  std::shuffle(probe_hashes_vec.begin(), probe_hashes_vec.end(), Rng());

  std::span<const int64_t> probe_hashes = probe_hashes_vec;

  std::vector<int64_t> out_row_ids_vec(kBatchSize);
  std::span<int64_t> out_row_ids = out_row_ids_vec;
  std::vector<int32_t> out_hash_idx_vec(kBatchSize);
  std::span<int32_t> out_hash_idx = out_hash_idx_vec;

  for (auto _iter : state) {
    std::unique_ptr<hashtable::HashTable> hashtable = hashtable::HashTable::MakeStl();
    uint64_t offset = 0;
    while (offset < kTotalSize) {
      std::span<const int64_t> batch_hashes{hashes.data() + offset, kBatchSize};
      std::span<const int64_t> batch_row_ids{row_ids.data() + offset, kBatchSize};
      hashtable->Encode(batch_hashes, batch_row_ids);
      offset += kBatchSize;
    }

    int64_t row_ids_read = 0;
    offset = 0;
    while (offset < probe_hashes_vec.size()) {
      int64_t length = 0;
      int64_t hash_idx_offset = 0;
      int64_t bucket_idx_offset = 0;

      uint64_t batch_size =
          std::min(static_cast<uint64_t>(kBatchSize), probe_hashes_vec.size() - offset);
      std::span<const int64_t> batch_probe_hashes{probe_hashes.data() + offset,
                                                  batch_size};
      while (!hashtable->Decode(batch_probe_hashes, out_hash_idx, out_row_ids, &length,
                                &hash_idx_offset, &bucket_idx_offset)) {
        benchmark::DoNotOptimize(out_row_ids.data());
        row_ids_read += length;
      }
      row_ids_read += length;
      offset += batch_size;
    }
  }
  state.SetItemsProcessed(int64_t(state.iterations()) * kTotalSize);
}

}  // namespace

}  // namespace quiver

// Register the function as a benchmark
BENCHMARK(quiver::BM_HashRows);
QUIVER_BENCHMARK_MAIN();
