#include <benchmark/benchmark.h>

#include <memory>

#include "quiver/benchutil/main.h"
#include "quiver/datagen/datagen.h"
#include "quiver/hash/hasher.h"
#include "quiver/util/literals.h"

namespace quiver {

using namespace util::literals;

namespace {

void BM_HashRows(benchmark::State& state) {
  constexpr int32_t kNumDataFields = 4;
  constexpr int32_t kMinFlatWidth = 1;
  constexpr int32_t kMaxFlatWidth = 16;
  constexpr int64_t kNumRows = 1_MiLL;
  datagen::GeneratedData data =
      datagen::Gen()
          ->Field(datagen::Flat(hash::HashWidthBytes()))
          ->NFieldsOf(kNumDataFields, datagen::Flat(kMinFlatWidth, kMaxFlatWidth))
          ->NRows(kNumRows);

  std::unique_ptr<hash::Hasher> hasher = hash::Hasher::CreateIdentityHasher();
  std::vector<int64_t> hashes(kNumRows);
  std::span<int64_t> hash_out = hashes;

  for (auto _iter : state) {
    hasher->HashBatch(data.batch.get(), hash_out).AbortNotOk();
    benchmark::DoNotOptimize(hash_out.data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(data.batch->NumBytes()));
}

}  // namespace

}  // namespace quiver

// Register the function as a benchmark
BENCHMARK(quiver::BM_HashRows);
QUIVER_BENCHMARK_MAIN();
