#include <benchmark/benchmark.h>

#include <iostream>
#include <memory>
#include <numeric>
#include <random>

#include "quiver/benchutil/main.h"
#include "quiver/datagen/datagen.h"
#include "quiver/map/equality.h"
#include "quiver/util/bit_util.h"
#include "quiver/util/literals.h"
#include "quiver/util/local_allocator_p.h"

namespace quiver::map {

using namespace util::literals;

namespace {

void BM_EqualityEquals(benchmark::State& state) {
  constexpr int64_t kTotalSize = 1_MiLL;
  const int32_t field_width_bytes = state.range(0);

  datagen::GeneratedData data =
      datagen::Gen()->Field(datagen::Flat(field_width_bytes))->NRows(kTotalSize);

  util::LocalAllocator alloc;
  util::local_ptr<std::span<uint8_t>> scratch =
      alloc.AllocateSpan<uint8_t>(bit_util::CeilDiv(kTotalSize, 8LL));

  std::unique_ptr<EqualityComparer> comparer =
      EqualityComparer::MakeSimpleBinaryEqualityComparer();

  for (auto _iter : state) {
    comparer->CompareEquality(data.batch->array(0), data.batch->array(0), *scratch)
        .AbortNotOk();
    benchmark::DoNotOptimize(scratch->data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * data.batch->NumBytes() * 2);
}

void BM_EqualityUnequals(benchmark::State& state) {
  constexpr int64_t kTotalSize = 1_MiLL;
  const int32_t field_width_bytes = state.range(0);

  datagen::GeneratedData lhs =
      datagen::Gen()->Field(datagen::Flat(field_width_bytes))->NRows(kTotalSize);
  datagen::GeneratedData rhs =
      datagen::Gen()->Field(datagen::Flat(field_width_bytes))->NRows(kTotalSize);

  util::LocalAllocator alloc;
  util::local_ptr<std::span<uint8_t>> scratch =
      alloc.AllocateSpan<uint8_t>(bit_util::CeilDiv(kTotalSize, 8LL));

  std::unique_ptr<EqualityComparer> comparer =
      EqualityComparer::MakeSimpleBinaryEqualityComparer();

  for (auto _iter : state) {
    comparer->CompareEquality(lhs.batch->array(0), rhs.batch->array(0), *scratch)
        .AbortNotOk();
    benchmark::DoNotOptimize(scratch->data());
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * lhs.batch->NumBytes() +
                          rhs.batch->NumBytes());
}

}  // namespace

}  // namespace quiver::map

// Register the function as a benchmark
BENCHMARK(quiver::map::BM_EqualityEquals)
    ->ArgName("FieldWidthBytes")
    ->Arg(1)
    ->Arg(4)
    ->Arg(8);
BENCHMARK(quiver::map::BM_EqualityUnequals)
    ->ArgName("FieldWidthBytes")
    ->Arg(1)
    ->Arg(4)
    ->Arg(8);
QUIVER_BENCHMARK_MAIN();
