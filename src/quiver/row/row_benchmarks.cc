#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <benchmark/benchmark.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <numeric>

#include "quiver/benchutil/datagen.h"
#include "quiver/core/array.h"
#include "quiver/core/arrow.h"
#include "quiver/core/io.h"
#include "quiver/datagen/datagen.h"
#include "quiver/row/row_p.h"
#include "quiver/util/arrow_util.h"
#include "quiver/util/literals.h"
#include "quiver/util/local_allocator_p.h"
#include "quiver/util/random.h"

namespace quiver {

using namespace util::literals;
namespace {

constexpr int32_t kNumBytes = 64_Mi;

struct TestData {
  SimpleSchema schema;
  std::unique_ptr<ReadOnlyBatch> batch;
};

TestData CreateTestData() {
  TestData test_data;

  const std::shared_ptr<ArrowSchema>& batch_schema = bench::GetFlatDataSchema();
  SimpleSchema::ImportFromArrow(batch_schema.get(), &test_data.schema).AbortNotOk();

  util::OwnedArrowArray random_batch = bench::GenFlatData(kNumBytes);
  ImportBatch(random_batch.release(), &test_data.schema, &test_data.batch).AbortNotOk();

  return test_data;
}

const TestData& GetTestData() {
  static TestData test_data = CreateTestData();
  return test_data;
}

void DoSetup(const benchmark::State& _state) { GetTestData(); }

void BM_EncodeRows(benchmark::State& state) {
  auto* buf = new uint8_t[kNumBytes];
  std::span<uint8_t> buf_span(buf, kNumBytes);

  StreamSink sink = StreamSink::FromFixedSizeSpan(buf_span);

  const TestData& test_data = GetTestData();

  std::unique_ptr<row::RowEncoder> encoder;
  row::RowEncoder::Create(&test_data.schema, &sink, &encoder).AbortNotOk();

  for (auto _iter : state) {
    int64_t row_id = -1;
    encoder->Append(*test_data.batch, &row_id).AbortNotOk();
    benchmark::DoNotOptimize(buf);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(kNumBytes));
}

void BM_DecodeRows(benchmark::State& state) {
  constexpr int64_t kTotalSize = 256_KiLL;
  constexpr int32_t kHashWidthBytes = sizeof(int64_t);
  constexpr int32_t kMinFieldWidth = 1;
  constexpr int32_t kMaxFieldWidth = 8;

  auto num_fields = static_cast<int32_t>(state.range(0));

  datagen::GeneratedData data =
      datagen::Gen()
          ->NFieldsOf(num_fields, datagen::Flat(kMinFieldWidth, kMaxFieldWidth))
          ->NRows(kTotalSize);

  util::LocalAllocator local_alloc;

  {
    local_alloc.AllocateSpan<uint8_t>(data.batch->num_bytes() * 2LL +
                                      kTotalSize * static_cast<int64_t>(sizeof(int64_t)));
  }

  util::local_ptr<std::span<uint8_t>> scratch =
      local_alloc.AllocateSpan<uint8_t>(data.batch->num_bytes() * 2);

  StreamSink sink = StreamSink::FromFixedSizeSpan(*scratch);
  std::unique_ptr<row::RowEncoder> encoder;
  row::RowEncoder::Create(&data.schema, &sink, &encoder).AbortNotOk();

  int64_t row_id;
  encoder->Append(*data.batch, &row_id).AbortNotOk();

  RandomAccessSource source = RandomAccessSource::WrapSpan(*scratch);
  std::unique_ptr<row::RowDecoder> decoder;
  row::RowDecoder::Create(&data.schema, &source, &decoder).AbortNotOk();

  util::local_ptr<std::span<int64_t>> indices =
      local_alloc.AllocateSpan<int64_t>(kTotalSize);
  std::iota(indices->begin(), indices->end(), 0);
  util::Shuffle(*indices);
  std::unique_ptr<Batch> batch =
      Batch::CreateInitializedBasic(&data.schema, data.batch->num_bytes());

  for (auto _iter : state) {
    decoder->Load(*indices, batch.get()).AbortNotOk();
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * data.batch->num_bytes());
}

}  // namespace

}  // namespace quiver

// Register the function as a benchmark
BENCHMARK(quiver::BM_EncodeRows);
BENCHMARK(quiver::BM_DecodeRows)->Arg(4)->Arg(16)->Arg(64)->Arg(256);
BENCHMARK_MAIN();
