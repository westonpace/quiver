#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <benchmark/benchmark.h>

#include <iostream>
#include <memory>

#include "quiver/benchutil/datagen.h"
#include "quiver/core/array.h"
#include "quiver/core/arrow.h"
#include "quiver/core/io.h"
#include "quiver/row/row_p.h"
#include "quiver/util/arrow_util.h"

namespace quiver {
namespace {

constexpr int32_t kMiB = 1024 * 1024;
constexpr int32_t kNumBytes = 64 * kMiB;

struct TestData {
  SimpleSchema schema;
  std::unique_ptr<ReadOnlyBatch> batch;
};

TestData CreateTestData() {
  TestData test_data;

  const std::shared_ptr<ArrowSchema>& batch_schema = bench::GetFlatDataSchema();
  std::cout << "Import" << std::endl;
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

  Sink sink = Sink::FromFixedSizeSpan(buf_span);

  const TestData& test_data = GetTestData();

  row::RowSchema row_schema(8, 8);
  row_schema.Initialize(test_data.schema).AbortNotOk();

  std::unique_ptr<row::RowQueueAppendingProducer> encoder;
  row::RowQueueAppendingProducer::Create(&row_schema, &sink, &encoder).AbortNotOk();

  for (auto _iter : state) {
    encoder->Append(*test_data.batch).AbortNotOk();
    benchmark::DoNotOptimize(buf);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(kNumBytes));
}

}  // namespace

}  // namespace quiver

// Register the function as a benchmark
BENCHMARK(quiver::BM_EncodeRows)->ThreadPerCpu();
BENCHMARK(quiver::BM_EncodeRows)->Threads(8);
BENCHMARK(quiver::BM_EncodeRows);
BENCHMARK_MAIN();
