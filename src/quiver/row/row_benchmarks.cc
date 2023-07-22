#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <benchmark/benchmark.h>
#include <fcntl.h>

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <numeric>

#include "quiver/benchutil/datagen.h"
#include "quiver/benchutil/main.h"
#include "quiver/core/array.h"
#include "quiver/core/arrow.h"
#include "quiver/core/io.h"
#include "quiver/datagen/datagen.h"
#include "quiver/row/row_p.h"
#include "quiver/testutil/test_util.h"
#include "quiver/testutil/tmpfiles.h"
#include "quiver/util/arrow_util.h"
#include "quiver/util/literals.h"
#include "quiver/util/local_allocator_p.h"
#include "quiver/util/random.h"

namespace quiver {

using namespace util::literals;
namespace {

#ifdef NDEBUG
constexpr int64_t kTotalSizeBytes = 64_MiLL;
#else
constexpr int64_t kTotalSizeBytes = 1_MiLL;
#endif
constexpr int32_t kMinFieldWidth = 1;
constexpr int32_t kMaxFieldWidth = 8;

struct TestData {
  SimpleSchema schema;
  std::unique_ptr<ReadOnlyBatch> batch;
};

TestData CreateTestData() {
  TestData test_data;

  const std::shared_ptr<ArrowSchema>& batch_schema = bench::GetFlatDataSchema();
  SimpleSchema::ImportFromArrow(batch_schema.get(), &test_data.schema).AbortNotOk();

  util::OwnedArrowArray random_batch = bench::GenFlatData(kTotalSizeBytes);
  ImportBatch(random_batch.release(), &test_data.schema, &test_data.batch).AbortNotOk();

  return test_data;
}

const TestData& GetTestData() {
  static TestData test_data = CreateTestData();
  return test_data;
}

void DoSetup(const benchmark::State& _state) { GetTestData(); }

void BM_EncodeMemory(benchmark::State& state) {
  std::unique_ptr<Storage> storage = TestStorage(kTotalSizeBytes * 2);

  const TestData& test_data = GetTestData();

  std::unique_ptr<row::RowEncoder> encoder;
  row::RowEncoder::Create(&test_data.schema, storage.get(), false, &encoder).AbortNotOk();

  for (auto _iter : state) {
    int64_t row_id = -1;
    encoder->Append(*test_data.batch, &row_id).AbortNotOk();
    encoder->Reset().AbortNotOk();
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(kTotalSizeBytes));
}

// void BM_EncodeFile(benchmark::State& state) {
//   std::unique_ptr<Storage> storage = TestStorage(kTotalSizeBytes);

//   const TestData& test_data = GetTestData();

//   std::unique_ptr<row::RowEncoder> encoder;
//   row::RowEncoder::Create(&test_data.schema, storage.get(), false,
//   &encoder).AbortNotOk();

//   for (auto _iter : state) {
//     int64_t row_id = -1;
//     encoder->Append(*test_data.batch, &row_id).AbortNotOk();
//   }
//   state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(kTotalSizeBytes));
// }

void BenchDecodeMemory(const datagen::GeneratedData& data, benchmark::State& state,
                       bool staged) {
  util::LocalAllocator local_alloc;
  int64_t num_rows = data.batch->length();

  {
    local_alloc.AllocateSpan<uint8_t>(data.batch->NumBytes() * 2LL +
                                      num_rows * static_cast<int64_t>(sizeof(int64_t)));
  }

  std::unique_ptr<Storage> storage = TestStorage(data.batch->NumBytes() * 2);

  std::unique_ptr<row::RowEncoder> encoder;
  row::RowEncoder::Create(data.schema.get(), storage.get(), false, &encoder).AbortNotOk();

  int64_t row_id;
  encoder->Append(*data.batch, &row_id).AbortNotOk();

  std::unique_ptr<row::RowDecoder> decoder;
  if (staged) {
    row::RowDecoder::CreateStaged(data.schema.get(), storage.get(), &decoder)
        .AbortNotOk();
  } else {
    row::RowDecoder::Create(data.schema.get(), storage.get(), &decoder).AbortNotOk();
  }

  util::local_ptr<std::span<int64_t>> indices =
      local_alloc.AllocateSpan<int64_t>(num_rows);
  std::iota(indices->begin(), indices->end(), 0);
  util::Shuffle(*indices);
  std::unique_ptr<Batch> batch =
      Batch::CreateInitializedBasic(data.schema.get(), data.batch->NumBytes());

  for (auto _iter : state) {
    decoder->Load(*indices, batch.get()).AbortNotOk();
  }
}

void BenchDecodeFile(const datagen::GeneratedData& data, benchmark::State& state,
                     bool staged) {
  util::LocalAllocator local_alloc;
  int64_t num_rows = data.batch->length();

  {
    local_alloc.AllocateSpan<uint8_t>(data.batch->NumBytes() * 2LL +
                                      num_rows * static_cast<int64_t>(sizeof(int64_t)));
  }

  std::unique_ptr<Storage> storage = TmpFileStorage(/*direct_io=*/true);

  std::unique_ptr<row::RowEncoder> encoder;
  row::RowEncoder::Create(data.schema.get(), storage.get(), false, &encoder).AbortNotOk();

  int64_t row_id;
  encoder->Append(*data.batch, &row_id).AbortNotOk();

  std::unique_ptr<row::RowDecoder> decoder;
  if (staged) {
    row::RowDecoder::CreateStaged(data.schema.get(), storage.get(), &decoder)
        .AbortNotOk();
  } else {
    row::RowDecoder::Create(data.schema.get(), storage.get(), &decoder).AbortNotOk();
  }

  util::local_ptr<std::span<int64_t>> indices =
      local_alloc.AllocateSpan<int64_t>(num_rows);
  std::iota(indices->begin(), indices->end(), 0);
  util::Shuffle(*indices);
  std::unique_ptr<Batch> batch =
      Batch::CreateInitializedBasic(data.schema.get(), data.batch->NumBytes());

  for (auto _iter : state) {
    decoder->Load(*indices, batch.get()).AbortNotOk();
  }
}

void BenchDecodeIoUring(const datagen::GeneratedData& data, benchmark::State& state) {
  util::LocalAllocator local_alloc;
  int64_t num_rows = data.batch->length();

  {
    local_alloc.AllocateSpan<uint8_t>(data.batch->NumBytes() * 2LL +
                                      num_rows * static_cast<int64_t>(sizeof(int64_t)));
  }

  std::unique_ptr<Storage> storage = TmpFileStorage(/*direct_io=*/true);

  // int scratch_file = fileno(std::tmpfile());
  int scratch_file =
      open("/tmp/scratch_file", O_TRUNC | O_DIRECT | O_RDWR | O_CREAT, 0644);
  QUIVER_CHECK_GE(scratch_file, 0) << "scratch file open failed " << strerror(errno);

  std::unique_ptr<row::RowEncoder> encoder;
  row::RowEncoder::Create(data.schema.get(), storage.get(), true, &encoder).AbortNotOk();

  int64_t row_id;
  encoder->Append(*data.batch, &row_id).AbortNotOk();
  AssertOk(encoder->Finish());

  std::unique_ptr<row::RowDecoder> decoder;
  row::RowDecoder::CreateIoUring(data.schema.get(), storage.get(), &decoder).AbortNotOk();

  util::local_ptr<std::span<int64_t>> indices =
      local_alloc.AllocateSpan<int64_t>(num_rows);
  std::iota(indices->begin(), indices->end(), 0);
  util::Shuffle(*indices);
  std::unique_ptr<Batch> batch =
      Batch::CreateInitializedBasic(data.schema.get(), data.batch->NumBytes());

  for (auto _iter : state) {
    decoder->Load(*indices, batch.get()).AbortNotOk();
  }

  assert(close(scratch_file) == 0);
}

void BM_DecodeRowsMemory(benchmark::State& state) {
  auto row_width_bytes = static_cast<int32_t>(state.range(0));
  auto num_rows = kTotalSizeBytes / row_width_bytes;

  datagen::GeneratedData data = datagen::Gen()
                                    ->FlatFieldsWithNBytesTotalWidth(
                                        row_width_bytes, kMinFieldWidth, kMaxFieldWidth)
                                    ->NRows(num_rows);

  BenchDecodeMemory(data, state, false);
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSizeBytes);
}

void BM_DecodeRowsMemoryOneWideField(benchmark::State& state) {
  auto row_width_bytes = static_cast<int32_t>(state.range(0));
  auto num_rows = kTotalSizeBytes / row_width_bytes;

  datagen::GeneratedData data =
      datagen::Gen()->Field(datagen::Flat(row_width_bytes))->NRows(num_rows);

  BenchDecodeMemory(data, state, false);
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSizeBytes);
}

void BM_DecodeRowsMemoryStaged(benchmark::State& state) {
  auto row_width_bytes = static_cast<int32_t>(state.range(0));
  auto num_rows = kTotalSizeBytes / row_width_bytes;

  datagen::GeneratedData data = datagen::Gen()
                                    ->FlatFieldsWithNBytesTotalWidth(
                                        row_width_bytes, kMinFieldWidth, kMaxFieldWidth)
                                    ->NRows(num_rows);

  BenchDecodeMemory(data, state, true);
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSizeBytes);
}

void BM_DecodeRowsFile(benchmark::State& state) {
  auto row_width_bytes = static_cast<int32_t>(state.range(0));
  auto num_rows = kTotalSizeBytes / row_width_bytes;

  datagen::GeneratedData data = datagen::Gen()
                                    ->FlatFieldsWithNBytesTotalWidth(
                                        row_width_bytes, kMinFieldWidth, kMaxFieldWidth)
                                    ->NRows(num_rows);

  BenchDecodeFile(data, state, false);
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSizeBytes);
}

void BM_DecodeRowsFileStaged(benchmark::State& state) {
  auto row_width_bytes = static_cast<int32_t>(state.range(0));
  auto num_rows = kTotalSizeBytes / row_width_bytes;

  datagen::GeneratedData data = datagen::Gen()
                                    ->FlatFieldsWithNBytesTotalWidth(
                                        row_width_bytes, kMinFieldWidth, kMaxFieldWidth)
                                    ->NRows(num_rows);

  BenchDecodeFile(data, state, true);
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSizeBytes);
}

void BM_DecodeRowsFileOneWideField(benchmark::State& state) {
  auto row_width_bytes = static_cast<int32_t>(state.range(0));
  auto num_rows = kTotalSizeBytes / row_width_bytes;

  datagen::GeneratedData data =
      datagen::Gen()->Field(datagen::Flat(row_width_bytes))->NRows(num_rows);

  BenchDecodeFile(data, state, false);
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSizeBytes);
}

void BM_DecodeRowsIoUring(benchmark::State& state) {
  auto row_width_bytes = static_cast<int32_t>(state.range(0));
  auto num_rows = kTotalSizeBytes / row_width_bytes;

  datagen::GeneratedData data =
      datagen::Gen()->Field(datagen::Flat(row_width_bytes))->NRows(num_rows);

  BenchDecodeIoUring(data, state);
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSizeBytes);
}

}  // namespace

}  // namespace quiver

// Register the function as a benchmark
BENCHMARK(quiver::BM_EncodeMemory);
BENCHMARK(quiver::BM_DecodeRowsIoUring)
    ->ArgName("RowWidthBytes")
    ->Arg(4)
    ->Arg(256)
    ->Arg(2048)
    ->Arg(4096)
    ->Arg(16384);
BENCHMARK(quiver::BM_DecodeRowsMemory)
    ->ArgName("RowWidthBytes")
    ->Arg(4)
    ->Arg(256)
    ->Arg(2048)
    ->Arg(4096)
    ->Arg(16384);
BENCHMARK(quiver::BM_DecodeRowsMemoryStaged)
    ->ArgName("RowWidthBytes")
    ->Arg(4)
    ->Arg(256)
    ->Arg(2048)
    ->Arg(4096)
    ->Arg(16384);
BENCHMARK(quiver::BM_DecodeRowsMemoryOneWideField)
    ->ArgName("RowWidthBytes")
    ->Arg(4)
    ->Arg(256)
    ->Arg(2048)
    ->Arg(4096)
    ->Arg(16384);
BENCHMARK(quiver::BM_DecodeRowsFile)
    ->ArgName("RowWidthBytes")
    ->Arg(4)
    ->Arg(256)
    ->Arg(2048)
    ->Arg(4096)
    ->Arg(16384);
BENCHMARK(quiver::BM_DecodeRowsFileStaged)
    ->ArgName("RowWidthBytes")
    ->Arg(4)
    ->Arg(256)
    ->Arg(2048)
    ->Arg(4096)
    ->Arg(16384);
BENCHMARK(quiver::BM_DecodeRowsFileOneWideField)
    ->ArgName("RowWidthBytes")
    ->Arg(4)
    ->Arg(256)
    ->Arg(2048)
    ->Arg(4096)
    ->Arg(16384);
QUIVER_BENCHMARK_MAIN();
