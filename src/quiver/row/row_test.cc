#include <arrow/array/builder_primitive.h>
#include <arrow/builder.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>
#include <omp.h>
#include <spdlog/spdlog.h>

#include <iosfwd>
#include <thread>

#include "gtest/gtest-param-test.h"
#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/datagen/datagen.h"
#include "quiver/row/row_p.h"
#include "quiver/testutil/test_util.h"
#include "quiver/util/arrow_util.h"
#include "quiver/util/config.h"
#include "quiver/util/random.h"
#include "quiver/util/tracing.h"

namespace quiver::row {

// These tests encode and decode from a big scratch space.  We don't
// want to bother with allocation, etc. and so we just allocate a bit
// space to use
constexpr int64_t kEnoughBytesForScratch = 1024LL * 1024LL;

enum class DecoderType { kMemory, kMemoryStaged, kFile, kFileStaged, kIoUring };
struct RowCodecParams {
  DecoderType decoder_type;

  static std::vector<RowCodecParams> Values() {
    return {{DecoderType::kMemory},
            {DecoderType::kMemoryStaged},
            {DecoderType::kFile},
            {DecoderType::kFileStaged},
            {DecoderType::kIoUring}};
  }

  friend void PrintTo(const RowCodecParams& point, std::ostream* out) {
    switch (point.decoder_type) {
      case DecoderType::kMemory:
        *out << "mem";
        break;
      case DecoderType::kMemoryStaged:
        *out << "mem_staged";
        break;
      case DecoderType::kFile:
        *out << "file";
        break;
      case DecoderType::kFileStaged:
        *out << "file_staged";
        break;
      case DecoderType::kIoUring:
        *out << "io_uring";
        break;
    }
  }
};

class RowEncodingTest : public ::testing::TestWithParam<RowCodecParams> {
 public:
  DecoderType GetDecoderType() { return GetParam().decoder_type; }

  bool IsFileStorage() {
    return GetDecoderType() == DecoderType::kFile ||
           GetDecoderType() == DecoderType::kFileStaged ||
           GetDecoderType() == DecoderType::kIoUring;
  }

  bool IsDecoderStaged() {
    return GetDecoderType() == DecoderType::kMemoryStaged ||
           GetDecoderType() == DecoderType::kFileStaged;
  }

  void Init(int64_t num_bytes = -1) {
    if (IsFileStorage()) {
      storage_ = TmpFileStorage(/*direct_io=*/false);
    } else {
      if (num_bytes > 0) {
        storage_ = TestStorage(num_bytes);
      } else {
        storage_ = TestStorage();
      }
    }
  }

  std::unique_ptr<row::RowEncoder> CreateEncoder(const SimpleSchema* schema) const {
    std::unique_ptr<RowEncoder> encoder;
    AssertOk(row::RowEncoder::Create(schema, storage_.get(), &encoder));
    return encoder;
  }

  std::unique_ptr<row::RowDecoder> CreateDecoder(const SimpleSchema* schema) {
    std::unique_ptr<RowDecoder> decoder;
    if (GetDecoderType() == DecoderType::kIoUring) {
      AssertOk(row::RowDecoder::CreateIoUring(schema, storage_.get(), &decoder));
    } else if (IsDecoderStaged()) {
      AssertOk(row::RowDecoder::CreateStaged(schema, storage_.get(), &decoder));
    } else {
      AssertOk(row::RowDecoder::Create(schema, storage_.get(), &decoder));
    }
    return decoder;
  }

  // Encode the entire batch, then select the entire batch, in order, and test for
  // equality to input
  void CheckFullRoundTrip(const SchemaAndBatch& data) {
    Init();
    std::unique_ptr<RowEncoder> encoder = CreateEncoder(data.schema.get());
    int64_t row_id = -1;
    AssertOk(encoder->Append(*data.batch, &row_id));
    DCHECK_EQ(row_id, 0);
    AssertOk(encoder->Finish());

    std::unique_ptr<RowDecoder> decoder = CreateDecoder(data.schema.get());

    std::unique_ptr<Batch> output =
        Batch::CreateInitializedBasic(data.schema.get(), kEnoughBytesForScratch);

    std::vector<int64_t> row_indices(data.batch->length());
    std::iota(row_indices.begin(), row_indices.end(), 0);
    AssertOk(decoder->Load({row_indices.data(), row_indices.size()}, output.get()));

    ASSERT_TRUE(output->BinaryEquals(*data.batch));
  }

  std::unique_ptr<Storage> storage_;
};

TEST_P(RowEncodingTest, BasicRoundTrip) {
  SchemaAndBatch data = TestBatch(
      {Int8Array({1, 2, {}}), Int64Array({{}, 100, 1000}), Float32Array({{}, {}, {}})});

  CheckFullRoundTrip(data);

  Init();

  // Same round trip but with a partial read
  std::unique_ptr<RowEncoder> encoder = CreateEncoder(data.schema.get());
  AssertOk(row::RowEncoder::Create(data.schema.get(), storage_.get(), &encoder));
  int64_t row_id = -1;
  AssertOk(encoder->Append(*data.batch, &row_id));
  DCHECK_EQ(0, row_id);
  AssertOk(encoder->Finish());

  std::unique_ptr<RowDecoder> decoder = CreateDecoder(data.schema.get());

  std::unique_ptr<Batch> output =
      Batch::CreateInitializedBasic(data.schema.get(), 1024LL * 1024LL);

  std::vector<int64_t> row_indices = {2, 0};
  AssertOk(decoder->Load({row_indices.data(), row_indices.size()}, output.get()));

  SchemaAndBatch expected =
      TestBatch({Int8Array({{}, 1}), Int64Array({1000, {}}), Float32Array({{}, {}})});

  ASSERT_TRUE(output->BinaryEquals(*expected.batch));
}

TEST_P(RowEncodingTest, MultithreadedRoundTrip) {
  constexpr int32_t kRowWidthBytes = 64;
  constexpr int64_t kNumRows = 1_MiLL;
  constexpr int32_t kNumThreads = 16;
  constexpr auto kBatchSize = static_cast<int32_t>(kNumRows / kNumThreads);

  util::config::SetLogLevel(util::config::LogLevel::kInfo);

  static_assert(kNumRows % kNumThreads == 0,
                "Don't make the math harder than it has to be");
  static_assert(
      (kNumRows / kNumThreads) % 8 == 0,
      "Slices must be in groups of 8 so validities are sliced on byte boundaries");

  Init(2 * kNumRows * kRowWidthBytes);

  util::Seed(42);

  datagen::GeneratedData data = datagen::Gen()
                                    ->FlatFieldsWithNBytesTotalWidth(kRowWidthBytes, 1, 8)
                                    ->NRows(kNumRows);

  std::unique_ptr<RowEncoder> encoder = CreateEncoder(data.schema.get());
  AssertOk(row::RowEncoder::Create(data.schema.get(), storage_.get(), &encoder));

  std::vector<int64_t> row_ids;
  std::mutex mutex;

  omp_set_num_threads(kNumThreads);
#pragma omp parallel
  {
    util::Tracer::SetCurrent(util::Tracer::Singleton());
    util::TracerScope task_scope;
    if (!util::Tracer::GetCurrent()->InOperation()) {
      task_scope = util::Tracer::GetCurrent()->StartOperation(util::tracecat::kUnitTest);
    }
    int32_t start = kBatchSize * omp_get_thread_num();
    int32_t length = kBatchSize;
    std::unique_ptr<ReadOnlyBatch> batch;
    AssertOk(data.batch->SliceView(start, length, &batch));

    int64_t row_id = -1;
    AssertOk(encoder->Append(*batch, &row_id));
    std::lock_guard lock(mutex);
    row_ids.push_back(row_id);
  }
  AssertOk(encoder->Finish());

  std::sort(row_ids.begin(), row_ids.end());
  ASSERT_EQ(row_ids.size(), kNumThreads);
  for (int i = 0; i < kNumThreads; i++) {
    ASSERT_EQ(row_ids[i], i * kBatchSize);
  }
}

// Test the case where we need more than one byte to store validity bits
TEST_P(RowEncodingTest, ManyColumns) {
  constexpr int kNumColumns = 50;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(kNumColumns);
  for (int i = 0; i < kNumColumns; i++) {
    arrays.push_back(Int8Array({{}, i, {}, 2}));
  }
  SchemaAndBatch data = TestBatch(std::move(arrays));
  CheckFullRoundTrip(data);
}

INSTANTIATE_TEST_SUITE_P(RowEncodingTests, RowEncodingTest,
                         testing::ValuesIn(RowCodecParams::Values()),
                         testing::PrintToStringParamName());

}  // namespace quiver::row