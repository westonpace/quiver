#include <arrow/array/builder_primitive.h>
#include <arrow/builder.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <iosfwd>

#include "gtest/gtest-param-test.h"
#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/row/row_p.h"
#include "quiver/testutil/test_util.h"
#include "quiver/util/arrow_util.h"

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

  void Init() {
    if (IsFileStorage()) {
      storage_ = TmpFileStorage(/*direct_io=*/false);
    } else {
      storage_ = TestStorage();
    }
  }

  std::unique_ptr<row::RowEncoder> CreateEncoder(const SimpleSchema* schema) const {
    std::unique_ptr<RowEncoder> encoder;
    AssertOk(row::RowEncoder::Create(schema, storage_.get(), false, &encoder));
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
    std::unique_ptr<RowEncoder> encoder = CreateEncoder(&data.schema);
    int64_t row_id = -1;
    AssertOk(encoder->Append(*data.batch, &row_id));
    DCHECK_EQ(row_id, 0);
    AssertOk(encoder->Finish());

    std::unique_ptr<RowDecoder> decoder = CreateDecoder(&data.schema);

    std::unique_ptr<Batch> output =
        Batch::CreateInitializedBasic(&data.schema, kEnoughBytesForScratch);

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
  std::unique_ptr<RowEncoder> encoder = CreateEncoder(&data.schema);
  AssertOk(row::RowEncoder::Create(&data.schema, storage_.get(), false, &encoder));
  int64_t row_id = -1;
  AssertOk(encoder->Append(*data.batch, &row_id));
  DCHECK_EQ(0, row_id);
  AssertOk(encoder->Finish());

  std::unique_ptr<RowDecoder> decoder = CreateDecoder(&data.schema);

  std::unique_ptr<Batch> output =
      Batch::CreateInitializedBasic(&data.schema, 1024LL * 1024LL);

  std::vector<int64_t> row_indices = {2, 0};
  AssertOk(decoder->Load({row_indices.data(), row_indices.size()}, output.get()));

  SchemaAndBatch expected =
      TestBatch({Int8Array({{}, 1}), Int64Array({1000, {}}), Float32Array({{}, {}})});

  ASSERT_TRUE(output->BinaryEquals(*expected.batch));
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