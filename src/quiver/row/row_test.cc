#include <arrow/array/builder_primitive.h>
#include <arrow/builder.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include "quiver/core/array.h"
#include "quiver/row/row_p.h"
#include "quiver/testutil/test_util.h"
#include "quiver/util/arrow_util.h"

namespace quiver::row {

// These tests encode and decode from a big scratch space.  We don't
// want to bother with allocation, etc. and so we just allocate a bit
// space to use
constexpr int64_t kEnoughBytesForScratch = 1024LL * 1024LL;

class RowEncodingTest : public ::testing::Test {
 public:
  RowEncodingTest()
      : scratch_buffer_(kEnoughBytesForScratch),
        scratch_(scratch_buffer_.data(), scratch_buffer_.size()),
        sink_(StreamSink::FromFixedSizeSpan(scratch_)),
        source_(RandomAccessSource::WrapSpan(scratch_)){};

  // Encode the entire batch, then select the entire batch, in order, and test for
  // equality to input
  void CheckFullRoundTrip(const SchemaAndBatch& data) {
    std::unique_ptr<RowQueueAppendingProducer> encoder;
    assert_ok(row::RowQueueAppendingProducer::Create(&data.schema, &sink_, &encoder));
    assert_ok(encoder->Append(*data.batch));

    std::unique_ptr<RowQueueRandomAccessConsumer> decoder;
    assert_ok(
        row::RowQueueRandomAccessConsumer::Create(&data.schema, &source_, &decoder));

    std::unique_ptr<Batch> output =
        Batch::CreateInitializedBasic(&data.schema, kEnoughBytesForScratch);

    std::vector<int32_t> row_indices(data.batch->length());
    std::iota(row_indices.begin(), row_indices.end(), 0);
    assert_ok(decoder->Load({row_indices.data(), row_indices.size()}, output.get()));

    ASSERT_TRUE(output->BinaryEquals(*data.batch));
  }

  std::vector<uint8_t> scratch_buffer_;
  std::span<uint8_t> scratch_;
  StreamSink sink_;
  RandomAccessSource source_;
};

TEST_F(RowEncodingTest, BasicRoundTrip) {
  SchemaAndBatch data = TestBatch(
      {Int8Array({1, 2, {}}), Int64Array({{}, 100, 1000}), Float32Array({{}, {}, {}})});

  CheckFullRoundTrip(data);

  std::unique_ptr<RowQueueAppendingProducer> encoder;
  assert_ok(row::RowQueueAppendingProducer::Create(&data.schema, &sink_, &encoder));
  assert_ok(encoder->Append(*data.batch));

  std::unique_ptr<RowQueueRandomAccessConsumer> decoder;
  assert_ok(row::RowQueueRandomAccessConsumer::Create(&data.schema, &source_, &decoder));

  std::unique_ptr<Batch> output =
      Batch::CreateInitializedBasic(&data.schema, 1024LL * 1024LL);

  std::vector<int32_t> row_indices = {2, 0};
  assert_ok(decoder->Load({row_indices.data(), row_indices.size()}, output.get()));

  SchemaAndBatch expected =
      TestBatch({Int8Array({{}, 1}), Int64Array({1000, {}}), Float32Array({{}, {}})});

  ASSERT_TRUE(output->BinaryEquals(*expected.batch));
}

// Test the case where we need more than one byte to store validity bits
TEST_F(RowEncodingTest, ManyColumns) {
  constexpr int kNumColumns = 50;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(kNumColumns);
  for (int i = 0; i < kNumColumns; i++) {
    arrays.push_back(Int8Array({{}, i, {}, 2}));
  }
  SchemaAndBatch data = TestBatch(std::move(arrays));
  CheckFullRoundTrip(data);
}

TEST_F(RowEncodingTest, LargeInput) {}

}  // namespace quiver::row