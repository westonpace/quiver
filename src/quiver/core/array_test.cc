#include "quiver/core/array.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/builder.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include "quiver/testutil/test_util.h"
#include "quiver/util/arrow_util.h"

namespace quiver {

std::shared_ptr<arrow::RecordBatch> BuildTestBatch() {
  arrow::Int32Builder builder;
  AssertOk(builder.Append(7));
  AssertOk(builder.Append(42));
  AssertOk(builder.AppendNull());
  std::shared_ptr<arrow::Array> arr = AssertOrAssign(builder.Finish());
  std::shared_ptr<arrow::Schema> schema =
      arrow::schema({arrow::field("x", arrow::int32())});
  return arrow::RecordBatch::Make(schema, arr->length(), {arr});
}

TEST(ArrowCData, BasicRoundTrip) {
  util::OwnedArrowArray input_array = util::AllocateArrowArray();
  util::OwnedArrowSchema input_schema = util::AllocateArrowSchema();

  AssertOk(
      arrow::ExportRecordBatch(*BuildTestBatch(), input_array.get(), input_schema.get()));

  SimpleSchema quiver_schema;
  AssertOk(SimpleSchema::ImportFromArrow(input_schema.get(), &quiver_schema,
                                         /*consume_schema=*/false));
  ASSERT_NE(input_schema->release, nullptr);

  std::unique_ptr<ReadOnlyBatch> quiver_batch;
  AssertOk(ImportBatch(input_array.get(), &quiver_schema, &quiver_batch));
  ASSERT_EQ(input_array->release, nullptr);

  ASSERT_EQ(13, quiver_batch->num_bytes());

  ReadOnlyFlatArray arr = std::get<ReadOnlyFlatArray>(quiver_batch->array(0));
  ASSERT_EQ(arr.length, 3);
  ASSERT_EQ(arr.values.size(), 12);
  ASSERT_EQ(arr.validity.size(), 1);

  ASSERT_EQ(arr.validity.data()[0], 0x3);

  // The memory still technically "lives" in arrow's memory pool
  ASSERT_GT(arrow::default_memory_pool()->bytes_allocated(), 0);

  AssertOk(std::move(*quiver_batch).ExportToArrow(input_array.get()));

  ASSERT_NE(input_array->release, nullptr);

  std::shared_ptr<arrow::RecordBatch> round_trip =
      AssertOrAssign(arrow::ImportRecordBatch(input_array.get(), input_schema.get()));

  ASSERT_TRUE(round_trip->Equals(*BuildTestBatch()));
}

TEST(ArrowCData, SchemaRoundTrip) {
  util::OwnedArrowSchema c_schema = util::AllocateArrowSchema();

  std::shared_ptr<arrow::Schema> arrow_schema = arrow::schema(
      {arrow::field("x", arrow::int32()), arrow::field("y", arrow::int64())});

  AssertOk(arrow::ExportSchema(*arrow_schema, c_schema.get()));

  SimpleSchema quiver_schema;
  AssertOk(SimpleSchema::ImportFromArrow(c_schema.get(), &quiver_schema));

  ASSERT_EQ(quiver_schema.num_fields(), 2);
  ASSERT_EQ(quiver_schema.field(0).name, "x");
  ASSERT_EQ(quiver_schema.field(1).name, "y");

  AssertOk(quiver_schema.ExportToArrow(c_schema.get()));

  std::shared_ptr<arrow::Schema> roundtrip =
      AssertOrAssign(arrow::ImportSchema(c_schema.get()));

  ASSERT_TRUE(roundtrip->Equals(*arrow_schema));
}

}  // namespace quiver