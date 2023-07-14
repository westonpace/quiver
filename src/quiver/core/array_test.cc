#include "quiver/core/array.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/builder.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include "quiver/datagen/datagen.h"
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

  ASSERT_EQ(13, quiver_batch->NumBytes());

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

TEST(ReadOnlyBatch, SelectView) {
  datagen::GeneratedData data = datagen::Gen()
                                    ->Field(datagen::Flat(3, 3))
                                    ->Field(datagen::Flat(2, 2))
                                    ->Field(datagen::Flat(8, 8))
                                    ->NRows(1024);

  SimpleSchema sub_schema = data.schema->Select({0, 2});
  ASSERT_EQ(sub_schema.num_types(), 2);
  std::unique_ptr<ReadOnlyBatch> view = data.batch->SelectView({0, 2}, &sub_schema);

  ASSERT_EQ(view->schema()->num_types(), 2);
}

TEST(SimpleSchema, AllColumnsFrom) {
  datagen::GeneratedData left =
      datagen::Gen()->FlatFieldsWithNBytesTotalWidth(64, 1, 8)->NRows(16);

  datagen::GeneratedData right =
      datagen::Gen()->FlatFieldsWithNBytesTotalWidth(64, 1, 8)->NRows(16);

  SimpleSchema combined = SimpleSchema::AllColumnsFrom(*left.schema, *right.schema);
  ASSERT_EQ(combined.num_fields(),
            left.schema->num_fields() + right.schema->num_fields());
  ASSERT_EQ(combined.num_types(), left.schema->num_types() + right.schema->num_types());
  for (int i = 0; i < left.schema->num_fields(); i++) {
    ASSERT_EQ(combined.field(i), left.schema->field(i));
  }
  for (int i = 0; i < right.schema->num_fields(); i++) {
    ASSERT_EQ(combined.field(i + left.schema->num_fields()), right.schema->field(i));
  }
}

TEST(BasicBatch, Combine) {
  datagen::GeneratedMutableData left = datagen::Gen()
                                           ->Field(datagen::Flat(1, 1))
                                           ->Field(datagen::Flat(2, 2))
                                           ->NMutableRows(16);
  datagen::GeneratedMutableData right = datagen::Gen()
                                            ->Field(datagen::Flat(4, 4))
                                            ->Field(datagen::Flat(8, 8))
                                            ->NMutableRows(16);

  SimpleSchema combined = SimpleSchema::AllColumnsFrom(*left.schema, *right.schema);

  ReadOnlyFlatArray right_sample_arr = std::get<ReadOnlyFlatArray>(right.batch->array(0));
  std::vector<uint8_t> right_sample_arr_data(right_sample_arr.values.size());
  std::memcpy(right_sample_arr_data.data(), right_sample_arr.values.data(),
              right_sample_arr.values.size());

  AssertOk(left.batch->Combine(std::move(*right.batch), &combined));

  ReadOnlyFlatArray combined_sample_arr =
      std::get<ReadOnlyFlatArray>(left.batch->array(2));
  ASSERT_TRUE(std::equal(right_sample_arr_data.begin(), right_sample_arr_data.end(),
                         combined_sample_arr.values.begin()));
}

}  // namespace quiver