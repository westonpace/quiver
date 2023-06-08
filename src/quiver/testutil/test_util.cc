#include "quiver/testutil/test_util.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/c/bridge.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include "quiver/util/logging_p.h"
#include "quiver/util/arrow_util.h"

namespace quiver {
void throw_not_ok(const arrow::Status& st) {
  if (!st.ok()) {
    throw std::runtime_error(st.ToString());
  }
}

void assert_ok(const arrow::Status& st, std::source_location loc) {
  if (!st.ok()) {
    ADD_FAILURE_AT(loc.file_name(), loc.line())
        << "expected an ok status but received " << st.ToString();
  }
}

void throw_not_ok(const Status& st) {
  if (!st.ok()) {
    throw std::runtime_error(st.ToString());
  }
}

void assert_ok(const Status& st, std::source_location loc) {
  if (!st.ok()) {
    ADD_FAILURE_AT(loc.file_name(), loc.line())
        << "expected an ok status but received " << st.ToString();
  }
}

template <typename BuilderType, typename CType>
std::shared_ptr<arrow::Array> TestArray(const std::vector<std::optional<CType>>& values) {
  BuilderType builder{};
  throw_not_ok(builder.Resize(values.size()));
  for (const auto val : values) {
    if (val.has_value()) {
      throw_not_ok(builder.Append(*val));
    } else {
      throw_not_ok(builder.AppendNull());
    }
  }
  arrow::Result<std::shared_ptr<arrow::Array>> arr = builder.Finish();
  return throw_or_assign(arr);
}

std::shared_ptr<arrow::Array> BoolArray(const std::vector<std::optional<bool>>& values) {
  return TestArray<arrow::BooleanBuilder, bool>(values);
}

std::shared_ptr<arrow::Array> Int8Array(
    const std::vector<std::optional<int8_t>>& values) {
  return TestArray<arrow::Int8Builder, int8_t>(values);
}
std::shared_ptr<arrow::Array> Int16Array(
    const std::vector<std::optional<int16_t>>& values) {
  return TestArray<arrow::Int16Builder, int16_t>(values);
}
std::shared_ptr<arrow::Array> Int32Array(
    const std::vector<std::optional<int32_t>>& values) {
  return TestArray<arrow::Int32Builder, int32_t>(values);
}
std::shared_ptr<arrow::Array> Int64Array(
    const std::vector<std::optional<int64_t>>& values) {
  return TestArray<arrow::Int64Builder, int64_t>(values);
}
std::shared_ptr<arrow::Array> Float32Array(
    const std::vector<std::optional<float>>& values) {
  return TestArray<arrow::FloatBuilder, float>(values);
}
std::shared_ptr<arrow::Array> Float64Array(
    const std::vector<std::optional<double>>& values) {
  return TestArray<arrow::DoubleBuilder, double>(values);
}

SchemaAndBatch TestBatch(
    std::vector<std::shared_ptr<arrow::Array>> arrays) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  if (arrays.size() == 0) {
    record_batch = throw_or_assign(arrow::RecordBatch::MakeEmpty(arrow::schema({})));
  } else {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    int64_t length = -1;
    for (std::size_t i = 0; i < arrays.size(); i++) {
      fields.push_back(arrow::field("f" + std::to_string(i), arrays[i]->type()));
      if (length < 0) {
        length = arrays[i]->length();
      } else {
        DCHECK_EQ(length, arrays[i]->length());
      }
    }
    std::shared_ptr<arrow::Schema> schema = arrow::schema(std::move(fields));
    record_batch = arrow::RecordBatch::Make(std::move(schema), length, std::move(arrays));
  }
  util::OwnedArrowArray c_data_arr = util::AllocateArrowArray();
  util::OwnedArrowSchema c_data_schema = util::AllocateArrowSchema();

  throw_not_ok(arrow::ExportRecordBatch(*record_batch, c_data_arr.get(), c_data_schema.get()));

  SchemaAndBatch schema_and_batch;
  throw_not_ok(SimpleSchema::ImportFromArrow(c_data_schema.get(), &schema_and_batch.schema));
  throw_not_ok(ImportBatch(c_data_arr.get(), &schema_and_batch.schema, &schema_and_batch.batch));

  return schema_and_batch;
}

}  // namespace quiver