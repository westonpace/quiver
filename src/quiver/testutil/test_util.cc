#include "quiver/testutil/test_util.h"

#include <arrow/array/builder_primitive.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>

#include <source_location>

#include "quiver/testutil/tmpfiles.h"
#include "quiver/util/arrow_util.h"
#include "quiver/util/local_allocator_p.h"
#include "quiver/util/logging_p.h"

namespace quiver {
void ThrowNotOk(const arrow::Status& status) {
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
}

void AssertOk(const arrow::Status& status, std::source_location loc) {
  if (!status.ok()) {
    ADD_FAILURE_AT(loc.file_name(), loc.line())
        << "expected an ok status but received " << status.ToString();
  }
}

void ThrowNotOk(const Status& status) {
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
}

void AssertOk(const Status& status, std::source_location loc) {
  if (!status.ok()) {
    ADD_FAILURE_AT(loc.file_name(), loc.line())
        << "expected an ok status but received " << status.ToString();
  }
}

template <typename BuilderType, typename CType>
std::shared_ptr<arrow::Array> TestArray(const std::vector<std::optional<CType>>& values) {
  BuilderType builder{};
  ThrowNotOk(builder.Resize(values.size()));
  for (const auto val : values) {
    if (val.has_value()) {
      ThrowNotOk(builder.Append(*val));
    } else {
      ThrowNotOk(builder.AppendNull());
    }
  }
  arrow::Result<std::shared_ptr<arrow::Array>> arr = builder.Finish();
  return ThrowOrAssign(arr);
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

template <typename BuilderType, typename CType>
util::local_ptr<FlatArray> LocalArray(util::LocalAllocator* alloc,
                                      const std::vector<std::optional<CType>>& values) {
  std::shared_ptr<arrow::Array> arr = TestArray<BuilderType, CType>(values);
  int data_width_bytes = arr->type()->byte_width();
  bool has_validity = arr->data()->buffers[0] != nullptr;
  util::local_ptr<FlatArray> flat_arr =
      alloc->AllocateFlatArray(data_width_bytes, arr->length(), has_validity);
  if (has_validity) {
    std::memcpy(flat_arr->validity.data(), arr->data()->buffers[0]->data(),
                arr->data()->buffers[0]->size());
  }
  std::memcpy(flat_arr->values.data(), arr->data()->buffers[1]->data(),
              arr->data()->buffers[1]->size());
  return flat_arr;
}

util::local_ptr<FlatArray> LocalBoolArray(
    util::LocalAllocator* alloc, const std::vector<std::optional<bool>>& values) {
  return LocalArray<arrow::BooleanBuilder, bool>(alloc, values);
}
util::local_ptr<FlatArray> LocalInt8Array(
    util::LocalAllocator* alloc, const std::vector<std::optional<int8_t>>& values) {
  return LocalArray<arrow::Int8Builder, int8_t>(alloc, values);
}
util::local_ptr<FlatArray> LocalInt16Array(
    util::LocalAllocator* alloc, const std::vector<std::optional<int16_t>>& values) {
  return LocalArray<arrow::Int16Builder, int16_t>(alloc, values);
}
util::local_ptr<FlatArray> LocalInt32Array(
    util::LocalAllocator* alloc, const std::vector<std::optional<int32_t>>& values) {
  return LocalArray<arrow::Int32Builder, int32_t>(alloc, values);
}
util::local_ptr<FlatArray> LocalInt64Array(
    util::LocalAllocator* alloc, const std::vector<std::optional<int64_t>>& values) {
  return LocalArray<arrow::Int64Builder, int64_t>(alloc, values);
}
util::local_ptr<FlatArray> Float32Array(util::LocalAllocator* alloc,
                                        const std::vector<std::optional<float>>& values) {
  return LocalArray<arrow::FloatBuilder, float>(alloc, values);
}
util::local_ptr<FlatArray> Float64Array(
    util::LocalAllocator* alloc, const std::vector<std::optional<double>>& values) {
  return LocalArray<arrow::DoubleBuilder, double>(alloc, values);
}

SchemaAndBatch TestBatch(std::vector<std::shared_ptr<arrow::Array>> arrays) {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  if (arrays.empty()) {
    record_batch = ThrowOrAssign(arrow::RecordBatch::MakeEmpty(arrow::schema({})));
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

  ThrowNotOk(
      arrow::ExportRecordBatch(*record_batch, c_data_arr.get(), c_data_schema.get()));

  SchemaAndBatch schema_and_batch;
  ThrowNotOk(
      SimpleSchema::ImportFromArrow(c_data_schema.get(), &schema_and_batch.schema));
  ThrowNotOk(
      ImportBatch(c_data_arr.get(), &schema_and_batch.schema, &schema_and_batch.batch));

  return schema_and_batch;
}

std::unique_ptr<Storage> TestStorage(int64_t size_bytes) {
  std::unique_ptr<Storage> storage;
  util::Uri specifier{"ram", "", {{"size_bytes", std::to_string(size_bytes)}}};
  AssertOk(Storage::FromSpecifier(specifier, &storage));
  return storage;
}

std::unique_ptr<Storage> TmpFileStorage(bool direct_io) {
  std::string path = testutil::TemporaryFiles().NewTemporaryFile();
  std::unique_ptr<Storage> storage;
  util::Uri specifier{"file", path, {{"direct", direct_io ? "true" : "false"}}};
  AssertOk(Storage::FromSpecifier(specifier, &storage));
  return storage;
}

}  // namespace quiver