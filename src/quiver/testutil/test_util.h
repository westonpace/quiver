#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <source_location>

#include "quiver/core/array.h"
#include "quiver/util/status.h"

namespace quiver {

template <typename T>
T throw_or_assign(arrow::Result<T> res) {
  if (!res.ok()) {
    throw std::runtime_error(res.status().ToString());
  }
  return res.MoveValueUnsafe();
}

void throw_not_ok(const arrow::Status& status);

void assert_ok(const arrow::Status& status,
               std::source_location loc = std::source_location::current());

void throw_not_ok(const Status& status);

void assert_ok(const Status& status,
               std::source_location loc = std::source_location::current());

template <typename T>
T assert_or_assign(arrow::Result<T> res,
                   std::source_location loc = std::source_location::current()) {
  assert_ok(res.status(), loc);
  return res.MoveValueUnsafe();
}

std::shared_ptr<arrow::Array> BoolArray(const std::vector<std::optional<bool>>& values);
std::shared_ptr<arrow::Array> Int8Array(const std::vector<std::optional<int8_t>>& values);
std::shared_ptr<arrow::Array> Int16Array(
    const std::vector<std::optional<int16_t>>& values);
std::shared_ptr<arrow::Array> Int32Array(
    const std::vector<std::optional<int32_t>>& values);
std::shared_ptr<arrow::Array> Int64Array(
    const std::vector<std::optional<int64_t>>& values);
std::shared_ptr<arrow::Array> Float32Array(
    const std::vector<std::optional<float>>& values);
std::shared_ptr<arrow::Array> Float64Array(
    const std::vector<std::optional<double>>& values);

struct SchemaAndBatch {
  SimpleSchema schema;
  std::unique_ptr<ReadOnlyBatch> batch;
};

SchemaAndBatch TestBatch(std::vector<std::shared_ptr<arrow::Array>> arrays);

}  // namespace quiver