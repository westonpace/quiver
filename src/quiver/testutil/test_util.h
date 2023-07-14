#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <source_location>

#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/testutil/tmpfiles.h"
#include "quiver/util/literals.h"
#include "quiver/util/local_allocator_p.h"
#include "quiver/util/status.h"

using namespace quiver::util::literals;

namespace quiver {

template <typename T>
T ThrowOrAssign(arrow::Result<T> res) {
  if (!res.ok()) {
    throw std::runtime_error(res.status().ToString());
  }
  return res.MoveValueUnsafe();
}

void ThrowNotOk(const arrow::Status& status);

void AssertOk(const arrow::Status& status,
              std::source_location loc = std::source_location::current());

void ThrowNotOk(const Status& status);

void AssertOk(const Status& status,
              std::source_location loc = std::source_location::current());

template <typename T>
T AssertOrAssign(arrow::Result<T> res,
                 std::source_location loc = std::source_location::current()) {
  AssertOk(res.status(), loc);
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

util::local_ptr<FlatArray> LocalBoolArray(util::LocalAllocator* alloc,
                                          const std::vector<std::optional<bool>>& values);
util::local_ptr<FlatArray> LocalInt8Array(
    util::LocalAllocator* alloc, const std::vector<std::optional<int8_t>>& values);
util::local_ptr<FlatArray> LocalInt16Array(
    util::LocalAllocator* alloc, const std::vector<std::optional<int16_t>>& values);
util::local_ptr<FlatArray> LocalInt32Array(
    util::LocalAllocator* alloc, const std::vector<std::optional<int32_t>>& values);
util::local_ptr<FlatArray> LocalInt64Array(
    util::LocalAllocator* alloc, const std::vector<std::optional<int64_t>>& values);
util::local_ptr<FlatArray> Float32Array(util::LocalAllocator* alloc,
                                        const std::vector<std::optional<float>>& values);
util::local_ptr<FlatArray> Float64Array(util::LocalAllocator* alloc,
                                        const std::vector<std::optional<double>>& values);

template <typename T>
util::local_ptr<std::span<T>> LocalSpan(util::LocalAllocator* alloc,
                                        const std::vector<T>& values) {
  util::local_ptr<std::span<T>> span = alloc->AllocateSpan<T>(values.size());
  std::copy(values.begin(), values.end(), span->begin());
  return span;
}

struct SchemaAndBatch {
  SimpleSchema schema;
  std::unique_ptr<ReadOnlyBatch> batch;
};

SchemaAndBatch TestBatch(std::vector<std::shared_ptr<arrow::Array>> arrays);

std::unique_ptr<Storage> TestStorage(int64_t size_bytes = 64_MiLL);
std::unique_ptr<Storage> TmpFileStorage(bool direct_io = false);

}  // namespace quiver