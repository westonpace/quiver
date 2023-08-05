#include "quiver/benchutil/datagen.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_traits.h>

#include <memory>
#include <random>

#include "arrow/array/util.h"
#include "quiver/util/arrow_util.h"
#include "quiver/util/bit_util.h"

namespace quiver::bench {

void AssertOk(const arrow::Status& status) {
  if (!status.ok()) {
    status.Abort();
  }
}
namespace {

const auto& RandomEngine() {
  static std::independent_bits_engine<std::default_random_engine, 32, uint32_t> engine;
  return engine;
}

std::shared_ptr<arrow::Schema> kSampleFlatSchema = arrow::schema(
    {arrow::field("int32", arrow::int32()), arrow::field("int64", arrow::int64()),
     arrow::field("int16", arrow::int16()), arrow::field("float32", arrow::float32())});

std::shared_ptr<arrow::Array> GenRandomFlatArray(
    int32_t num_rows, const std::shared_ptr<arrow::DataType>& type) {
  int32_t byte_width = type->byte_width();
  int32_t bytes_values = num_rows * byte_width;
  int32_t bytes_validity = bit_util::CeilDiv(num_rows, 8);

  std::unique_ptr<arrow::Buffer> values =
      arrow::AllocateBuffer(bytes_values).ValueOrDie();

  uint8_t* values_data = values->mutable_data();
  uint8_t* values_end = values_data + values->size();
  std::generate(values_data, values_end, RandomEngine());

  std::vector<std::shared_ptr<arrow::Buffer>> buffers = {nullptr, std::move(values)};
  std::shared_ptr<arrow::ArrayData> array_data =
      arrow::ArrayData::Make(type, num_rows, std::move(buffers), {}, 0, 0);
  return arrow::MakeArray(array_data);
}

int32_t CalculateBytesPerRow(const arrow::Schema& schema) {
  int32_t num_bytes = 0;
  for (const auto& field : schema.fields()) {
    num_bytes += field->type()->byte_width();
  }
  return num_bytes;
}

util::OwnedArrowSchema CreateFlatDataSchema() {
  util::OwnedArrowSchema schema = util::AllocateArrowSchema();
  AssertOk(arrow::ExportSchema(*kSampleFlatSchema, schema.get()));
  return schema;
}

}  // namespace

const std::shared_ptr<ArrowSchema>& GetFlatDataSchema() {
  static std::shared_ptr<ArrowSchema> schema = CreateFlatDataSchema();
  return schema;
}

util::OwnedArrowArray GenFlatData(int32_t target_num_bytes) {
  int32_t value_bytes_per_row = CalculateBytesPerRow(*kSampleFlatSchema);
  // One whole validity byte every 8 rows
  int32_t bytes_per_8_rows = (value_bytes_per_row * 8) + kSampleFlatSchema->num_fields();
  int32_t num_octets = bit_util::CeilDiv(target_num_bytes, bytes_per_8_rows);
  int32_t num_rows = num_octets * 8;

  QUIVER_DCHECK_GT(num_rows, 0);

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (const auto& field : kSampleFlatSchema->fields()) {
    arrays.push_back(GenRandomFlatArray(num_rows, field->type()));
  }

  std::shared_ptr<arrow::RecordBatch> batch =
      arrow::RecordBatch::Make(kSampleFlatSchema, num_rows, std::move(arrays));

  util::OwnedArrowArray arrow_array = util::AllocateArrowArray();
  AssertOk(arrow::ExportRecordBatch(*batch, arrow_array.get()));
  return arrow_array;
}

}  // namespace quiver::bench