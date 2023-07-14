#include "quiver/datagen/datagen.h"

#include <cstring>
#include <memory>
#include <random>

#include "quiver/util/bit_util.h"
#include "quiver/util/logging_p.h"
#include "quiver/util/random.h"

namespace quiver::datagen {

class FlatFieldGeneratorImpl : public FieldGenerator {
 public:
  FlatFieldGeneratorImpl(int min_data_width_bytes, int max_data_width_bytes)
      : min_data_width_bytes_(min_data_width_bytes),
        max_data_width_bytes_(max_data_width_bytes) {}

  std::unique_ptr<FieldBuilder> CreateBuilder() override {
    return FieldBuilder::Flat(
        util::RandInt(min_data_width_bytes_, max_data_width_bytes_));
  }

  void PopulateField(Batch* batch, int field_idx, int64_t num_rows) override {
    int data_width_bytes = batch->schema()->field(field_idx).data_width_bytes;
    int64_t num_value_bytes = num_rows * data_width_bytes;
    int64_t num_validity_bytes = bit_util::CeilDiv(num_rows, 8LL);
    batch->ResizeBufferBytes(field_idx, 0, num_validity_bytes);
    batch->ResizeBufferBytes(field_idx, 1, num_value_bytes);
    FlatArray flat_arr = std::get<FlatArray>(batch->mutable_array(field_idx));
    bit_util::SetBitmap(flat_arr.validity.data(), 0, flat_arr.length);
    util::RandomBytes(flat_arr.values);
  }

 private:
  int min_data_width_bytes_;
  int max_data_width_bytes_;
};

std::unique_ptr<FieldGenerator> Flat(int min_data_width_bytes, int max_data_width_bytes) {
  int actual_max_data_width_bytes = max_data_width_bytes;
  if (actual_max_data_width_bytes < 0) {
    actual_max_data_width_bytes = min_data_width_bytes;
  }
  return std::make_unique<FlatFieldGeneratorImpl>(min_data_width_bytes,
                                                  actual_max_data_width_bytes);
}

class DataGeneratorImpl : public DataGenerator {
 public:
  DataGenerator* NFieldsOf(int num_fields,
                           std::unique_ptr<FieldGenerator> field_gen) override {
    std::size_t idx = owned_field_builders_.size();
    owned_field_builders_.push_back(std::move(field_gen));
    for (int field_idx = 0; field_idx < num_fields; field_idx++) {
      field_builders_.push_back(owned_field_builders_[idx].get());
    }
    return this;
  }

  DataGenerator* Field(std::unique_ptr<FieldGenerator> field_gen) override {
    NFieldsOf(1, std::move(field_gen));
    return this;
  }

  DataGenerator* FlatFieldsWithNBytesTotalWidth(int n, int min_data_width_bytes,
                                                int max_data_width_bytes) override {
    int bytes_remaining = n;
    while (bytes_remaining > 0) {
      int next_width = util::RandInt(min_data_width_bytes, max_data_width_bytes);
      next_width = std::min(next_width, bytes_remaining);
      bytes_remaining -= next_width;
      owned_field_builders_.push_back(Flat(next_width));
      field_builders_.push_back(owned_field_builders_.back().get());
    }
    return this;
  }

  GeneratedData NRows(int64_t num_rows) override {
    GeneratedMutableData mutable_data = NMutableRows(num_rows);
    GeneratedData out;
    out.batch = std::move(mutable_data.batch);
    out.schema = std::move(mutable_data.schema);
    return out;
  }

  GeneratedMutableData NMutableRows(int64_t num_rows) override {
    GeneratedMutableData out;
    out.schema = std::make_unique<SimpleSchema>();
    std::unique_ptr<SchemaBuilder> schema_builder = SchemaBuilder::Start();
    for (auto* field_builder : field_builders_) {
      schema_builder->Field(field_builder->CreateBuilder());
    }
    QUIVER_DCHECK_OK(schema_builder->Finish(out.schema.get()));
    std::unique_ptr<Batch> batch = Batch::CreateBasic(out.schema.get());
    batch->SetLength(num_rows);
    for (int32_t field_idx = 0; field_idx < static_cast<int32_t>(field_builders_.size());
         field_idx++) {
      field_builders_[field_idx]->PopulateField(batch.get(), field_idx, num_rows);
    }
    out.batch = std::move(batch);
    return out;
  }

  SimpleSchema Schema() override {
    SimpleSchema schema;
    std::unique_ptr<SchemaBuilder> schema_builder = SchemaBuilder::Start();
    for (auto* field_builder : field_builders_) {
      schema_builder->Field(field_builder->CreateBuilder());
    }
    QUIVER_DCHECK_OK(schema_builder->Finish(&schema));
    return schema;
  }

  std::vector<std::unique_ptr<FieldGenerator>> owned_field_builders_;
  std::vector<FieldGenerator*> field_builders_;
};

std::unique_ptr<DataGenerator> Gen() { return std::make_unique<DataGeneratorImpl>(); }

}  // namespace quiver::datagen
