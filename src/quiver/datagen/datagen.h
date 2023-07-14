#pragma once

#include "quiver/core/array.h"
#include "quiver/core/builder.h"
#include "quiver/pch.h"

namespace quiver::datagen {

struct GeneratedData {
  std::unique_ptr<SimpleSchema> schema;
  std::unique_ptr<ReadOnlyBatch> batch;
};

struct GeneratedMutableData {
  std::unique_ptr<SimpleSchema> schema;
  std::unique_ptr<Batch> batch;
};

class FieldGenerator {
 public:
  virtual ~FieldGenerator() = default;
  virtual std::unique_ptr<FieldBuilder> CreateBuilder() = 0;
  virtual void PopulateField(Batch* batch, int field_idx, int64_t num_rows) = 0;
};

/// Generates a flat field with a random width between the given min and max (inclusive)
std::unique_ptr<FieldGenerator> Flat(int min_data_width_bytes,
                                     int max_data_width_bytes = -1);

class DataGenerator {
 public:
  virtual ~DataGenerator() = default;
  virtual DataGenerator* Field(std::unique_ptr<FieldGenerator> field_gen) = 0;
  virtual DataGenerator* NFieldsOf(int n, std::unique_ptr<FieldGenerator> field_gen) = 0;
  virtual DataGenerator* FlatFieldsWithNBytesTotalWidth(
      int n, int min_data_width_bytes, int max_data_width_bytes = -1) = 0;
  virtual GeneratedData NRows(int64_t num_rows) = 0;
  virtual GeneratedMutableData NMutableRows(int64_t num_rows) = 0;
  virtual SimpleSchema Schema() = 0;
};
std::unique_ptr<DataGenerator> Gen();

}  // namespace quiver::datagen
