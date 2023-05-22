// SPDX-License-Identifier: Apache-2.0

#include "quiver/core/array.h"

#include <cstring>
#include <limits>
#include <sstream>

#include "quiver/util/bit_util.h"
#include "quiver/util/finally.h"
#include "quiver/util/logging_p.h"
#include "quiver/util/variant_p.h"

namespace quiver {

namespace {

int32_t CountNumBuffers(const ArrowArray& array) {
  int32_t num_buffers = 0;
  num_buffers += array.n_buffers;
  if (array.dictionary != nullptr) {
    num_buffers += CountNumBuffers(*array.dictionary);
  }
  for (int32_t child_idx = 0; child_idx < array.n_children; child_idx++) {
    num_buffers += CountNumBuffers(*(array.children[child_idx]));
  }
  return num_buffers;
}

int64_t CountNumFields(const ArrowSchema& schema) {
  int64_t num_fields = 0;
  for (int64_t i = 0; i < schema.n_children; i++) {
    num_fields += CountNumFields(*schema.children[i]) + 1;
  }
  if (schema.dictionary != nullptr) {
    num_fields += CountNumFields(*schema.dictionary) + 1;
  }
  return num_fields;
}

Status CopyArrowSchema(const ArrowSchema& schema, FieldDescriptor* descriptor,
                       int index) {
  descriptor->format = std::string(schema.format);
  descriptor->metadata = std::string(schema.metadata);
  descriptor->name = std::string(schema.name);
  descriptor->num_children = schema.n_children;
  if (schema.dictionary != nullptr) {
    descriptor->num_children++;
  }
  descriptor->nullable = schema.flags & ARROW_FLAG_NULLABLE;
  descriptor->map_keys_sorted = schema.flags & ARROW_FLAG_MAP_KEYS_SORTED;
  descriptor->dict_indices_ordered = schema.flags & ARROW_FLAG_DICTIONARY_ORDERED;
  descriptor->layout = LayoutKind::kFlat;
  descriptor->index = index;
  std::string_view format = descriptor->format;
  if (format == "n") {
    descriptor->data_width_bytes = -1;
  } else if (format == "b") {
    descriptor->data_width_bytes = 0;
  } else if (format == "c") {
    descriptor->data_width_bytes = 1;
  } else if (format == "C") {
    descriptor->data_width_bytes = 1;
  } else if (format == "s") {
    descriptor->data_width_bytes = 2;
  } else if (format == "S") {
    descriptor->data_width_bytes = 2;
  } else if (format == "i") {
    descriptor->data_width_bytes = 4;
  } else if (format == "I") {
    descriptor->data_width_bytes = 4;
  } else if (format == "l") {
    descriptor->data_width_bytes = 8;
  } else if (format == "L") {
    descriptor->data_width_bytes = 8;
  } else if (format == "e") {
    descriptor->data_width_bytes = 2;
  } else if (format == "f") {
    descriptor->data_width_bytes = 4;
  } else if (format == "g") {
    descriptor->data_width_bytes = 8;
  } else if (format == "z") {
    descriptor->layout = LayoutKind::kInt32ContiguousList;
    descriptor->data_width_bytes = 4;
  } else if (format == "Z") {
    descriptor->layout = LayoutKind::kInt64ContiguousList;
    descriptor->data_width_bytes = 8;
  } else if (format == "u") {
    descriptor->layout = LayoutKind::kInt32ContiguousList;
    descriptor->data_width_bytes = 4;
  } else if (format == "U") {
    descriptor->layout = LayoutKind::kInt64ContiguousList;
    descriptor->data_width_bytes = 8;
  } else if (format.starts_with("d:")) {
    std::istringstream reader(descriptor->format);
    int throwaway = 0;
    int bitwidth = 0;
    reader.ignore(2);     // d:
    reader >> throwaway;  // precision
    reader.ignore(1);     // ,
    reader >> throwaway;  // scale
    if (reader.fail()) {
      return Status::Invalid("Failed to parse decimal format string ",
                             descriptor->format);
    }
    reader.ignore(1);
    if (reader.eof()) {
      bitwidth = 128;
    } else {
      reader >> bitwidth;
      if (reader.fail()) {
        return Status::Invalid("Failed to parse decimal format string ",
                               descriptor->format);
      }
      if (bitwidth % 8 != 0) {
        return Status::Invalid("Decimal bit-width was not a multiple of 8");
      }
    }
    descriptor->data_width_bytes = bitwidth / 8;
  } else if (format.starts_with("w:")) {
    std::istringstream reader(descriptor->format);
    int bytewidth = 0;
    reader.ignore(2);
    reader >> bytewidth;
    if (reader.fail()) {
      return Status::Invalid("Failed to parse fixed width format string ",
                             descriptor->format);
    }
    descriptor->data_width_bytes = bytewidth;
  } else if (format == "tdD") {
    descriptor->data_width_bytes = 4;
  } else if (format == "tdm") {
    descriptor->data_width_bytes = 8;
  } else if (format == "tts") {
    descriptor->data_width_bytes = 4;
  } else if (format == "ttm") {
    descriptor->data_width_bytes = 4;
  } else if (format == "ttu") {
    descriptor->data_width_bytes = 8;
  } else if (format == "ttn") {
    descriptor->data_width_bytes = 8;
  } else if (format.starts_with("ts")) {
    descriptor->data_width_bytes = 8;
  } else if (format.starts_with("tD")) {
    descriptor->data_width_bytes = 8;
  } else if (format == "tiM") {
    descriptor->data_width_bytes = 4;
  } else if (format == "tiD") {
    descriptor->data_width_bytes = 8;
  } else if (format == "tin") {
    descriptor->data_width_bytes = 16;
  } else if (format == "+l") {
    descriptor->data_width_bytes = 4;
    descriptor->layout = LayoutKind::kInt32ContiguousList;
  } else if (format == "+L") {
    descriptor->data_width_bytes = 8;
    descriptor->layout = LayoutKind::kInt64ContiguousList;
  } else if (format.starts_with("+w")) {
    descriptor->data_width_bytes = -1;
    descriptor->layout = LayoutKind::kFixedListArray;
  } else if (format == "+s") {
    descriptor->data_width_bytes = -1;
    descriptor->layout = LayoutKind::kStructArray;
  } else if (format == "+m") {
    descriptor->data_width_bytes = 4;
    descriptor->layout = LayoutKind::kInt32ContiguousList;
  } else if (format.starts_with("ud")) {
    descriptor->data_width_bytes = 4;
    descriptor->layout = LayoutKind::kUnion;
  } else if (format.starts_with("us")) {
    descriptor->data_width_bytes = -1;
    descriptor->layout = LayoutKind::kUnion;
  }
  return Status::OK();
}

Status DoImportSchemaField(const ArrowSchema& schema, SimpleSchema* out) {
  int index = static_cast<int>(out->types.size());
  out->types.emplace_back();
  out->types.back().schema = out;
  QUIVER_RETURN_NOT_OK(CopyArrowSchema(schema, &out->types.back(), index));
  if (schema.dictionary != nullptr) {
    QUIVER_RETURN_NOT_OK(DoImportSchemaField(*schema.dictionary, out));
  }
  for (int64_t i = 0; i < schema.n_children; i++) {
    QUIVER_RETURN_NOT_OK(DoImportSchemaField(*schema.children[i], out));
  }
  return Status::OK();
}

}  // namespace

FieldDescriptor& FieldDescriptor::child(int index) const {
  DCHECK_GE(index, 0);
  return schema->types[this->index + index];
}

bool SimpleSchema::Equals(const SimpleSchema& other) const {
  return types == other.types && top_level_indices == other.top_level_indices;
}

Status SimpleSchema::ImportFromArrow(ArrowSchema* schema, SimpleSchema* out) {
  util::Finally release_schema([schema] { schema->release(schema); });
  if (std::strncmp(schema->format, "+s", 2) != 0) {
    return Status::Invalid("Top level schema must be a struct type");
  }
  int64_t total_num_fields = CountNumFields(*schema);
  if (total_num_fields > std::numeric_limits<int32_t>::max()) {
    return Status::Invalid("Can only handle schema with less than 2^32 total fields");
  }

  out->types.clear();
  out->types.reserve(total_num_fields);
  out->top_level_types.resize(schema->n_children);
  out->top_level_indices.resize(schema->n_children);
  for (int64_t i = 0; i < schema->n_children; i++) {
    int top_level_index = static_cast<int32_t>(out->types.size());
    out->top_level_indices[i] = top_level_index;
    QUIVER_RETURN_NOT_OK(DoImportSchemaField(*schema->children[i], out));
    out->top_level_types[i] = out->types[top_level_index];
  }
  DCHECK_EQ(out->types.size(), total_num_fields);
  return Status::OK();
}

template <typename T>
Status GetBufferOrEmptySpan(const ArrowArray& array, int index, int64_t length, int width,
                            LayoutKind layout, std::span<T>* out) {
  if (array.n_buffers <= index) {
    return Status::Invalid("Expected a buffer at index ", index, " for an array with ",
                           LayoutToString(layout),
                           " layout but none (not even nullptr) was present");
  }
  auto buff_or_null = reinterpret_cast<T*>(array.buffers[index]);
  if (buff_or_null == nullptr) {
    *out = {};
  } else if (width == 0) {
    *out = std::span<T>(buff_or_null, bit_util::CeilDiv(length, 8));
  } else {
    *out = std::span<T>(buff_or_null, length * width);
  }
  return Status::OK();
}

class ImportedBatch : public ReadOnlyBatch {
 public:
  const ReadOnlyArray& array(int32_t index) const override {
    DCHECK_GE(index, 0);
    DCHECK_LT(index, arrays_.size());
    return arrays_[index];
  }

  int32_t num_arrays() const override { return arrays_.size(); }
  const SimpleSchema* schema() const override { return schema_; };

  ImportedBatch() = default;
  ImportedBatch(const ImportedBatch&) = delete;
  ImportedBatch& operator=(const ImportedBatch&) = delete;

  ImportedBatch(ImportedBatch&& other)
      : schema_(other.schema_),
        arrays_(std::move(other.arrays_)),
        backing_array_(other.backing_array_) {
    // Important to clear this on move to avoid double free
    other.backing_array_ = nullptr;
  }

  ImportedBatch& operator=(ImportedBatch&& other) {
    schema_ = other.schema_;
    arrays_ = std::move(other.arrays_);
    other.backing_array_ = nullptr;
    return *this;
  }

  ~ImportedBatch() {
    if (backing_array_ && backing_array_->release != nullptr) {
      backing_array_->release(backing_array_);
    }
  }

 private:
  Status DoImportArray(const ArrowArray& array, const FieldDescriptor& field) {
    if (array.offset != 0) {
      return Status::NotImplemented("support for offsets in imported arrays");
    }
    int index = arrays_.size();

    int64_t length = array.length;
    std::span<const uint8_t> validity;

    if (field.layout != LayoutKind::kUnion) {
      QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const uint8_t>(array, /*index=*/0, length,
                                                               /*width=*/0, field.layout,
                                                               &validity));
    }

    switch (field.layout) {
      case LayoutKind::kFlat: {
        std::span<const uint8_t> values;
        QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const uint8_t>(
            array, /*index=*/1, length, field.data_width_bytes, field.layout, &values));
        arrays_.push_back(ReadOnlyFlatArray(validity, values, &field, length));
        break;
      }
      case LayoutKind::kInt32ContiguousList: {
        std::span<const int32_t> offsets;
        QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const int32_t>(
            array, /*index=*/1, length, /*width=*/1, field.layout, &offsets));
        arrays_.push_back(
            ReadOnlyInt32ContiguousListArray(validity, offsets, length + 1));
        if (std::string_view(field.format).starts_with("+")) {
          if (array.n_children == 0) {
            return Status::Invalid("List or map array that had no children");
          }
          QUIVER_RETURN_NOT_OK(DoImportArray(*array.children[0], field.child(0)));
        }
        break;
      }
      case LayoutKind::kInt64ContiguousList: {
        std::span<const int64_t> offsets;
        QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const int64_t>(
            array, /*index=*/1, length, /*width=*/1, field.layout, &offsets));
        arrays_.push_back(
            ReadOnlyInt64ContiguousListArray(validity, offsets, length + 1));
        // There is a child array here but the only variations of int64 list are binary
        // and we handle those specially below
        break;
      }
    }

    // The arrow spec treats string arrays as a special kind of layout but quiver makes
    // them look like list arrays for simplicity
    if (field.format == "z" || field.format == "u") {
      ReadOnlyInt32ContiguousListArray list_arr;
      QUIVER_RETURN_NOT_OK(util::checked_get(&arrays_.back(), &list_arr));
      int32_t data_length = list_arr.offsets[length];
      std::span<const uint8_t> str_data;
      std::span<const uint8_t> str_validity;
      QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const uint8_t>(
          array, /*index=*/2, data_length, /*width=*/1, field.layout, &str_data));
      arrays_.push_back(
          ReadOnlyFlatArray(str_validity, str_data, &field.child(0), length));
    } else if (field.format == "Z" || field.format == "U") {
      ReadOnlyInt64ContiguousListArray list_arr;
      QUIVER_RETURN_NOT_OK(util::checked_get(&arrays_.back(), &list_arr));
      int64_t data_length = list_arr.offsets[length];
      std::span<const uint8_t> str_data;
      std::span<const uint8_t> str_validity;
      QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const uint8_t>(
          array, /*index=*/2, data_length, /*width=*/1, field.layout, &str_data));
      arrays_.push_back(
          ReadOnlyFlatArray(str_validity, str_data, &field.child(0), length));
    }
    return Status::OK();
  }

  const SimpleSchema* schema_;
  std::vector<ReadOnlyArray> arrays_;
  ArrowArray* backing_array_ = nullptr;

  friend Status ImportBatch(ArrowArray* array, SimpleSchema* schema,
                            std::unique_ptr<ReadOnlyBatch>* out);
};

Status ImportBatch(ArrowArray* array, SimpleSchema* schema,
                   std::unique_ptr<ReadOnlyBatch>* out) {
  auto imported_batch = std::make_unique<ImportedBatch>();
  if (imported_batch->backing_array_ != nullptr) {
    return Status::Invalid("Cannot overwrite existing data");
  }
  if (array->n_children != schema->num_fields()) {
    return Status::Invalid("Imported array had ", array->n_children, " but expected ",
                           schema->num_fields(), " according to the schema");
  }
  if (array->n_buffers != 0) {
    return Status::Invalid(
        "Top level array must be a struct array (which should have no buffers)");
  }
  imported_batch->backing_array_ = array;
  imported_batch->schema_ = schema;
  imported_batch->arrays_.reserve(schema->num_types());
  for (int i = 0; i < static_cast<int>(array->n_children); i++) {
    QUIVER_RETURN_NOT_OK(
        imported_batch->DoImportArray(*array->children[i], schema->field(i)));
  }
  *out = std::move(imported_batch);
  return Status::OK();
}

}  // namespace quiver