// SPDX-License-Identifier: Apache-2.0

#include "quiver/core/array.h"

#include <array>
#include <cstdio>
#include <cstring>
#include <iosfwd>
#include <limits>
#include <sstream>
#include <string_view>

#include "quiver/util/bit_util.h"
#include "quiver/util/finally.h"
#include "quiver/util/logging_p.h"
#include "quiver/util/variant_p.h"

namespace quiver {

namespace layout {

namespace {
constexpr std::array kLayoutKindNames = {
    "flat", "list", "large list", "struct", "fixed size list", "union"};

}  // namespace

std::string_view to_string(LayoutKind layout) {
  return kLayoutKindNames[static_cast<std::size_t>(layout)];
}

int num_buffers(LayoutKind layout) {
  // This may need to be more complicated someday but right now this is true for all
  // layouts
  return 2;
}

bool is_variable_length(LayoutKind kind) {
  switch (kind) {
    case LayoutKind::kFlat:
    case LayoutKind::kStructArray:
    case LayoutKind::kFixedListArray:
      return false;
    case LayoutKind::kInt32ContiguousList:
    case LayoutKind::kInt64ContiguousList:
    // Union might be fixed length but without knowing all the types
    // we can't tell
    case LayoutKind::kUnion:
      return true;
  }
  QUIVER_CHECK(false) << "Should be unreachable";
  return false;
}
}  // namespace layout

namespace {

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

constexpr int kDefaultDecimalBitwidth = 128;

Status CopyArrowSchema(const ArrowSchema& schema, FieldDescriptor* descriptor,
                       int index) {
  descriptor->format = std::string(schema.format);
  descriptor->metadata = schema.metadata == nullptr ? "" : std::string(schema.metadata);
  descriptor->name = schema.name == nullptr ? "" : std::string(schema.name);
  descriptor->num_children = static_cast<int32_t>(schema.n_children);
  if (schema.dictionary != nullptr) {
    descriptor->num_children++;
  }
  descriptor->nullable = ((schema.flags & ARROW_FLAG_NULLABLE) != 0);
  descriptor->map_keys_sorted = ((schema.flags & ARROW_FLAG_MAP_KEYS_SORTED) != 0);
  descriptor->dict_indices_ordered =
      ((schema.flags & ARROW_FLAG_DICTIONARY_ORDERED) != 0);
  descriptor->layout = LayoutKind::kFlat;
  descriptor->index = index;
  std::string_view format = descriptor->format;
  // NOLINTBEGIN(bugprone-branch-clone)
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
      bitwidth = kDefaultDecimalBitwidth;
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
    descriptor->data_width_bytes = 16;  // NOLINT(readability-magic-numbers)
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
  // NOLINTEND(bugprone-branch-clone)
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

namespace buffer {

void PrintBitmap(std::span<const uint8_t> bitmap, int length, int indentation_level,
                 int max_chars, std::ostream& out) {
  for (int i = 0; i < indentation_level; i++) {
    out << " ";
  }

  int chars_remaining = max_chars - indentation_level;
  bool truncated = false;
  if (chars_remaining < length) {
    length = chars_remaining;
    truncated = true;
  }

  uint8_t bitmask = 1;
  auto itr = bitmap.begin();

  for (int i = 0; i < length; i++) {
    if (*itr & bitmask) {
      out << "#";
    } else {
      out << "-";
    }
    bitmask <<= 1;
    if (bitmask == 0) {
      bitmask = 1;
      itr++;
    }
  }
  if (truncated) {
    out << "...";
  }
}

constexpr std::string_view kHexDigits = "0123456789ABCDEF";

void PrintBuffer(std::span<const uint8_t> buffer, int bytes_per_element,
                 int indentation_level, int max_chars, std::ostream& out) {
  int chars_written = indentation_level;
  for (int i = 0; i < indentation_level; i++) {
    out << " ";
  }
  auto itr = buffer.begin();
  auto end = buffer.end();
  bool ended_early = false;
  while (itr != end) {
    if (chars_written + bytes_per_element >= max_chars) {
      ended_early = true;
      break;
    }
    for (int i = 0; i < bytes_per_element && itr != end; i++) {
      out << kHexDigits[(*itr >> 4)];
      out << kHexDigits[(*itr & 0x0F)];
      itr++;
    }
    if (itr != end) {
      out << " ";
    }
    chars_written += bytes_per_element + 1;
  }
  if (ended_early) {
    out << "...";
  }
}

bool BinaryEquals(std::span<const uint8_t> lhs, std::span<const uint8_t> rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  return std::memcmp(lhs.data(), rhs.data(), lhs.size()) == 0;
}

bool BinaryEqualsWithSelection(std::span<const uint8_t> lhs, std::span<const uint8_t> rhs,
                               int32_t data_width_bytes,
                               std::span<const uint8_t> selection_bitmap) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  DCHECK_EQ(lhs.size() % data_width_bytes, 0);
  DCHECK_EQ(rhs.size() % data_width_bytes, 0);
  DCHECK_EQ(bit_util::CeilDiv(lhs.size() / data_width_bytes, 8), selection_bitmap.size());
  // TODO: Surely there is a much better SIMD way to do this
  uint8_t bitmask = 1;
  const uint8_t* left_itr = lhs.data();
  const uint8_t* left_end = left_itr + lhs.size();
  const uint8_t* right_itr = rhs.data();
  auto selection_itr = selection_bitmap.begin();
  while (left_itr != left_end) {
    bool selected = *selection_itr & bitmask;
    bitmask <<= 1;
    if (bitmask == 0) {
      bitmask = 1;
      selection_itr++;
    }
    if (selected) {
      if (std::memcmp(left_itr, right_itr, data_width_bytes) != 0) {
        return false;
      }
    }
    left_itr += data_width_bytes;
    right_itr += data_width_bytes;
  }
  return true;
}

}  // namespace buffer

namespace array {

Array EmptyArray(LayoutKind layout) {
  switch (layout) {
    case LayoutKind::kFlat:
      return FlatArray{{}, {}, 0};
    default:
      DCHECK(false) << "Not yet implemented";
      return {};
  }
}

ReadOnlyArray ArrayView(Array array) {
  switch (ArrayLayout(array)) {
    case LayoutKind::kFlat: {
      FlatArray flat_array = std::get<FlatArray>(array);
      return ReadOnlyFlatArray{flat_array.validity, flat_array.values,
                               flat_array.width_bytes, flat_array.length};
    }
    default:
      DCHECK(false) << "Not yet implemented";
      return {};
  }
}

LayoutKind ArrayLayout(Array array) { return kArrayVariantIdxToLayout[array.index()]; }

LayoutKind ArrayLayout(ReadOnlyArray array) {
  return kArrayVariantIdxToLayout[array.index()];
}

void PrintArray(ReadOnlyArray array, const FieldDescriptor& type, int indentation_level,
                int max_chars, std::ostream& out) {
  int chars_remaining = max_chars;
  auto print_indent = [&] {
    for (int i = 0; i < indentation_level; i++) {
      out << " ";
    }
    chars_remaining = max_chars - indentation_level;
  };
  switch (ArrayLayout(array)) {
    case LayoutKind::kFlat: {
      ReadOnlyFlatArray flat_array = std::get<ReadOnlyFlatArray>(array);
      print_indent();
      out << "validity: ";
      ::quiver::buffer::PrintBitmap(flat_array.validity, flat_array.length,
                                    /*indentation_level=*/0, chars_remaining - 10, out);
      out << std::endl;
      print_indent();
      out << "values: ";
      ::quiver::buffer::PrintBuffer(flat_array.values, type.data_width_bytes,
                                    /*indentation_level=*/0, chars_remaining - 7, out);
      break;
    }
    default:
      DCHECK(false) << "Not yet implemented";
  }
}

std::string ToString(ReadOnlyArray array, const FieldDescriptor& type) {
  std::stringstream sstr;
  PrintArray(array, type, /*indentation_level=*/0, /*max_chars=*/80, sstr);
  return sstr.str();
}

bool BinaryEquals(ReadOnlyArray lhs, ReadOnlyArray rhs) {
  if (ArrayLayout(lhs) != ArrayLayout(rhs)) {
    return false;
  }
  switch (ArrayLayout(lhs)) {
    case LayoutKind::kFlat: {
      ReadOnlyFlatArray lhs_flat = std::get<ReadOnlyFlatArray>(lhs);
      ReadOnlyFlatArray rhs_flat = std::get<ReadOnlyFlatArray>(rhs);
      if (lhs_flat.width_bytes != rhs_flat.width_bytes) {
        return false;
      }
      if (!buffer::BinaryEquals(lhs_flat.validity, rhs_flat.validity)) {
        return false;
      }
      return buffer::BinaryEqualsWithSelection(lhs_flat.values, rhs_flat.values,
                                               lhs_flat.width_bytes, lhs_flat.validity);
    }
    default:
      DCHECK(false) << "Not yet impelemented";
      return false;
  }
}

}  // namespace array

FieldDescriptor& FieldDescriptor::child(int index) const {
  DCHECK_GE(index, 0);
  int64_t type_index = this->index;
  return schema->types[type_index + index];
}

bool SimpleSchema::Equals(const SimpleSchema& other) const {
  return types == other.types && top_level_indices == other.top_level_indices;
}

Status SimpleSchema::ImportFromArrow(ArrowSchema* schema, SimpleSchema* out,
                                     bool consume_schema) {
  util::Finally release_schema([schema, consume_schema] {
    if (consume_schema && schema != nullptr && schema->release != nullptr) {
      schema->release(schema);
    }
  });
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
  DCHECK_EQ(static_cast<int32_t>(out->types.size()), total_num_fields);
  return Status::OK();
}

Status SimpleSchema::ExportToArrow(ArrowSchema* out) {
  return Status::NotImplemented("TODO");
}

template <typename T>
Status GetBufferOrEmptySpan(const ArrowArray& array, int index, int64_t length, int width,
                            LayoutKind layout, std::span<T>* out) {
  if (array.n_buffers <= index) {
    return Status::Invalid("Expected a buffer at index ", index, " for an array with ",
                           layout::to_string(layout),
                           " layout but none (not even nullptr) was present");
  }
  auto buff_or_null = reinterpret_cast<T*>(array.buffers[index]);
  if (buff_or_null == nullptr) {
    *out = {};
  } else if (width == 0) {
    *out = std::span<T>(buff_or_null, bit_util::CeilDiv(length, static_cast<int64_t>(8)));
  } else {
    *out = std::span<T>(buff_or_null, length * width);
  }
  return Status::OK();
}

class ImportedBatch : public ReadOnlyBatch {
 public:
  [[nodiscard]] ReadOnlyArray array(int32_t index) const override {
    DCHECK_GE(index, 0);
    DCHECK_LT(index, static_cast<int32_t>(arrays_.size()));
    return arrays_[index];
  }

  [[nodiscard]] const SimpleSchema* schema() const override { return schema_; }
  [[nodiscard]] int64_t length() const override { return length_; }
  [[nodiscard]] int64_t num_bytes() const override { return num_bytes_; }
  [[nodiscard]] int32_t num_buffers() const override { return num_buffers_; }

  ImportedBatch() = default;
  ImportedBatch(const ImportedBatch&) = delete;
  ImportedBatch& operator=(const ImportedBatch&) = delete;

  ImportedBatch(ImportedBatch&& other) noexcept
      : schema_(other.schema_),
        arrays_(std::move(other.arrays_)),
        backing_array_(other.backing_array_) {
    // Important to clear this on move to avoid double free
    other.backing_array_.release = nullptr;
  }

  ImportedBatch& operator=(ImportedBatch&& other) noexcept {
    schema_ = other.schema_;
    arrays_ = std::move(other.arrays_);
    other.backing_array_.release = nullptr;
    return *this;
  }

  Status ExportToArrow(ArrowArray* out) && override {
    if (backing_array_.release == nullptr) {
      return Status::Invalid(
          "The underlying arrow array has already been released from this instance");
    }
    // Shallow assignment of fields should be sufficient
    *out = backing_array_;
    backing_array_.release = nullptr;
    return Status::OK();
  }

  ~ImportedBatch() override {
    if (backing_array_.release != nullptr) {
      backing_array_.release(&backing_array_);
    }
  }

 private:
  Status DoImportArray(const ArrowArray& array, const FieldDescriptor& field) {
    if (array.offset != 0) {
      return Status::NotImplemented("support for offsets in imported arrays");
    }
    int64_t length = array.length;
    std::span<const uint8_t> validity;

    if (field.layout != LayoutKind::kUnion) {
      QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const uint8_t>(array, /*index=*/0, length,
                                                               /*width=*/0, field.layout,
                                                               &validity));
      num_bytes_ += validity.size();
      num_buffers_++;
    }

    switch (field.layout) {
      case LayoutKind::kFlat: {
        std::span<const uint8_t> values;
        QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const uint8_t>(
            array, /*index=*/1, length, field.data_width_bytes, field.layout, &values));
        arrays_.emplace_back(
            ReadOnlyFlatArray{validity, values, field.data_width_bytes, length});
        num_bytes_ += values.size();
        num_buffers_++;
        break;
      }
      case LayoutKind::kInt32ContiguousList: {
        std::span<const int32_t> offsets;
        QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const int32_t>(
            array, /*index=*/1, length, /*width=*/1, field.layout, &offsets));
        arrays_.emplace_back(
            ReadOnlyInt32ContiguousListArray{validity, offsets, length + 1});
        if (std::string_view(field.format).starts_with("+")) {
          if (array.n_children == 0) {
            return Status::Invalid("List or map array that had no children");
          }
          QUIVER_RETURN_NOT_OK(DoImportArray(*array.children[0], field.child(0)));
        }
        num_bytes_ += offsets.size_bytes();
        num_buffers_++;
        break;
      }
      case LayoutKind::kInt64ContiguousList: {
        std::span<const int64_t> offsets;
        QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const int64_t>(
            array, /*index=*/1, length, /*width=*/1, field.layout, &offsets));
        arrays_.push_back(
            ReadOnlyInt64ContiguousListArray{validity, offsets, length + 1});
        // There is a child array here but the only variations of int64 list are binary
        // and we handle those specially below
        num_bytes_ += offsets.size_bytes();
        num_buffers_++;
        break;
      }
      default:
        return Status::NotImplemented("No support yet for importing array type: ",
                                      layout::to_string(field.layout));
    }

    // The arrow spec treats string arrays as a special kind of layout but quiver makes
    // them look like list arrays for simplicity
    if (field.format == "z" || field.format == "u") {
      ReadOnlyInt32ContiguousListArray list_arr;
      QUIVER_RETURN_NOT_OK(util::get_or_raise(&arrays_.back(), &list_arr));
      int32_t data_length = list_arr.offsets[length];
      std::span<const uint8_t> str_data;
      // str_validity is an empty span.  Since a string array in Arrow is not a child
      // array it cannot possibly have a validity bitmap
      std::span<const uint8_t> str_validity;
      QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const uint8_t>(
          array, /*index=*/2, data_length, /*width=*/1, field.layout, &str_data));
      num_bytes_ += str_data.size_bytes();
      num_buffers_ += 2;
      arrays_.emplace_back(ReadOnlyFlatArray(str_validity, str_data, length));
    } else if (field.format == "Z" || field.format == "U") {
      ReadOnlyInt64ContiguousListArray list_arr;
      QUIVER_RETURN_NOT_OK(util::get_or_raise(&arrays_.back(), &list_arr));
      int64_t data_length = list_arr.offsets[length];
      std::span<const uint8_t> str_data;
      std::span<const uint8_t> str_validity;
      QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const uint8_t>(
          array, /*index=*/2, data_length, /*width=*/1, field.layout, &str_data));
      num_bytes_ += str_data.size_bytes();
      num_buffers_ += 2;
      arrays_.emplace_back(ReadOnlyFlatArray(str_validity, str_data, length));
    }
    return Status::OK();
  }

  const SimpleSchema* schema_ = nullptr;
  std::vector<ReadOnlyArray> arrays_;
  ArrowArray backing_array_ = ArrowArray::Default();
  int64_t length_ = 0;
  int64_t num_bytes_ = 0;
  int32_t num_buffers_ = 0;

  friend Status ImportBatch(ArrowArray* array, const SimpleSchema* schema,
                            std::unique_ptr<ReadOnlyBatch>* out);
};

Status ImportBatch(ArrowArray* array, const SimpleSchema* schema,
                   std::unique_ptr<ReadOnlyBatch>* out) {
  if (array->release == nullptr) {
    return Status::Invalid("Cannot import already released array");
  }
  auto imported_batch = std::make_unique<ImportedBatch>();
  imported_batch->backing_array_ = *array;
  array->release = nullptr;

  if (array->n_children != schema->num_fields()) {
    return Status::Invalid("Imported array had ", array->n_children, " but expected ",
                           schema->num_fields(), " according to the schema");
  }
  if (array->n_buffers != 1) {
    return Status::Invalid(
        "Top level array must be a struct array (which should have no buffers)");
  }
  if (array->buffers[0] != nullptr) {
    return Status::NotImplemented("Nulls in the top-level struct array");
  }
  imported_batch->schema_ = schema;
  imported_batch->length_ = array->length;
  imported_batch->arrays_.reserve(schema->num_types());
  for (int i = 0; i < static_cast<int>(array->n_children); i++) {
    QUIVER_RETURN_NOT_OK(
        imported_batch->DoImportArray(*array->children[i], schema->field(i)));
  }
  *out = std::move(imported_batch);
  return Status::OK();
}

class BasicBatch : public Batch {
 public:
  explicit BasicBatch(const SimpleSchema* schema) : schema_(schema) {
    int num_buffers_needed = 0;
    for (const auto& type : schema->types) {
      num_buffers_needed += layout::num_buffers(type.layout);
    }
    array_idx_to_buffers_.resize(schema->num_types());
    buffers_.resize(num_buffers_needed);

    int buffer_idx = 0;
    for (int i = 0; i < schema->num_types(); i++) {
      array_idx_to_buffers_[i] = buffer_idx;
      buffer_idx += layout::num_buffers(schema->types[i].layout);
    }
  }

  ReadOnlyArray array(int32_t index) const override {
    std::size_t buffer_offset = array_idx_to_buffers_[index];
    switch (schema_->types[index].layout) {
      case LayoutKind::kFlat: {
        const FieldDescriptor& type = schema_->types[index];
        std::span<const uint8_t> validity =
            BufferToSpan(buffer_offset, bit_util::CeilDiv(length_, 8LL));
        std::span<const uint8_t> values =
            BufferToSpan(buffer_offset + 1, length_ * type.data_width_bytes);
        return ReadOnlyFlatArray{validity, values, type.data_width_bytes, length_};
      }
      default:
        DCHECK(false) << "Not yet implemented";
        return {};
    }
  }
  Array mutable_array(int32_t index) {
    std::size_t buffer_offset = array_idx_to_buffers_[index];
    switch (schema_->types[index].layout) {
      case LayoutKind::kFlat: {
        const FieldDescriptor& type = schema_->types[index];
        std::span<uint8_t> validity =
            BufferToSpan(buffer_offset, bit_util::CeilDiv(length_, 8LL));
        std::span<uint8_t> values =
            BufferToSpan(buffer_offset + 1, length_ * type.data_width_bytes);
        return FlatArray{validity, values, type.data_width_bytes, length_};
      }
      default:
        DCHECK(false) << "Not yet implemented";
        return {};
    }
  }

  const SimpleSchema* schema() const override { return schema_; }
  int64_t length() const override { return length_; }
  int64_t num_bytes() const override { return num_bytes_; }
  int32_t num_buffers() const override { return static_cast<int32_t>(buffers_.size()); }

  Status ExportToArrow(ArrowArray* out) && override {
    return Status::NotImplemented("TODO");
  }

  void SetLength(int64_t new_length) override { length_ = new_length; }

  void ResizeBufferBytes(int32_t array_index, int32_t buffer_index,
                         int64_t num_bytes) override {
    std::size_t buffer_offset = array_idx_to_buffers_[array_index];
    buffers_[buffer_offset + buffer_index].resize(num_bytes);
  }

  int64_t buffer_capacity(int32_t array_index, int32_t buffer_index) override {
    std::size_t buffer_offset = array_idx_to_buffers_[array_index];
    return static_cast<int64_t>(buffers_[buffer_offset + buffer_index].size());
  }

 private:
  std::span<const uint8_t> BufferToSpan(std::size_t index, uint64_t num_bytes) const {
    const std::vector<uint8_t>& buffer = buffers_[index];
    DCHECK_LE(num_bytes, buffer.size());
    return {buffer.begin(), num_bytes};
  }
  std::span<uint8_t> BufferToSpan(std::size_t index, uint64_t num_bytes) {
    std::vector<uint8_t>& buffer = buffers_[index];
    DCHECK_LE(num_bytes, buffer.size());
    return {buffer.begin(), num_bytes};
  }

  const SimpleSchema* schema_;
  double grow_factor_ = 2.0;
  int64_t length_ = 0;
  int64_t num_bytes_ = 0;
  std::vector<std::size_t> array_idx_to_buffers_;
  std::vector<std::vector<uint8_t>> buffers_;
};

std::string ReadOnlyBatch::ToString() const {
  std::stringstream sstr;
  for (int array_idx = 0; array_idx < schema()->num_fields(); array_idx++) {
    sstr << array_idx << ":" << std::endl;
    array::PrintArray(array(array_idx), schema()->top_level_types[array_idx],
                      /*indentation_level=*/2,
                      /*max_chars=*/80, sstr);
    if (array_idx < schema()->num_fields() - 1) {
      sstr << std::endl;
    }
  }
  return sstr.str();
}

bool ReadOnlyBatch::BinaryEquals(const ReadOnlyBatch& other) const {
  for (int array_idx = 0; array_idx < schema()->num_fields(); array_idx++) {
    ReadOnlyArray this_arr = array(array_idx);
    ReadOnlyArray other_arr = other.array(array_idx);
    if (!array::BinaryEquals(this_arr, other_arr)) {
      return false;
    }
  }
  return true;
}

void Batch::ResizeFixedParts(int32_t array_index, int64_t new_length) {
  const FieldDescriptor& type = schema()->top_level_types[array_index];
  switch (type.layout) {
    case LayoutKind::kFlat: {
      int64_t num_validity_bytes = bit_util::CeilDiv(new_length, 8LL);
      int64_t num_value_bytes = new_length * type.data_width_bytes;
      ResizeBufferBytes(array_index, 0, num_validity_bytes);
      ResizeBufferBytes(array_index, 1, num_value_bytes);
      break;
    }
    default:
      DCHECK(false) << "Not yet implemented";
  }
}

std::unique_ptr<Batch> Batch::CreateBasic(const SimpleSchema* schema) {
  return std::make_unique<BasicBatch>(schema);
}
std::unique_ptr<Batch> Batch::CreateInitializedBasic(const SimpleSchema* schema,
                                                     int64_t num_bytes) {
  auto batch = std::make_unique<BasicBatch>(schema);
  int64_t bytes_per_buffer =
      bit_util::FloorDiv(num_bytes, static_cast<int64_t>(batch->num_buffers()));

  for (int i = 0; i < schema->num_types(); i++) {
    for (int j = 0; j < layout::num_buffers(schema->types[i].layout); j++) {
      batch->ResizeBufferBytes(i, j, bytes_per_buffer);
    }
  }
  return batch;
}

}  // namespace quiver