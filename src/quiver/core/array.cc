// SPDX-License-Identifier: Apache-2.0

#include "quiver/core/array.h"

#include <array>
#include <cstdio>
#include <cstring>
#include <iosfwd>
#include <iostream>
#include <iterator>
#include <limits>
#include <sstream>
#include <string_view>

#include "quiver/core/arrow.h"
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

int num_buffers(LayoutKind /*layout*/) {
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

namespace {
void PrintBitmap(std::span<const uint8_t> bitmap, int32_t length,
                 int32_t indentation_level, int32_t max_chars, std::ostream& out,
                 char true_char, char false_char) {
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
    if ((*itr & bitmask) != 0) {
      out << true_char;
    } else {
      out << false_char;
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
}  // namespace

void PrintBitmap(std::span<const uint8_t> bitmap, int32_t length,
                 int32_t indentation_level, int32_t max_chars, std::ostream& out) {
  PrintBitmap(bitmap, length, indentation_level, max_chars, out, '#', '-');
}

void PrintBoolmap(std::span<const uint8_t> bitmap, int32_t length,
                  int32_t indentation_level, int32_t max_chars, std::ostream& out) {
  PrintBitmap(bitmap, length, indentation_level, max_chars, out, 'T', 'F');
}

void PrintImplicitBitmap(int32_t length, int32_t indentation_level, int32_t max_chars,
                         std::ostream& out) {
  for (int i = 0; i < indentation_level; i++) {
    out << " ";
  }

  int chars_remaining = max_chars - indentation_level;
  bool truncated = false;
  if (chars_remaining < length) {
    length = chars_remaining;
    truncated = true;
  }

  for (int i = 0; i < length; i++) {
    out << "#";
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
                               int32_t element_size_bytes,
                               std::span<const uint8_t> selection_bitmap) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  DCHECK_EQ(lhs.size() % element_size_bytes, 0);
  DCHECK_EQ(rhs.size() % element_size_bytes, 0);
  DCHECK_EQ(bit_util::CeilDiv(static_cast<int64_t>(lhs.size()) /
                                  static_cast<int64_t>(element_size_bytes),
                              8LL),
            static_cast<int64_t>(selection_bitmap.size()));
  // TODO: Surely there is a much better SIMD way to do this
  uint8_t bitmask = 1;
  const uint8_t* left_itr = lhs.data();
  const uint8_t* left_end = left_itr + lhs.size();
  const uint8_t* right_itr = rhs.data();
  auto selection_itr = selection_bitmap.begin();
  while (left_itr != left_end) {
    bool selected = (*selection_itr & bitmask) != 0;
    bitmask <<= 1;
    if (bitmask == 0) {
      bitmask = 1;
      selection_itr++;
    }
    if (selected) {
      if (std::memcmp(left_itr, right_itr, element_size_bytes) != 0) {
        return false;
      }
    }
    left_itr += element_size_bytes;
    right_itr += element_size_bytes;
  }
  return true;
}

bool IsAllSet(std::span<const uint8_t> span) {
  return std::find(span.begin(), span.end(), 0) == span.end();
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
      if (flat_array.validity.empty()) {
        ::quiver::buffer::PrintImplicitBitmap(static_cast<int32_t>(flat_array.length),
                                              /*indentation_level=*/0,
                                              chars_remaining - 10, out);
      } else {
        ::quiver::buffer::PrintBitmap(flat_array.validity,
                                      static_cast<int32_t>(flat_array.length),
                                      /*indentation_level=*/0, chars_remaining - 10, out);
      }
      out << std::endl;
      print_indent();
      out << "values: ";
      if (type.data_width_bytes == 0) {
        ::quiver::buffer::PrintBoolmap(flat_array.values,
                                       static_cast<int32_t>(flat_array.length),
                                       /*indentation_level=*/0, chars_remaining - 7, out);
      } else {
        ::quiver::buffer::PrintBuffer(flat_array.values, type.data_width_bytes,
                                      /*indentation_level=*/0, chars_remaining - 7, out);
      }
      break;
    }
    default:
      DCHECK(false) << "Not yet implemented";
  }
}

std::string ToString(ReadOnlyArray array, const FieldDescriptor& type) {
  std::stringstream sstr;
  PrintArray(array, type, /*indentation_level=*/0,
             /*max_chars=*/80, sstr);
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
      if (lhs_flat.validity.empty()) {
        if (!rhs_flat.validity.empty()) {
          // There is no left validity so the right validity better be all 1s
          if (!buffer::IsAllSet(rhs_flat.validity)) {
            return false;
          }
        }
      } else if (rhs_flat.validity.empty()) {
        // There is no right validity so the left validity better be all 1s
        if (!buffer::IsAllSet(lhs_flat.validity)) {
          return false;
        }
      } else {
        // Either both validities are empty or both validities are set and
        // so we can just compare them
        if (!buffer::BinaryEquals(lhs_flat.validity, rhs_flat.validity)) {
          return false;
        }
      }

      if (lhs_flat.validity.empty()) {
        // Implicit validity, so we can just compare the values buffers
        return buffer::BinaryEquals(lhs_flat.values, rhs_flat.values);
      }
      // Only compare the non-null parts of the values buffers
      return buffer::BinaryEqualsWithSelection(lhs_flat.values, rhs_flat.values,
                                               lhs_flat.width_bytes, lhs_flat.validity);
    }
    default:
      DCHECK(false) << "Not yet impelemented";
      return false;
  }
}

bool HasNulls(ReadOnlyArray arr) {
  switch (ArrayLayout(arr)) {
    case LayoutKind::kFlat: {
      ReadOnlyFlatArray flat_array = std::get<ReadOnlyFlatArray>(arr);
      if (flat_array.validity.empty()) {
        return false;
      }
      return std::find(flat_array.validity.begin(), flat_array.validity.end(), 0) !=
             flat_array.validity.end();
    }
    default:
      DCHECK(false) << "Not yet implemented";
      return false;
  }
}

int64_t NumBytes(ReadOnlyArray arr) {
  switch (ArrayLayout(arr)) {
    case LayoutKind::kFlat: {
      ReadOnlyFlatArray flat_array = std::get<ReadOnlyFlatArray>(arr);
      return static_cast<int64_t>(flat_array.values.size() + flat_array.validity.size());
    }
    default:
      DCHECK(false) << "Not yet implemented";
      return 0;
  }
}

ReadOnlyArray Slice(ReadOnlyArray array, int64_t offset, int64_t length) {
  DCHECK_EQ(offset % 8, 0) << "offset must be a multiple of 8";
  switch (ArrayLayout(array)) {
    case LayoutKind::kFlat: {
      ReadOnlyFlatArray flat_array = std::get<ReadOnlyFlatArray>(array);
      int64_t validity_byte_offset = offset / 8;
      int64_t validity_num_bytes = bit_util::CeilDiv(length, 8LL);
      std::span<const uint8_t> validity =
          flat_array.validity.subspan(validity_byte_offset, validity_num_bytes);
      int64_t values_byte_offset = offset * flat_array.width_bytes;
      int64_t values_length = length * flat_array.width_bytes;
      std::span<const uint8_t> values =
          flat_array.values.subspan(values_byte_offset, values_length);
      return ReadOnlyFlatArray{validity, values, flat_array.width_bytes, length};
    }
    default:
      DCHECK(false) << "Not yet implemented";
      return {};
  }
}

}  // namespace array

FieldDescriptor& FieldDescriptor::child(int index) const {
  DCHECK_GE(index, 0);
  int64_t type_index = this->index;
  return schema->types[type_index + index];
}

std::string FieldDescriptor::ToString() const {
  switch (layout) {
    case LayoutKind::kFlat:
      return "flat<" + std::to_string(data_width_bytes) + ">";
    default:
      DCHECK(false) << "Not yet implemented";
      return "";
  }
}

SimpleSchema SimpleSchema::Select(const std::vector<int32_t>& field_indices) const {
  std::vector<FieldDescriptor> new_types;
  std::vector<FieldDescriptor> new_top_level_types;
  new_top_level_types.reserve(field_indices.size());
  std::vector<int32_t> new_top_level_indices;
  new_top_level_indices.reserve(field_indices.size());
  for (const auto& field_index : field_indices) {
    int32_t start = top_level_indices[field_index];
    int32_t end;
    if (static_cast<int32_t>(top_level_types.size()) == field_index + 1) {
      end = static_cast<int32_t>(top_level_types.size());
    } else {
      end = top_level_indices[field_index + 1];
    }
    new_top_level_indices.push_back(static_cast<int32_t>(new_types.size()));
    for (int32_t type_idx = start; type_idx < end; type_idx++) {
      new_types.push_back(types[type_idx]);
    }
    new_top_level_types.push_back(new_types[new_top_level_indices.back()]);
  }
  return {std::move(new_types), std::move(new_top_level_types),
          std::move(new_top_level_indices)};
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

std::string SimpleSchema::ToString() const {
  std::stringstream out;
  out << "schema{";
  for (int i = 0; i < num_fields(); i++) {
    out << top_level_types[i].ToString();
    if (i < num_fields() - 1) {
      out << ", ";
    }
  }
  out << "}";
  return out.str();
}

struct ExportedSchemaData {
  std::string format;
  std::string name;
  std::string metadata;
  std::vector<ArrowSchema> children;
  std::vector<ArrowSchema*> child_pointers;
};

void ReleaseSchema(ArrowSchema* schema) {
  if (schema->release != nullptr) {
    schema->release(schema);
  }
}

void ReleaseExportedSchema(ArrowSchema* schema) {
  if (schema->release == nullptr) {
    return;
  }
  for (int64_t i = 0; i < schema->n_children; ++i) {
    struct ArrowSchema* child = schema->children[i];
    ReleaseSchema(child);
    DCHECK_EQ(child->release, nullptr)
        << "Child release callback should have marked it released";
  }
  struct ArrowSchema* dict = schema->dictionary;
  if (dict != nullptr) {
    ReleaseSchema(dict);
    DCHECK_EQ(dict->release, nullptr)
        << "Dictionary release callback should have marked it released";
  }
  DCHECK_NE(schema->private_data, nullptr);
  delete reinterpret_cast<ExportedSchemaData*>(schema->private_data);

  schema->release = nullptr;
}

Status ExportFlatField(const FieldDescriptor& /*field*/, ExportedSchemaData* /*data*/,
                       ArrowSchema* /*out*/) {
  // We don't need to do anything extra since there are no children
  return Status::OK();
}

Status ExportSchemaField(const FieldDescriptor& field, ArrowSchema* out) {
  auto* data = new ExportedSchemaData();
  out->private_data = reinterpret_cast<void*>(data);
  out->release = ReleaseExportedSchema;

  data->name = field.name;
  data->metadata = field.metadata;
  data->format = field.format;

  out->metadata = data->metadata.c_str();
  out->format = data->format.c_str();
  out->name = data->name.c_str();
  out->dictionary = nullptr;
  out->children = nullptr;
  out->n_children = 0;
  out->flags = 0;
  if (field.nullable) {
    out->flags |= ARROW_FLAG_NULLABLE;
  }
  if (field.map_keys_sorted) {
    out->flags |= ARROW_FLAG_MAP_KEYS_SORTED;
  }
  if (field.dict_indices_ordered) {
    out->flags |= ARROW_FLAG_DICTIONARY_ORDERED;
  }
  switch (field.layout) {
    case LayoutKind::kFlat:
      return ExportFlatField(field, data, out);
    default:
      return Status::NotImplemented("Exporting schema field with layout ",
                                    layout::to_string(field.layout));
  }
}

Status SimpleSchema::ExportToArrow(ArrowSchema* out) const {
  auto* data = new ExportedSchemaData();
  out->private_data = reinterpret_cast<void*>(data);
  out->release = ReleaseExportedSchema;

  data->children.resize(num_fields());

  for (int64_t i = 0; i < num_fields(); ++i) {
    QUIVER_RETURN_NOT_OK(ExportSchemaField(top_level_types[i], &data->children[i]));
    data->child_pointers.push_back(&data->children[i]);
  }

  data->format = "+s";
  data->metadata = "";
  data->name = "";
  out->dictionary = nullptr;
  out->metadata = data->metadata.c_str();
  out->format = data->format.c_str();
  out->name = data->name.c_str();
  out->n_children = static_cast<int32_t>(top_level_types.size());
  out->children = data->child_pointers.data();
  out->flags = 0;
  return Status::OK();
}

SimpleSchema SimpleSchema::AllColumnsFrom(const SimpleSchema& left,
                                          const SimpleSchema& right) {
  std::vector<FieldDescriptor> all_types;
  all_types.reserve(left.types.size() + right.types.size());
  all_types.insert(all_types.end(), left.types.begin(), left.types.end());
  all_types.insert(all_types.end(), right.types.begin(), right.types.end());

  std::vector<FieldDescriptor> all_top_level_types;
  all_top_level_types.reserve(left.top_level_types.size() + right.top_level_types.size());
  all_top_level_types.insert(all_top_level_types.end(), left.top_level_types.begin(),
                             left.top_level_types.end());
  all_top_level_types.insert(all_top_level_types.end(), right.top_level_types.begin(),
                             right.top_level_types.end());

  std::vector<int32_t> all_top_level_indices;
  all_top_level_indices.reserve(left.top_level_indices.size() +
                                right.top_level_indices.size());
  all_top_level_indices.insert(all_top_level_indices.end(),
                               left.top_level_indices.begin(),
                               left.top_level_indices.end());
  for (int32_t top_level_index : right.top_level_indices) {
    all_top_level_indices.push_back(left.num_types() + top_level_index);
  }

  return {std::move(all_types), std::move(all_top_level_types),
          std::move(all_top_level_indices)};
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

class ViewBatch : public ReadOnlyBatch {
 public:
  ViewBatch(const ReadOnlyBatch* target, std::vector<int32_t> column_indices,
            const SimpleSchema* new_schema_)
      : target_(target),
        column_indices_(std::move(column_indices)),
        new_schema_(new_schema_) {}

  [[nodiscard]] ReadOnlyArray array(int32_t index) const override {
    return target_->array(column_indices_[index]);
  }

  [[nodiscard]] const SimpleSchema* schema() const override { return new_schema_; }

  [[nodiscard]] int64_t length() const override { return target_->length(); }

  Status ExportToArrow([[maybe_unused]] ArrowArray* out) && override {
    return Status::Invalid(
        "You cannot export part of a batch to Arrow (quiver will not split ownership)");
  }

  // Override SelectView so we don't end up with a view into a view into a view...
  [[nodiscard]] std::unique_ptr<ReadOnlyBatch> SelectView(
      std::vector<int32_t> indices, const SimpleSchema* new_schema) const override {
    for (auto& index : indices) {
      index = column_indices_[index];
    }
    return std::make_unique<ViewBatch>(target_, std::move(indices), new_schema);
  }

 private:
  const ReadOnlyBatch* target_;
  const std::vector<int32_t> column_indices_;
  const SimpleSchema* new_schema_;
};

class ImportedBatch : public ReadOnlyBatch {
 public:
  [[nodiscard]] ReadOnlyArray array(int32_t index) const override {
    DCHECK_GE(index, 0);
    DCHECK_LT(index, static_cast<int32_t>(arrays_.size()));
    return arrays_[index];
  }

  [[nodiscard]] const SimpleSchema* schema() const override { return schema_; }
  [[nodiscard]] int64_t length() const override { return length_; }

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
    }

    switch (field.layout) {
      case LayoutKind::kFlat: {
        std::span<const uint8_t> values;
        QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const uint8_t>(
            array, /*index=*/1, length, field.data_width_bytes, field.layout, &values));
        arrays_.emplace_back(
            ReadOnlyFlatArray{validity, values, field.data_width_bytes, length});
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
        break;
      }
      case LayoutKind::kInt64ContiguousList: {
        std::span<const int64_t> offsets;
        QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const int64_t>(
            array, /*index=*/1, length, /*width=*/1, field.layout, &offsets));
        arrays_.emplace_back(
            ReadOnlyInt64ContiguousListArray{validity, offsets, length + 1});
        // There is a child array here but the only variations of int64 list are binary
        // and we handle those specially below
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
      arrays_.emplace_back(ReadOnlyFlatArray{str_validity, str_data, 1, length});
    } else if (field.format == "Z" || field.format == "U") {
      ReadOnlyInt64ContiguousListArray list_arr;
      QUIVER_RETURN_NOT_OK(util::get_or_raise(&arrays_.back(), &list_arr));
      int64_t data_length = list_arr.offsets[length];
      std::span<const uint8_t> str_data;
      std::span<const uint8_t> str_validity;
      QUIVER_RETURN_NOT_OK(GetBufferOrEmptySpan<const uint8_t>(
          array, /*index=*/2, data_length, /*width=*/1, field.layout, &str_data));
      arrays_.emplace_back(ReadOnlyFlatArray{str_validity, str_data, 1, length});
    }
    return Status::OK();
  }

  const SimpleSchema* schema_ = nullptr;
  std::vector<ReadOnlyArray> arrays_;
  ArrowArray backing_array_ = ArrowArray::Default();
  int64_t length_ = 0;

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
    return Status::Invalid("Imported array had ", array->n_children,
                           " children but expected ", schema->num_fields(),
                           " according to the schema");
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
  ~BasicBatch() override = default;
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

  [[nodiscard]] ReadOnlyArray array(int32_t index) const override {
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
  Array mutable_array(int32_t index) override {
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
        DCHECK(false) << "Not yet implemented: mutable_array with layout("
                      << layout::to_string(schema_->types[index].layout) << ")";
        return {};
    }
  }

  [[nodiscard]] const SimpleSchema* schema() const override { return schema_; }
  [[nodiscard]] int64_t length() const override { return length_; }

  struct ExportedArrayData {
    std::vector<ArrowArray> children;
    std::vector<ArrowArray*> child_pointers;
    std::vector<std::vector<uint8_t>> buffers;
    std::vector<const void*> buffer_pointers;
  };

  static void ReleaseArray(ArrowArray* array) {
    if (array->release != nullptr) {
      array->release(array);
    }
  }

  static void ReleaseExportedArray(ArrowArray* array) {
    if (array->release == nullptr) {
      return;
    }
    for (int64_t i = 0; i < array->n_children; ++i) {
      struct ArrowArray* child = array->children[i];
      ReleaseArray(child);
      DCHECK_EQ(child->release, nullptr)
          << "Child release callback should have marked it released";
    }
    DCHECK_NE(array->private_data, nullptr);
    delete reinterpret_cast<ExportedArrayData*>(array->private_data);

    array->release = nullptr;
  }

  Status ExportFlatArray(int array_index, const FieldDescriptor& /*field*/,
                         ExportedArrayData* data, ArrowArray* out) {
    out->n_children = 0;
    out->children = nullptr;
    out->n_buffers = 2;
    out->length = length_;

    data->buffers.reserve(2);
    data->buffer_pointers.reserve(2);
    std::size_t buffer_offset = array_idx_to_buffers_[array_index];
    data->buffers.push_back(std::move(buffers_[buffer_offset]));
    data->buffer_pointers.push_back(data->buffers.back().data());
    data->buffers.push_back(std::move(buffers_[buffer_offset + 1]));
    data->buffer_pointers.push_back(data->buffers.back().data());

    out->buffers = data->buffer_pointers.data();
    return Status::OK();
  }

  Status ExportArray(int array_index, const FieldDescriptor& field, ArrowArray* out) {
    auto* data = new ExportedArrayData();
    out->private_data = reinterpret_cast<void*>(data);
    out->release = ReleaseExportedArray;

    out->offset = 0;
    out->null_count = -1;
    out->dictionary = nullptr;

    switch (field.layout) {
      case LayoutKind::kFlat:
        return ExportFlatArray(array_index, field, data, out);
      default:
        return Status::NotImplemented("ExportArray with layout ",
                                      layout::to_string(field.layout));
    }
  }

  Status ExportToArrow(ArrowArray* out) && override {
    auto* data = new ExportedArrayData();
    out->private_data = reinterpret_cast<void*>(data);
    out->release = ReleaseExportedArray;

    data->children.resize(schema_->num_fields());

    for (int32_t i = 0; i < schema_->num_fields(); ++i) {
      QUIVER_RETURN_NOT_OK(
          ExportArray(i, schema_->top_level_types[i], &data->children[i]));
      data->child_pointers.push_back(&data->children[i]);
    }
    data->buffer_pointers.push_back(nullptr);
    out->children = data->child_pointers.data();
    out->n_children = static_cast<int64_t>(data->child_pointers.size());
    out->buffers = data->buffer_pointers.data();
    out->n_buffers = 1;
    out->dictionary = nullptr;
    out->length = length_;
    out->null_count = 0;
    out->offset = 0;

    return Status::OK();
  }

  void SetLength(int64_t new_length) override { length_ = new_length; }

  void ResizeBufferBytes(int32_t array_index, int32_t buffer_index,
                         int64_t num_bytes) override {
    std::size_t buffer_offset = array_idx_to_buffers_[array_index];
    std::size_t buffer_idx = buffer_offset + buffer_index;
    buffers_[buffer_idx].resize(num_bytes);
  }

  int64_t buffer_capacity(int32_t array_index, int32_t buffer_index) override {
    std::size_t buffer_offset = array_idx_to_buffers_[array_index];
    return static_cast<int64_t>(buffers_[buffer_offset + buffer_index].size());
  }

  Status Combine(Batch&& other, const SimpleSchema* combined_schema_) override {
    auto&& other_cast = dynamic_cast<BasicBatch&&>(other);
    if (length_ != other_cast.length_) {
      return Status::Invalid("Cannot combine batches with different lengths");
    }
    std::size_t num_left_buffers = buffers_.size();
    array_idx_to_buffers_.reserve(array_idx_to_buffers_.size() +
                                  other_cast.array_idx_to_buffers_.size());
    for (std::size_t idx : other_cast.array_idx_to_buffers_) {
      array_idx_to_buffers_.push_back(num_left_buffers + idx);
    }
    buffers_.reserve(buffers_.size() + other_cast.buffers_.size());
    buffers_.insert(buffers_.end(), std::make_move_iterator(other_cast.buffers_.begin()),
                    std::make_move_iterator(other_cast.buffers_.end()));
    schema_ = combined_schema_;
    other_cast.schema_ = nullptr;
    other_cast.length_ = 0;
    other_cast.array_idx_to_buffers_.clear();
    other_cast.buffers_.clear();
    return Status::OK();
  }

 private:
  [[nodiscard]] std::span<const uint8_t> BufferToSpan(std::size_t index,
                                                      uint64_t num_bytes) const {
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
  int64_t length_ = 0;
  std::vector<std::size_t> array_idx_to_buffers_;
  std::vector<std::vector<uint8_t>> buffers_;
};

int64_t ReadOnlyBatch::NumBytes() const {
  int64_t num_bytes = 0;
  for (int array_idx = 0; array_idx < schema()->num_fields(); array_idx++) {
    num_bytes += array::NumBytes(array(array_idx));
  }
  return num_bytes;
}

[[nodiscard]] std::unique_ptr<ReadOnlyBatch> ReadOnlyBatch::SelectView(
    std::vector<int32_t> indices, const SimpleSchema* new_schema) const {
  return std::make_unique<ViewBatch>(this, std::move(indices), new_schema);
}

std::string ReadOnlyBatch::ToString() const {
  std::stringstream sstr;
  sstr << "batch with " << length() << " rows" << std::endl;
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
  if (length() != other.length()) {
    return false;
  }
  for (int array_idx = 0; array_idx < schema()->num_fields(); array_idx++) {
    ReadOnlyArray this_arr = array(array_idx);
    ReadOnlyArray other_arr = other.array(array_idx);
    if (!array::BinaryEquals(this_arr, other_arr)) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] ReadOnlyArray BatchView::array(int32_t index) const {
  ReadOnlyArray base_array = target_->array(index);
  return array::Slice(base_array, offset_, length_);
}

[[nodiscard]] const SimpleSchema* BatchView::schema() const { return target_->schema(); }

Status BatchView::ExportToArrow(ArrowArray* /*out*/) && {
  return Status::Invalid("Cannot convert a slice to Arrow (will not split ownership)");
}

BatchView::BatchView(const ReadOnlyBatch* target, int64_t offset, int64_t length)
    : target_(target), offset_(offset), length_(length) {}

BatchView BatchView::SliceBatch(const ReadOnlyBatch* batch, int64_t offset,
                                int64_t length) {
  DCHECK_EQ(offset % 8, 0) << "offset must be a multiple of 8";
  int64_t remaining = batch->length() - offset;
  int64_t actual_length = length;
  if (remaining < 0) {
    length = 0;
  } else if (remaining < length) {
    actual_length = remaining;
  }
  const auto* maybe_view = dynamic_cast<const BatchView*>(batch);
  if (maybe_view == nullptr) {
    return {batch, offset, actual_length};
  }
  return {maybe_view->target_, offset, actual_length};
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
  int64_t num_buffers = 0;
  for (int i = 0; i < schema->num_types(); i++) {
    num_buffers += layout::num_buffers(schema->types[i].layout);
  }
  int64_t bytes_per_buffer = bit_util::FloorDiv(num_bytes, num_buffers);

  for (int i = 0; i < schema->num_types(); i++) {
    for (int j = 0; j < layout::num_buffers(schema->types[i].layout); j++) {
      batch->ResizeBufferBytes(i, j, bytes_per_buffer);
    }
  }
  return batch;
}

}  // namespace quiver