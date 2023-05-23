// SPDX-License-Identifier: Apache-2.0

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "quiver/core/arrow.h"
#include "quiver/util/status.h"

namespace quiver {

// Note: The order of this enum MUST match the order of the Array variant below
enum class LayoutKind : std::size_t {
  kFlat = 0,
  kInt32ContiguousList = 1,
  kInt64ContiguousList = 2,
  kStructArray = 3,
  kFixedListArray = 4,
  kUnion = 5  // Still TODO
};

namespace layout {

bool is_variable_length(LayoutKind kind);

}  // namespace layout

constexpr std::string_view kLayoutKindNames[] = {
    "flat", "list", "large list", "struct", "fixed size list", "union"};

std::string_view LayoutToString(LayoutKind layout) {
  return kLayoutKindNames[static_cast<std::size_t>(layout)];
}

struct SimpleSchema;

struct FieldDescriptor {
  std::string format;
  std::string name;
  std::string metadata;
  bool nullable;
  bool dict_indices_ordered;
  bool map_keys_sorted;
  int32_t num_children;

  // The index of the type, in the schema
  int32_t index;
  LayoutKind layout;
  // This is the second buffer which is the data values for fixed types and the offsets
  // buffer for variable types.
  //
  // This will be 0 for bool buffers
  // This will be -1 for types without a second buffer (null, fixed width list, struct)
  int data_width_bytes;
  SimpleSchema* schema;

  // Default shallow equality
  auto operator<=>(const FieldDescriptor&) const = default;
  FieldDescriptor& child(int child_index) const;
};

struct SimpleSchema {
  std::vector<FieldDescriptor> types;
  std::vector<FieldDescriptor> top_level_types;
  std::vector<int> top_level_indices;

  int num_fields() const { return static_cast<int>(top_level_indices.size()); }
  int num_types() const { return static_cast<int>(types.size()); }

  const FieldDescriptor& field(int i) { return types[top_level_indices[i]]; }

  static Status ImportFromArrow(ArrowSchema* schema, SimpleSchema* out);

  bool Equals(const SimpleSchema& other) const;
};

struct FlatArray {
  // The validity buffer, may be empty
  std::span<uint8_t> validity;
  // The values buffer for fixed arrays, offsets for variable arrays
  std::span<uint8_t> values;
  // The array type
  const FieldDescriptor* descriptor;
  // The length (in values, not bytes) of the array
  int length;
};

struct ReadOnlyFlatArray {
  std::span<const uint8_t> validity;
  std::span<const uint8_t> values;
  const FieldDescriptor* descriptor = nullptr;
  int length = 0;

  ReadOnlyFlatArray(std::span<const uint8_t> validity, std::span<const uint8_t> values,
                    const FieldDescriptor* descriptor, int length)
      : validity(validity), values(values), descriptor(descriptor), length(length) {}

  ReadOnlyFlatArray(FlatArray view)
      : validity(view.validity),
        values(view.values),
        descriptor(view.descriptor),
        length(view.length) {}
};

template <typename IndexType>
struct ContiguousListArray {
  std::span<uint8_t> validity;
  std::span<IndexType> offsets;
  int length = 0;
};

struct Int32ContiguousListArray : ContiguousListArray<int32_t> {};
struct Int64ContiguousListArray : ContiguousListArray<int64_t> {};

template <typename IndexType>
struct ReadOnlyContiguousListArray {
  std::span<const uint8_t> validity;
  std::span<const IndexType> offsets;
  int length = 0;

  ReadOnlyContiguousListArray() = default;
  ReadOnlyContiguousListArray(std::span<const uint8_t> validity,
                              std::span<const IndexType> offsets, int length)
      : validity(validity), offsets(offsets), length(length) {}
};

struct ReadOnlyInt32ContiguousListArray : ReadOnlyContiguousListArray<int32_t> {
  using ReadOnlyContiguousListArray<int32_t>::ReadOnlyContiguousListArray;
};
struct ReadOnlyInt64ContiguousListArray : ReadOnlyContiguousListArray<int64_t> {
  using ReadOnlyContiguousListArray<int64_t>::ReadOnlyContiguousListArray;
};

struct StructArray {
  std::span<uint8_t> validity;
  int length = 0;
};

struct ReadOnlyStructArray {
  std::span<const uint8_t> validity;
  int length = 0;
};

struct FixedListArray {
  std::span<uint8_t> validity;
  int width = 0;
  int length = 0;
};

struct ReadOnlyFixedListArray {
  std::span<uint8_t> validity;
  int width = 0;
  int length = 0;
};

using Array = std::variant<FlatArray, Int32ContiguousListArray, Int64ContiguousListArray,
                           StructArray, FixedListArray>;
using ReadOnlyArray = std::variant<ReadOnlyFlatArray, ReadOnlyInt32ContiguousListArray,
                                   ReadOnlyInt64ContiguousListArray, ReadOnlyStructArray,
                                   ReadOnlyFixedListArray>;

class ArrayVisitor {
  virtual Status Visit(FlatArray* array) = 0;
  virtual Status Visit(Int32ContiguousListArray* array) = 0;
  virtual Status Visit(Int64ContiguousListArray* array) = 0;
  virtual Status Visit(StructArray* array) = 0;
  virtual Status Visit(FixedListArray* array) = 0;
};

class ReadOnlyArrayVisitor {
  virtual Status Visit(FlatArray* array) = 0;
  virtual Status Visit(Int32ContiguousListArray* array) = 0;
  virtual Status Visit(Int64ContiguousListArray* array) = 0;
  virtual Status Visit(ReadOnlyStructArray* array) = 0;
  virtual Status Visit(ReadOnlyFixedListArray* array) = 0;
};

class ReadOnlyBatch {
 public:
  virtual const ReadOnlyArray& array(int32_t index) const = 0;
  virtual int32_t num_arrays() const = 0;
  virtual const SimpleSchema* schema() const = 0;
};

class FixedBatch : public ReadOnlyBatch {
 public:
  virtual const Array& mutable_array(int32_t index) = 0;
};

class Batch : public FixedBatch {
 public:
  virtual void ResizeArray(int32_t index, int64_t num_rows) = 0;
  virtual void ResizeArrayBytes(int32_t index, int64_t num_bytes) = 0;
};

static Status ImportBatch(ArrowArray* array, SimpleSchema* schema,
                          std::unique_ptr<ReadOnlyBatch>* out);

}  // namespace quiver
