// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <array>
#include <cstdint>
#include <iosfwd>
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
std::string_view to_string(LayoutKind layout);
int num_buffers(LayoutKind layout);

}  // namespace layout

struct SimpleSchema;

struct FieldDescriptor {
  std::string format;
  std::string name;
  std::string metadata;
  bool nullable = false;
  bool dict_indices_ordered = false;
  bool map_keys_sorted = false;
  int32_t num_children = 0;

  // The index of the type, in the schema
  int32_t index = 0;
  LayoutKind layout = LayoutKind::kFlat;
  // This is the second buffer which is the data values for fixed types and the offsets
  // buffer for variable types.
  //
  // This will be 0 for bool buffers
  // This will be -1 for types without a second buffer (null, fixed width list, struct)
  int data_width_bytes = 0;
  SimpleSchema* schema = nullptr;

  // Default shallow equality
  auto operator<=>(const FieldDescriptor&) const = default;
  [[nodiscard]] FieldDescriptor& child(int child_index) const;
  [[nodiscard]] std::string ToString() const;
};

struct SimpleSchema {
  std::vector<FieldDescriptor> types;
  std::vector<FieldDescriptor> top_level_types;
  std::vector<int> top_level_indices;

  [[nodiscard]] int num_fields() const {
    return static_cast<int>(top_level_indices.size());
  }
  [[nodiscard]] int num_types() const { return static_cast<int>(types.size()); }

  [[nodiscard]] const FieldDescriptor& field(int field_idx) const {
    return types[top_level_indices[field_idx]];
  }
  [[nodiscard]] SimpleSchema Select(const std::vector<int32_t>& field_indices) const;

  /// <summary>
  /// Converts from a C data schema to a quiver schema
  /// </summary>
  /// By default the C data schema is consumed (its `release` will be set to nullptr)
  /// whether the import succeeds or fails.
  ///
  /// This is merely a convenience.  The import process fully copies all data from the
  /// source schema to the quiver schema and so there is no confusion of ownership.
  ///
  /// By setting `consume_schema` to false the C data schema is never consumed.
  static Status ImportFromArrow(ArrowSchema* schema, SimpleSchema* out,
                                bool consume_schema = true);

  static SimpleSchema AllColumnsFrom(const SimpleSchema& left, const SimpleSchema& right);

  Status ExportToArrow(ArrowSchema* out) const;
  [[nodiscard]] std::string ToString() const;

  [[nodiscard]] bool Equals(const SimpleSchema& other) const;
};

struct FlatArray {
  // The validity buffer, may be empty
  std::span<uint8_t> validity;
  // The values buffer for fixed arrays, offsets for variable arrays
  std::span<uint8_t> values;
  // The width (in bytes) of each item in the array
  int32_t width_bytes;
  // The length (in values, not bytes) of the array
  int64_t length = 0;
};

struct ReadOnlyFlatArray {
  std::span<const uint8_t> validity;
  std::span<const uint8_t> values;
  int32_t width_bytes;
  int64_t length = 0;

  static ReadOnlyFlatArray View(FlatArray array) {
    return {array.validity, array.values, array.width_bytes, array.length};
  }
};

template <typename IndexType>
struct ContiguousListArray {
  std::span<uint8_t> validity;
  std::span<IndexType> offsets;
  int64_t length = 0;
};

struct Int32ContiguousListArray : ContiguousListArray<int32_t> {};
struct Int64ContiguousListArray : ContiguousListArray<int64_t> {};

template <typename IndexType>
struct ReadOnlyContiguousListArray {
  std::span<const uint8_t> validity;
  std::span<const IndexType> offsets;
  int64_t length = 0;
};

struct ReadOnlyInt32ContiguousListArray : ReadOnlyContiguousListArray<int32_t> {};
struct ReadOnlyInt64ContiguousListArray : ReadOnlyContiguousListArray<int64_t> {};

struct StructArray {
  std::span<uint8_t> validity;
  int64_t length = 0;
};

struct ReadOnlyStructArray {
  std::span<const uint8_t> validity;
  int64_t length = 0;
};

struct FixedListArray {
  std::span<uint8_t> validity;
  int width = 0;
  int64_t length = 0;
};

struct ReadOnlyFixedListArray {
  std::span<uint8_t> validity;
  int width = 0;
  int64_t length = 0;
};

constexpr std::array<LayoutKind, 5> kArrayVariantIdxToLayout = {
    LayoutKind::kFlat, LayoutKind::kInt32ContiguousList, LayoutKind::kInt64ContiguousList,
    LayoutKind::kStructArray, LayoutKind::kFixedListArray};
using Array = std::variant<FlatArray, Int32ContiguousListArray, Int64ContiguousListArray,
                           StructArray, FixedListArray>;
using ReadOnlyArray = std::variant<ReadOnlyFlatArray, ReadOnlyInt32ContiguousListArray,
                                   ReadOnlyInt64ContiguousListArray, ReadOnlyStructArray,
                                   ReadOnlyFixedListArray>;

namespace buffer {
void PrintBitmap(std::span<const uint8_t> bitmap, int length, int indentation_level,
                 int max_chars, std::ostream& out);
void PrintBoolmap(std::span<const uint8_t> bitmap, int length, int indentation_level,
                  int max_chars, std::ostream& out);
void PrintImplicitBitmap(int length, int indentation_level, int max_chars,
                         std::ostream& out);
void PrintBuffer(std::span<const uint8_t> buffer, int bytes_per_element,
                 int indentation_level, int max_chars, std::ostream& out);
bool BinaryEquals(std::span<const uint8_t> lhs, std::span<const uint8_t> rhs);
bool BinaryEqualsWithSelection(std::span<const uint8_t> lhs, std::span<const uint8_t> rhs,
                               int32_t element_size_bytes,
                               std::span<const uint8_t> selection);
bool IsAllSet(std::span<const uint8_t> span);
}  // namespace buffer

namespace array {

Array EmptyArray(LayoutKind layout);
ReadOnlyArray ArrayView(Array array);
ReadOnlyArray Slice(ReadOnlyArray array, int64_t offset, int64_t length);
LayoutKind ArrayLayout(Array array);
LayoutKind ArrayLayout(ReadOnlyArray array);
void PrintArray(ReadOnlyArray array, const FieldDescriptor& type, int indentation_level,
                int max_chars, std::ostream& out);
std::string ToString(ReadOnlyArray array, const FieldDescriptor& type);
bool BinaryEquals(ReadOnlyArray lhs, ReadOnlyArray rhs);
bool HasNulls(ReadOnlyArray arr);
int64_t NumBytes(ReadOnlyArray arr);

}  // namespace array

class ArrayVisitor {
 public:
  virtual ~ArrayVisitor() = default;
  virtual Status Visit(FlatArray* array) = 0;
  virtual Status Visit(Int32ContiguousListArray* array) = 0;
  virtual Status Visit(Int64ContiguousListArray* array) = 0;
  virtual Status Visit(StructArray* array) = 0;
  virtual Status Visit(FixedListArray* array) = 0;
};

class ReadOnlyArrayVisitor {
 public:
  virtual ~ReadOnlyArrayVisitor() = default;
  virtual Status Visit(FlatArray* array) = 0;
  virtual Status Visit(Int32ContiguousListArray* array) = 0;
  virtual Status Visit(Int64ContiguousListArray* array) = 0;
  virtual Status Visit(ReadOnlyStructArray* array) = 0;
  virtual Status Visit(ReadOnlyFixedListArray* array) = 0;
};

/// <summary>
/// A batch of memory that has zero or more arrays
///
/// Batches own their memory and free it upon destruction
///
/// In quiver, batches are flat.  This means that nested structures will have multiple
/// entries in the batch.  For example, a list array at index 0 will have a child array at
/// index 1 which contains the list items.  This child array will not have the same length
/// as the list array
///
/// However, all "top-level" arrays (as described by the schema) will have the same length
/// </summary>
class ReadOnlyBatch {
 public:
  ReadOnlyBatch() = default;
  /// <summary>
  /// Destroys the batch and frees any associated memory.  Arrays in a batch
  /// should no longer be used after it is destroyed
  /// </summary>
  virtual ~ReadOnlyBatch() = default;
  ReadOnlyBatch(const ReadOnlyBatch&) = delete;
  ReadOnlyBatch& operator=(const ReadOnlyBatch&) = delete;
  /// <summary>
  /// Access an array in the batch, by index
  /// </summary>
  [[nodiscard]] virtual ReadOnlyArray array(int32_t index) const = 0;
  /// <summary>
  /// A view into the schema of the batch
  ///
  /// Batches do not own their schemas.  The schema is usually associated with
  /// a queue or more permanent structure which may have many batches.
  /// </summary>
  [[nodiscard]] virtual const SimpleSchema* schema() const = 0;
  /// <summary>
  /// The number of rows in top-level arrays in this batch
  /// </summary>
  [[nodiscard]] virtual int64_t length() const = 0;
  /// <summary>
  /// The number of bytes occupied by the arrays
  /// </summary>
  [[nodiscard]] int64_t NumBytes() const;
  /// <summary>
  /// Exports the batch to the C data interface.
  /// </summary>
  ///
  /// Once this method has been called then ownership of the data is passed to the
  /// ArrowArray. Any attempt to use this instance after this method is called will result
  /// in undefined behavior.
  virtual Status ExportToArrow(ArrowArray* out) && = 0;

  /// <summary>
  /// Returns a view into some columns of this batch.
  /// </summary>
  ///
  /// The returned batch is a view only and should not outlive the source batch.
  [[nodiscard]] virtual std::unique_ptr<ReadOnlyBatch> SelectView(
      std::vector<int32_t> indices, const SimpleSchema* new_schema) const;

  [[nodiscard]] std::string ToString() const;
  [[nodiscard]] bool BinaryEquals(const ReadOnlyBatch& other) const;
};

class BatchView : public ReadOnlyBatch {
 public:
  static BatchView SliceBatch(const ReadOnlyBatch* batch, int64_t offset, int64_t length);

  [[nodiscard]] ReadOnlyArray array(int32_t index) const override;
  [[nodiscard]] const SimpleSchema* schema() const override;
  [[nodiscard]] int64_t length() const override { return length_; };
  Status ExportToArrow(ArrowArray* out) && override;

 private:
  BatchView(const ReadOnlyBatch* target, int64_t offset, int64_t length);

  const ReadOnlyBatch* target_;
  int64_t offset_;
  int64_t length_;
};

/// <summary>
/// A batch whose arrays can be modified
/// </summary>
class MutableBatch : public ReadOnlyBatch {
 public:
  ~MutableBatch() override = default;
  /// <summary>
  /// Access an array for modification, by index
  /// </summary>
  virtual Array mutable_array(int32_t index) = 0;
};

/// <summary>
/// A batch whose arrays can be resized
/// </summary>
class Batch : public MutableBatch {
 public:
  ~Batch() override = default;
  /// <summary>
  /// Resizes a buffer in the batch.  In addition to allocating more memory this may
  /// require copying the bytes from the old buffer into the new buffer.  This also sets
  /// the bytes to zero
  /// </summary>
  // TODO: THIS CAN FAIL
  virtual void ResizeBufferBytes(int32_t array_index, int32_t buffer_index,
                                 int64_t num_bytes) = 0;
  /// <summary>
  /// Sets the length for top-level arrays in this batch.  This does not resize any
  /// buffers.  The caller is responsible for doing that.
  /// </summary>
  virtual void SetLength(int64_t new_length) = 0;
  /// <summary>
  /// Fetch the capacity of the buffer.  This is how large the buffer can be resized
  /// before a new allocation will be required.
  /// </summary>
  [[nodiscard]] virtual int64_t buffer_capacity(int32_t array_index,
                                                int32_t buffer_index) = 0;

  void ResizeFixedParts(int32_t array_index, int64_t new_length);

  virtual Status Combine(Batch&& other, const SimpleSchema* combined_schema_) = 0;

  /// <summary>
  /// Creates a basic instance of Batch
  /// </summary>
  /// In this basic instance each buffer is a separate allocation.  Resizing will perform
  /// a reallocation if the buffer is not already large enough and copy the data.
  static std::unique_ptr<Batch> CreateBasic(const SimpleSchema* schema);
  /// <summary>
  /// Creates a basic batch that has already been allocated some space
  /// </summary>
  /// The `num_bytes` space will be allocated evenly across all buffers.  In practice this
  /// is unlikely to be useful if the batch will contain any variable length buffers.
  /// However, for benchmarking, testing, or various other cases it can be useful to do
  /// all of the allocations up-front so they don't show up in results.
  static std::unique_ptr<Batch> CreateInitializedBasic(const SimpleSchema* schema,
                                                       int64_t num_bytes);
};

/// <summary>
/// Consumes an ArrowArray to create a ReadOnlyBatch
/// </summary>
///
/// This converts from the C data standard to quiver's internal representation.  This is a
/// zero copy operation.  No buffers will be copied, only metadata.
///
/// The input array will be consumed (its `release` will be set to nullptr).  This is true
/// even if there is an error in conversion (in this case the input array will immediately
/// be released).
///
/// The data will be released when the newly created ReadOnlyBatch is destroyed.
Status ImportBatch(ArrowArray* array, const SimpleSchema* schema,
                   std::unique_ptr<ReadOnlyBatch>* out);

}  // namespace quiver
