// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "quiver/row/row_p.h"

#include <memory>

#include "quiver/util/variant_p.h"

namespace quiver::row {

int32_t RowSchema::num_varbinary_cols() const {
  int32_t result = 0;
  for (const auto& field : schema->types) {
    if (layout::is_variable_length(field.layout)) {
      ++result;
    }
  }
  return result;
}

bool RowSchema::IsCompatible(const RowSchema& other) const {
  if (other.num_cols() != num_cols()) {
    return false;
  }
  if (row_alignment != other.row_alignment ||
      string_alignment != other.string_alignment) {
    return false;
  }
  return schema->Equals(*other.schema);
}

namespace {
Status CheckSupportsSchema(const SimpleSchema& schema) {
  for (const auto& type : schema.top_level_types) {
    if (type.layout == LayoutKind::kStructArray ||
        type.layout == LayoutKind::kFixedListArray) {
      // Not implemented yet, but should be doable
      return Status::NotImplemented(
          "Struct and FixedSizeList aren't yet supported in the row table");
    }
    if (type.layout == LayoutKind::kUnion) {
      // Not implemented, an probably never will be
      return Status::Invalid("Unions will never be supported in the row table");
    }
    if (type.layout == LayoutKind::kInt32ContiguousList ||
        type.layout == LayoutKind::kInt64ContiguousList) {
      const FieldDescriptor& child_type = type.child(0);
      if (child_type.layout != LayoutKind::kFlat) {
        return Status::Invalid(
            "Nested variable length types will never be supported in the row table");
      }
    }
  }
  return Status::OK();
}

bool IsInitialized(const RowSchema& metadata) { return metadata.schema != nullptr; }

// Rounding up offset to the beginning of next column,
// choosing required alignment based on the data type of that column.
int32_t PaddingNeeded(int32_t offset, int32_t string_alignment,
                      const FieldDescriptor& field) {
  if (bit_util::IsPwr2OrZero(field.data_width_bytes)) {
    return 0;
  }
  return bit_util::PaddingNeededPwr2(offset, string_alignment);
}

}  // namespace

Status RowSchema::Initialize(const SimpleSchema& schema) {
  QUIVER_RETURN_NOT_OK(CheckSupportsSchema(schema));
  if (IsInitialized(*this)) {
    return Status::Invalid("Attempt to initialize non-empty metadata");
  }
  this->schema = &schema;

  const auto num_cols = static_cast<int32_t>(schema.types.size());

  is_fixed_length = true;

  column_offsets.resize(num_cols);
  int32_t offset_within_row = 0;
  for (int32_t i = 0; i < num_cols; ++i) {
    const FieldDescriptor& col = schema.top_level_types[i];
    offset_within_row += PaddingNeeded(offset_within_row, string_alignment, col);
    column_offsets[i] = offset_within_row;
    is_fixed_length &= !layout::is_variable_length(col.layout);
    if (col.data_width_bytes == 0) {
      // Boolean columns are stored as an entire byte in row-encoding
      offset_within_row += 1;
    } else {
      offset_within_row += col.data_width_bytes;
    }
  }

  if (is_fixed_length) {
    fixed_length =
        offset_within_row + bit_util::PaddingNeededPwr2(offset_within_row, row_alignment);
  } else {
    fixed_length = offset_within_row +
                   bit_util::PaddingNeededPwr2(offset_within_row, string_alignment);
  }

  // We set the number of bytes per row storing null masks of individual key columns
  // to be a power of two. This is not required. It could be also set to the minimal
  // number of bytes required for a given number of bits (one bit per column).
  null_masks_bytes_per_row = 1;
  while (static_cast<int32_t>(null_masks_bytes_per_row * 8) < num_cols) {
    null_masks_bytes_per_row *= 2;
  }

  return Status::OK();
}

// void RowTableEncoder::PrepareEncodeSelected(int64_t start_row, int64_t num_rows,
//                                             const std::vector<KeyColumnArray>& cols) {
//   // Prepare column array vectors
//   PrepareKeyColumnArrays(start_row, num_rows, cols);
// }

class FlatEncoder {
 public:
  FlatEncoder(const FieldDescriptor& field) : field_(field) {
    width_ = field_.data_width_bytes;
  }

  void Prepare(const ReadOnlyArray& array) {
    const auto& flat_array = std::get<ReadOnlyFlatArray>(array);
    values_ = flat_array.values.data();
    validity_ = flat_array.validity.data();
    bitmask_ = 1;
  }

  void EncodeValue(Sink* sink) {
    sink->CopyInto(values_, width_);
    values_ += width_;
  }

  bool EncodeValid() {
    if (validity_ == nullptr) {
      return true;
    }
    bool is_valid = (*validity_ & bitmask_) != 0;
    bitmask_ <<= 1;
    if (bitmask_ == 0) {
      bitmask_ = 1;
      validity_++;
    }
    return is_valid;
  }

  bool MayHaveNulls() { return validity_ != nullptr; }

 private:
  const FieldDescriptor& field_;
  const uint8_t* values_;
  const uint8_t* validity_;
  int width_;
  uint8_t bitmask_;
};

template <typename ArrayType, typename OffsetType>
class ContiguousListEncoder {
 public:
  void Prepare(const ReadOnlyArray& array) {
    const ArrayType& clist_array = std::get<ArrayType>(array);
    offsets = clist_array.offsets.data();
    validity = clist_array.validity.data();
    bitmask = 1;
  }

  void EncodeOffset(Sink* sink) {
    sink->CopyInto(offsets, sizeof(OffsetType));
    offsets++;
  }

 private:
  const OffsetType* offsets;
  const uint8_t* data;
  const uint8_t* validity;
  uint8_t bitmask;
};

class RowQueueAppendingProducerImpl : public RowQueueAppendingProducer {
 public:
  RowQueueAppendingProducerImpl(const RowSchema* schema, Sink* sink)
      : schema_(schema), sink_(sink) {}

  Status Initialize() {
    for (std::size_t field_idx = 0; field_idx < schema_->schema->top_level_types.size();
         field_idx++) {
      const auto& field = schema_->schema->top_level_types[field_idx];
      switch (field.layout) {
        case LayoutKind::kFlat:
          flat_col_indices.push_back(static_cast<int32_t>(field_idx));
          flat_encoders.emplace_back(field);
          break;
        default:
          return Status::Invalid("No row based encoder yet provided for layout ",
                                 layout::to_string(field.layout));
      }
    }
    return Status::OK();
  }

  Status Append(const ReadOnlyBatch& batch) override {
    // Prepare
    for (std::size_t flat_idx = 0; flat_idx < flat_col_indices.size(); flat_idx++) {
      int32_t field_idx = flat_col_indices[flat_idx];
      flat_encoders[flat_idx].Prepare(batch.array(field_idx));
    }
    int64_t batch_length = batch.length();
    for (int64_t row_idx = 0; row_idx < batch_length; row_idx++) {
      // Encode values
      for (auto& flat_encoder : flat_encoders) {
        flat_encoder.EncodeValue(sink_);
      }
      // Encode validity
      uint8_t bitmask = 1;
      uint8_t validity_byte = 0;
      for (auto& flat_encoder : flat_encoders) {
        if (flat_encoder.EncodeValid()) {
          validity_byte |= bitmask;
        }
        bitmask <<= 1;
        if (bitmask == 0) {
          bitmask = 1;
          sink_->CopyInto(validity_byte);
        }
      }
      if (bitmask != 1) {
        sink_->CopyInto(validity_byte);
      }
    }
    return Status::OK();
  }

 private:
  const RowSchema* schema_;
  Sink* sink_;
  std::vector<int32_t> flat_col_indices;
  std::vector<FlatEncoder> flat_encoders;
  std::vector<FlatEncoder> flat_encoders_may_have_nulls;
  std::vector<int32_t> clist_int32_indices;
  std::vector<ContiguousListEncoder<Int32ContiguousListArray, int32_t>>
      clist_int32_encoders;
  std::vector<int32_t> clist_int64_indices;
  std::vector<ContiguousListEncoder<Int64ContiguousListArray, int64_t>>
      clist_int64_encoders;
};

Status RowQueueAppendingProducer::Create(
    const RowSchema* schema, Sink* sink,
    std::unique_ptr<RowQueueAppendingProducer>* out) {
  auto impl = std::make_unique<RowQueueAppendingProducerImpl>(schema, sink);
  QUIVER_RETURN_NOT_OK(impl->Initialize());
  *out = std::move(impl);
  return Status::OK();
}

}  // namespace quiver::row
