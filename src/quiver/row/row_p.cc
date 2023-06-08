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

/// Description of the data stored in a RowBatch
///
/// Each row (with N fixed-length columns and M variable-length columns) is stored in the
/// following form:
///
/// | FIX_0 | FIX_1 | . | FIX_N | VALID_0 | VALID_LOG_2(N) | VAR_0 | VAR_1 | . | VAR_M |
///
/// FIX_x is the fixed-length portion of the column value
/// VALID_x is the validity-bitmap of the row
/// VAR_x is the variable-length portion of the column value
///
/// Each value is aligned as follows:
///
/// A fixed value, that is a bit, is packed into a byte with no alignment
/// A fixed value, that is a power of 2 # of bytes, has no padding
/// A fixed value, that is a non-power of 2 # of bytes, is padded to string_alignment
/// Variable length values are padded to string_alignment
///
/// TODO: Why pad bytes to string_alignment?  Does it matter?
///
/// Each row is aligned to row_alignment
struct RowSchema {
  /// \brief True if there are no variable length columns in the table
  bool is_fixed_length = true;

  /// The size of the fixed portion of the row
  ///
  /// If this is the entire row then this will be a multiple of row_alignment
  ///
  /// If there are any varlength columns then this will be a multiple of string_alignment
  int32_t fixed_length = 0;

  /// Fixed number of bytes per row that are used to encode null masks.
  /// Null masks indicate for a single row which of its columns are null.
  /// Nth bit in the sequence of bytes assigned to a row represents null
  /// information for Nth field according to the order in which they are encoded.
  int32_t null_masks_bytes_per_row = 0;

  int32_t null_masks_offset = 0;

  /// Power of 2. Every row will start at an offset aligned to that number of bytes.
  int32_t row_alignment;

  /// Power of 2. Must be no greater than row alignment.
  /// Every non-power-of-2 binary field and every varbinary field bytes
  /// will start aligned to that number of bytes.
  int32_t string_alignment;

  const SimpleSchema* schema = nullptr;

  /// Offsets within a row to fields in their encoding order.
  std::vector<int32_t> column_offsets;

  RowSchema(int32_t row_alignment, int32_t string_alignment)
      : row_alignment(row_alignment), string_alignment(string_alignment) {}

  Status Initialize(const SimpleSchema& schema);

  [[nodiscard]] int32_t encoded_field_offset(int32_t icol) const {
    return column_offsets[icol];
  }

  [[nodiscard]] int32_t num_cols() const { return schema->num_fields(); }

  [[nodiscard]] int32_t num_varbinary_cols() const;

  /// \brief True if `other` is based on the same schema
  [[nodiscard]] bool IsCompatible(const RowSchema& other) const;
};

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
    column_offsets[i] = offset_within_row;
    is_fixed_length &= !layout::is_variable_length(col.layout);
    if (col.data_width_bytes == 0) {
      // Boolean columns are stored as an entire byte in row-encoding
      offset_within_row += 1;
    } else {
      offset_within_row += col.data_width_bytes;
    }
  }

  null_masks_bytes_per_row = bit_util::CeilDiv(num_cols, 8);

  fixed_length = offset_within_row + null_masks_bytes_per_row;

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

  void EncodeValue(StreamSink* sink) {
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
  const uint8_t* values_{};
  const uint8_t* validity_{};
  int width_;
  uint8_t bitmask_{};
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

  void EncodeOffset(StreamSink* sink) {
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
  RowQueueAppendingProducerImpl(RowSchema schema, StreamSink* sink)
      : schema_(std::move(schema)), sink_(sink) {}

  Status Initialize() {
    for (std::size_t field_idx = 0; field_idx < schema_.schema->top_level_types.size();
         field_idx++) {
      const auto& field = schema_.schema->top_level_types[field_idx];
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
  RowSchema schema_;
  StreamSink* sink_;
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
    const SimpleSchema* schema, StreamSink* sink,
    std::unique_ptr<RowQueueAppendingProducer>* out) {
  RowSchema row_schema(sizeof(int64_t), sizeof(int64_t));
  QUIVER_RETURN_NOT_OK(row_schema.Initialize(*schema));
  auto impl =
      std::make_unique<RowQueueAppendingProducerImpl>(std::move(row_schema), sink);
  QUIVER_RETURN_NOT_OK(impl->Initialize());
  *out = std::move(impl);
  return Status::OK();
}

class FlatDecoder {
 public:
  FlatDecoder(const FieldDescriptor* field, int32_t field_idx)
      : field_(field), field_index_(field_idx) {}

  [[nodiscard]] int32_t fixed_width() const { return field_->data_width_bytes; }

  void Prepare(int32_t num_rows, Batch* out) {
    int num_validity_bytes = bit_util::CeilDiv(num_rows, 8);
    out->ResizeBufferBytes(field_index_, 0, num_validity_bytes);
    int num_value_bytes = num_rows * field_->data_width_bytes;
    out->ResizeBufferBytes(field_index_, 1, num_value_bytes);
    FlatArray out_array = std::get<FlatArray>(out->mutable_array(field_index_));
    values_itr_ = out_array.values.data();
    validity_itr_ = out_array.validity.data();
    std::memset(validity_itr_, 0, num_validity_bytes);
    validity_bit_mask_ = 1;
  }

  void DecodeValue(RandomAccessSource* source, int64_t src_offset) {
    source->CopyFrom(values_itr_, src_offset, field_->data_width_bytes);
    values_itr_ += field_->data_width_bytes;
  }

  void AdvanceValidity() {
    validity_bit_mask_ <<= 1;
    if (validity_bit_mask_ == 0) {
      validity_bit_mask_ = 1;
      validity_itr_++;
    }
  }

  void DecodeValid() {
    *validity_itr_ |= validity_bit_mask_;
    AdvanceValidity();
  }

  void DecodeNull() { AdvanceValidity(); }

  const FieldDescriptor* field_;
  int32_t field_index_;
  uint8_t* values_itr_{};
  uint8_t* validity_itr_{};
  uint8_t validity_bit_mask_{};
};

class RowQueueRandomAccessConsumerImpl : public RowQueueRandomAccessConsumer {
 public:
  RowQueueRandomAccessConsumerImpl(RowSchema schema, RandomAccessSource* source)
      : schema_(std::move(schema)), source_(source) {}

  Status Initialize() {
    for (std::size_t field_idx = 0; field_idx < schema_.schema->top_level_types.size();
         field_idx++) {
      const auto& field = schema_.schema->top_level_types[field_idx];
      switch (field.layout) {
        case LayoutKind::kFlat:
          flat_decoders_.emplace_back(&field, static_cast<int>(field_idx));
          break;
        default:
          return Status::Invalid("No row based encoder yet provided for layout ",
                                 layout::to_string(field.layout));
      }
    }
    int32_t num_validity_bytes = bit_util::CeilDiv(schema_.schema->num_fields(), 8);
    validity_scratch_.resize(num_validity_bytes);
    return Status::OK();
  }

  Status Load(std::span<int32_t> indices, Batch* out) override {
    out->SetLength(static_cast<int32_t>(indices.size()));
    for (auto& flat_decoder : flat_decoders_) {
      flat_decoder.Prepare(static_cast<int32_t>(indices.size()), out);
    }
    for (int32_t index : indices) {
      int64_t field_offset = static_cast<int64_t>(schema_.fixed_length) * index;
      for (auto& flat_decoder : flat_decoders_) {
        flat_decoder.DecodeValue(source_, field_offset);
        field_offset += flat_decoder.fixed_width();
      }
      // Load validity bytes
      source_->CopyFrom(validity_scratch_.data(), field_offset,
                        static_cast<int32_t>(validity_scratch_.size()));
      auto flat_decoders_itr = flat_decoders_.begin();
      uint8_t bitmask = 1;
      auto validity_itr = validity_scratch_.begin();
      while (flat_decoders_itr != flat_decoders_.end()) {
        bool valid = (bitmask & *validity_itr) != 0;
        if (!valid) {
          flat_decoders_itr->DecodeNull();
        } else {
          flat_decoders_itr->DecodeValid();
        }
        bitmask <<= 1;
        if (bitmask == 0) {
          bitmask = 1;
          validity_itr++;
        }
        flat_decoders_itr++;
      }
    }
    return Status::OK();
  }

 private:
  RowSchema schema_;
  RandomAccessSource* source_;

  std::vector<FlatDecoder> flat_decoders_;
  std::vector<uint8_t> validity_scratch_;
};

Status RowQueueRandomAccessConsumer::Create(
    const SimpleSchema* schema, RandomAccessSource* source,
    std::unique_ptr<RowQueueRandomAccessConsumer>* out) {
  RowSchema row_schema(sizeof(int64_t), sizeof(int64_t));
  QUIVER_RETURN_NOT_OK(row_schema.Initialize(*schema));
  auto impl =
      std::make_unique<RowQueueRandomAccessConsumerImpl>(std::move(row_schema), source);
  QUIVER_RETURN_NOT_OK(impl->Initialize());
  *out = std::move(impl);
  return Status::OK();
}

}  // namespace quiver::row
