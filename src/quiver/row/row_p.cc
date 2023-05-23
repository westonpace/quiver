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

}  // namespace

Status RowSchema::Initialize(const SimpleSchema& schema) {
  QUIVER_RETURN_NOT_OK(CheckSupportsSchema(schema));
  if (IsInitialized(*this)) {
    return Status::Invalid("Attempt to initialize non-empty metadata");
  }
  this->schema = &schema;

  const auto num_cols = static_cast<int32_t>(schema.types.size());

  // Sort columns.
  //
  // Columns are sorted based on the size in bytes of their fixed-length part.
  // For the varying-length column, the fixed-length part is the 32-bit field storing
  // cumulative length of varying-length fields.
  //
  // The rules are:
  //
  // a) Boolean column, marked with fixed-length 0, is considered to have fixed-length
  // part of 1 byte.
  //
  // b) Columns with fixed-length part being power of 2 or multiple of row
  // alignment precede other columns. They are sorted in decreasing order of the size of
  // their fixed-length part.
  //
  // c) Fixed-length columns precede varying-length columns when
  // both have the same size fixed-length part.
  //
  column_order.resize(num_cols);
  for (int32_t i = 0; i < num_cols; ++i) {
    column_order[i] = i;
  }
  std::sort(column_order.begin(), column_order.end(),
            [&](int32_t left_index, int32_t right_index) {
              const FieldDescriptor& left = schema.types[left_index];
              const FieldDescriptor& right = schema.types[right_index];
              bool is_left_fixedlen = !layout::is_variable_length(left.layout);
              bool is_right_fixedlen = !layout::is_variable_length(right.layout);
              bool is_left_pow2 =
                  !is_left_fixedlen || bit_util::IsPwr2OrZero(left.data_width_bytes);
              bool is_right_pow2 =
                  !is_right_fixedlen || bit_util::IsPwr2OrZero(right.data_width_bytes);
              int32_t width_left = left.data_width_bytes;
              int32_t width_right = right.data_width_bytes;
              if (is_left_pow2 != is_right_pow2) {
                return is_left_pow2;
              }
              if (!is_left_pow2) {
                return left_index < right_index;
              }
              if (width_left != width_right) {
                return width_left > width_right;
              }
              if (is_left_fixedlen != is_right_fixedlen) {
                return is_left_fixedlen;
              }
              return left_index < right_index;
            });
  inverse_column_order.resize(num_cols);
  for (int32_t i = 0; i < num_cols; ++i) {
    inverse_column_order[column_order[i]] = i;
  }

  is_fixed_length = true;

  column_offsets.resize(num_cols);
  int32_t num_varbinary_cols = 0;
  int32_t offset_within_row = 0;
  for (int32_t i = 0; i < num_cols; ++i) {
    const FieldDescriptor& col = schema.top_level_types[column_order[i]];
    offset_within_row +=
        RowSchema::PaddingNeeded(offset_within_row, string_alignment, col);
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
  void Prepare(const ReadOnlyArray& array, const FieldDescriptor& field) {
    const auto& flat_array = std::get<ReadOnlyFlatArray>(array);
    values = flat_array.values.data();
    validity = flat_array.validity.data();
    width = field.data_width_bytes;
    bitmask = 1;
  }

  void EncodeValue(Sink* sink) {
    sink->CopyInto(values, width);
    values += width;
  }

  bool EncodeValid() {
    bool is_valid = (*validity & bitmask) != 0;
    bitmask <<= 1;
    if (bitmask == 0) {
      bitmask = 1;
      validity++;
    }
    return is_valid;
  }

 private:
  const uint8_t* values;
  const uint8_t* validity;
  int width;
  uint8_t bitmask;
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

  bool EncodeValid() {}

 private:
  const OffsetType* offsets;
  const uint8_t* data;
  const uint8_t* validity;
  uint8_t bitmask;
};

class RowQueueAppendingProducerImpl : public RowQueueAppendingProducer {
 public:
  Status Append(const ReadOnlyBatch& batch) override {
    // Prepare
    for (std::size_t flat_idx = 0; flat_idx < flat_col_indices.size(); flat_idx++) {
      int32_t field_idx = flat_col_indices[flat_idx];
      const FieldDescriptor& field = batch.schema()->top_level_types[field_idx];
      flat_encoders[flat_idx].Prepare(batch.array(field_idx), field);
    }
    for (int64_t row_idx = 0; row_idx < batch.length(); row_idx++) {
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
  Sink* sink_;
  std::vector<int32_t> flat_col_indices;
  std::vector<FlatEncoder> flat_encoders;
  std::vector<int32_t> clist_int32_indices;
  std::vector<ContiguousListEncoder<Int32ContiguousListArray, int32_t>>
      clist_int32_encoders;
  std::vector<int32_t> clist_int64_indices;
  std::vector<ContiguousListEncoder<Int64ContiguousListArray, int64_t>>
      clist_int64_encoders;
};

// Status
// RowQueueAppendingProducer::Append(const Batch& batch) {
//   for (int row_idx = 0; row_idx < batch.) rows->Clean();
//   RETURN_NOT_OK(
//       rows->AppendEmpty(static_cast<uint32_t>(num_selected),
//       static_cast<uint32_t>(0)));

//   EncoderOffsets::GetRowOffsetsSelected(rows, batch_varbinary_cols_, num_selected,
//                                         selection);

//   RETURN_NOT_OK(rows->AppendEmpty(static_cast<uint32_t>(0),
//                                   static_cast<uint32_t>(rows->offsets()[num_selected])));

//   for (size_t icol = 0; icol < batch_all_cols_.size(); ++icol) {
//     if (batch_all_cols_[icol].metadata().is_fixed_length) {
//       uint32_t offset_within_row = rows->metadata().column_offsets[icol];
//       EncoderBinary::EncodeSelected(offset_within_row, rows, batch_all_cols_[icol],
//                                     num_selected, selection);
//     }
//   }

//   EncoderOffsets::EncodeSelected(rows, batch_varbinary_cols_, num_selected, selection);

//   for (size_t icol = 0; icol < batch_varbinary_cols_.size(); ++icol) {
//     EncoderVarBinary::EncodeSelected(static_cast<uint32_t>(icol), rows,
//                                      batch_varbinary_cols_[icol], num_selected,
//                                      selection);
//   }

//   EncoderNulls::EncodeSelected(rows, batch_all_cols_, num_selected, selection);

//   return Status::OK();
// }

// RowBatchImpl::RowBatchImpl() : pool_(nullptr), rows_capacity_(0), bytes_capacity_(0) {}

// Status RowBatchImpl::Init(MemoryPool* pool, const RowSchema& metadata) {
//   pool_ = pool;
//   metadata_ = metadata;

//   DCHECK(!null_masks_ && !offsets_ && !rows_);

//   constexpr int64_t kInitialRowsCapacity = 8;
//   constexpr int64_t kInitialBytesCapacity = 1024;

//   // Null masks
//   ARROW_ASSIGN_OR_RAISE(
//       auto null_masks,
//       AllocateResizableBuffer(size_null_masks(kInitialRowsCapacity), pool_));
//   null_masks_ = std::move(null_masks);
//   memset(null_masks_->mutable_data(), 0, size_null_masks(kInitialRowsCapacity));

//   // Offsets and rows
//   if (!metadata.is_fixed_length) {
//     ARROW_ASSIGN_OR_RAISE(
//         auto offsets, AllocateResizableBuffer(size_offsets(kInitialRowsCapacity),
//         pool_));
//     offsets_ = std::move(offsets);
//     memset(offsets_->mutable_data(), 0, size_offsets(kInitialRowsCapacity));
//     reinterpret_cast<int32_t*>(offsets_->mutable_data())[0] = 0;

//     ARROW_ASSIGN_OR_RAISE(
//         auto rows,
//         AllocateResizableBuffer(size_rows_varying_length(kInitialBytesCapacity),
//         pool_));
//     rows_ = std::move(rows);
//     memset(rows_->mutable_data(), 0, size_rows_varying_length(kInitialBytesCapacity));
//     bytes_capacity_ =
//         size_rows_varying_length(kInitialBytesCapacity) - kPaddingForVectors;
//   } else {
//     ARROW_ASSIGN_OR_RAISE(
//         auto rows,
//         AllocateResizableBuffer(size_rows_fixed_length(kInitialRowsCapacity), pool_));
//     rows_ = std::move(rows);
//     memset(rows_->mutable_data(), 0, size_rows_fixed_length(kInitialRowsCapacity));
//     bytes_capacity_ = size_rows_fixed_length(kInitialRowsCapacity) -
//     kPaddingForVectors;
//   }

//   UpdateBufferPointers();

//   rows_capacity_ = kInitialRowsCapacity;

//   num_rows_ = 0;
//   num_rows_for_has_any_nulls_ = 0;
//   has_any_nulls_ = false;

//   return Status::OK();
// }

// void RowBatchImpl::Clean() {
//   num_rows_ = 0;
//   num_rows_for_has_any_nulls_ = 0;
//   has_any_nulls_ = false;

//   if (!metadata_.is_fixed_length) {
//     reinterpret_cast<int32_t*>(offsets_->mutable_data())[0] = 0;
//   }
// }

// int64_t RowBatchImpl::size_null_masks(int64_t num_rows) const {
//   return num_rows * metadata_.null_masks_bytes_per_row + kPaddingForVectors;
// }

// int64_t RowBatchImpl::size_offsets(int64_t num_rows) const {
//   return (num_rows + 1) * sizeof(int32_t) + kPaddingForVectors;
// }

// int64_t RowBatchImpl::size_rows_fixed_length(int64_t num_rows) const {
//   return num_rows * metadata_.fixed_length + kPaddingForVectors;
// }

// int64_t RowBatchImpl::size_rows_varying_length(int64_t num_bytes) const {
//   return num_bytes + kPaddingForVectors;
// }

// void RowBatchImpl::UpdateBufferPointers() {
//   buffers_[0] = null_masks_->mutable_data();
//   if (metadata_.is_fixed_length) {
//     buffers_[1] = rows_->mutable_data();
//     buffers_[2] = nullptr;
//   } else {
//     buffers_[1] = offsets_->mutable_data();
//     buffers_[2] = rows_->mutable_data();
//   }
// }

// Status RowBatchImpl::ResizeFixedLengthBuffers(int64_t num_extra_rows) {
//   if (rows_capacity_ >= num_rows_ + num_extra_rows) {
//     return Status::OK();
//   }

//   int64_t rows_capacity_new = std::max(static_cast<int64_t>(1), 2 * rows_capacity_);
//   while (rows_capacity_new < num_rows_ + num_extra_rows) {
//     rows_capacity_new *= 2;
//   }

//   // Null masks
//   RETURN_NOT_OK(null_masks_->Resize(size_null_masks(rows_capacity_new), false));
//   memset(null_masks_->mutable_data() + size_null_masks(rows_capacity_), 0,
//          size_null_masks(rows_capacity_new) - size_null_masks(rows_capacity_));

//   // Either offsets or rows
//   if (!metadata_.is_fixed_length) {
//     RETURN_NOT_OK(offsets_->Resize(size_offsets(rows_capacity_new), false));
//     memset(offsets_->mutable_data() + size_offsets(rows_capacity_), 0,
//            size_offsets(rows_capacity_new) - size_offsets(rows_capacity_));
//   } else {
//     RETURN_NOT_OK(rows_->Resize(size_rows_fixed_length(rows_capacity_new), false));
//     memset(rows_->mutable_data() + size_rows_fixed_length(rows_capacity_), 0,
//            size_rows_fixed_length(rows_capacity_new) -
//                size_rows_fixed_length(rows_capacity_));
//     bytes_capacity_ = size_rows_fixed_length(rows_capacity_new) - kPaddingForVectors;
//   }

//   UpdateBufferPointers();

//   rows_capacity_ = rows_capacity_new;

//   return Status::OK();
// }

// Status RowBatchImpl::ResizeOptionalVaryingLengthBuffer(int64_t num_extra_bytes) {
//   int64_t num_bytes = offsets()[num_rows_];
//   if (bytes_capacity_ >= num_bytes + num_extra_bytes || metadata_.is_fixed_length) {
//     return Status::OK();
//   }

//   int64_t bytes_capacity_new = std::max(static_cast<int64_t>(1), 2 * bytes_capacity_);
//   while (bytes_capacity_new < num_bytes + num_extra_bytes) {
//     bytes_capacity_new *= 2;
//   }

//   RETURN_NOT_OK(rows_->Resize(size_rows_varying_length(bytes_capacity_new), false));
//   memset(rows_->mutable_data() + size_rows_varying_length(bytes_capacity_), 0,
//          size_rows_varying_length(bytes_capacity_new) -
//              size_rows_varying_length(bytes_capacity_));

//   UpdateBufferPointers();

//   bytes_capacity_ = bytes_capacity_new;

//   return Status::OK();
// }

// Status RowBatchImpl::AppendSelectionFrom(const RowBatchImpl& from,
//                                          int32_t num_rows_to_append,
//                                          const uint16_t* source_row_ids) {
//   DCHECK(metadata_.is_compatible(from.metadata()));

//   RETURN_NOT_OK(ResizeFixedLengthBuffers(num_rows_to_append));

//   if (!metadata_.is_fixed_length) {
//     // Varying-length rows
//     auto from_offsets = reinterpret_cast<const int32_t*>(from.offsets_->data());
//     auto to_offsets = reinterpret_cast<int32_t*>(offsets_->mutable_data());
//     int32_t total_length = to_offsets[num_rows_];
//     int32_t total_length_to_append = 0;
//     for (int32_t i = 0; i < num_rows_to_append; ++i) {
//       uint16_t row_id = source_row_ids ? source_row_ids[i] : i;
//       int32_t length = from_offsets[row_id + 1] - from_offsets[row_id];
//       total_length_to_append += length;
//       to_offsets[num_rows_ + i + 1] = total_length + total_length_to_append;
//     }

//     RETURN_NOT_OK(ResizeOptionalVaryingLengthBuffer(total_length_to_append));

//     const uint8_t* src = from.rows_->data();
//     uint8_t* dst = rows_->mutable_data() + total_length;
//     for (int32_t i = 0; i < num_rows_to_append; ++i) {
//       uint16_t row_id = source_row_ids ? source_row_ids[i] : i;
//       int32_t length = from_offsets[row_id + 1] - from_offsets[row_id];
//       auto src64 = reinterpret_cast<const uint64_t*>(src + from_offsets[row_id]);
//       auto dst64 = reinterpret_cast<uint64_t*>(dst);
//       for (int32_t j = 0; j < bit_util::CeilDiv(length, 8); ++j) {
//         dst64[j] = src64[j];
//       }
//       dst += length;
//     }
//   } else {
//     // Fixed-length rows
//     const uint8_t* src = from.rows_->data();
//     uint8_t* dst = rows_->mutable_data() + num_rows_ * metadata_.fixed_length;
//     for (int32_t i = 0; i < num_rows_to_append; ++i) {
//       uint16_t row_id = source_row_ids ? source_row_ids[i] : i;
//       int32_t length = metadata_.fixed_length;
//       auto src64 = reinterpret_cast<const uint64_t*>(src + length * row_id);
//       auto dst64 = reinterpret_cast<uint64_t*>(dst);
//       for (int32_t j = 0; j < bit_util::CeilDiv(length, 8); ++j) {
//         dst64[j] = src64[j];
//       }
//       dst += length;
//     }
//   }

//   // Null masks
//   int32_t byte_length = metadata_.null_masks_bytes_per_row;
//   uint64_t dst_byte_offset = num_rows_ * byte_length;
//   const uint8_t* src_base = from.null_masks_->data();
//   uint8_t* dst_base = null_masks_->mutable_data();
//   for (int32_t i = 0; i < num_rows_to_append; ++i) {
//     int32_t row_id = source_row_ids ? source_row_ids[i] : i;
//     int64_t src_byte_offset = row_id * byte_length;
//     const uint8_t* src = src_base + src_byte_offset;
//     uint8_t* dst = dst_base + dst_byte_offset;
//     for (int32_t ibyte = 0; ibyte < byte_length; ++ibyte) {
//       dst[ibyte] = src[ibyte];
//     }
//     dst_byte_offset += byte_length;
//   }

//   num_rows_ += num_rows_to_append;

//   return Status::OK();
// }

// Status RowBatchImpl::AppendEmpty(int32_t num_rows_to_append,
//                                  int32_t num_extra_bytes_to_append) {
//   RETURN_NOT_OK(ResizeFixedLengthBuffers(num_rows_to_append));
//   RETURN_NOT_OK(ResizeOptionalVaryingLengthBuffer(num_extra_bytes_to_append));
//   num_rows_ += num_rows_to_append;
//   if (metadata_.row_alignment > 1 || metadata_.string_alignment > 1) {
//     memset(rows_->mutable_data(), 0, bytes_capacity_);
//   }
//   return Status::OK();
// }

// bool RowBatchImpl::has_any_nulls(const LightContext* ctx) const {
//   if (has_any_nulls_) {
//     return true;
//   }
//   if (num_rows_for_has_any_nulls_ < num_rows_) {
//     auto size_per_row = metadata().null_masks_bytes_per_row;
//     has_any_nulls_ = !util::bit_util::are_all_bytes_zero(
//         ctx->hardware_flags, null_masks() + size_per_row * num_rows_for_has_any_nulls_,
//         static_cast<int32_t>(size_per_row * (num_rows_ -
//         num_rows_for_has_any_nulls_)));
//     num_rows_for_has_any_nulls_ = num_rows_;
//   }
//   return has_any_nulls_;
// }

}  // namespace quiver::row
