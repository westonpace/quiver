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

#include <linux/io_uring.h>
#include <spdlog/spdlog.h>
#include <sys/mman.h>

#include <atomic>
#include <iostream>
#include <memory>
#include <thread>

#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/util/bit_util.h"
#include "quiver/util/memory.h"
#include "quiver/util/tracing.h"
#include "quiver/util/variant_p.h"

/*
 * System call wrappers provided since glibc does not yet
 * provide wrappers for io_uring system calls.
 * */
int io_uring_setup(unsigned entries, struct io_uring_params* params) {
  return (int)syscall(__NR_io_uring_setup, entries, params);
}

int io_uring_enter(int ring_fd, unsigned int to_submit, unsigned int min_complete,
                   unsigned int flags) {
  return (int)syscall(__NR_io_uring_enter, ring_fd, to_submit, min_complete, flags, NULL,
                      0);
}

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
  int32_t row_padding = 0;

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
  row_padding = bit_util::PaddingNeededPwr2(fixed_length, row_alignment);
  fixed_length += row_padding;

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

  void EncodeValue(SinkBuffer* sink) {
    SPDLOG_TRACE("Encoding {} bytes value", width_);
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

class RowEncoderImpl : public RowEncoder {
 public:
  RowEncoderImpl(RowSchema schema, Storage* storage)
      : schema_(std::move(schema)), storage_(storage) {
    util::Tracer::RegisterCategory(util::tracecat::kRowEncoderEncode,
                                   "RowEncoder::Encode");
  }

  Status Initialize() {
    QUIVER_RETURN_NOT_OK(storage_->OpenRandomAccessSink(&sink_));
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

  Status Append(const ReadOnlyBatch& batch, int64_t* out_row_id) override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kRowEncoderEncode);
    *out_row_id = row_id_counter_;
    row_id_counter_ += batch.length();
    // Prepare
    for (std::size_t flat_idx = 0; flat_idx < flat_col_indices.size(); flat_idx++) {
      int32_t field_idx = flat_col_indices[flat_idx];
      flat_encoders[flat_idx].Prepare(batch.array(field_idx));
    }
    SinkBuffer sink_buffer;
    int64_t chunk_bytes = batch.length() * schema_.fixed_length;
    QUIVER_RETURN_NOT_OK(sink_->ReserveChunkAt(offset_bytes_, chunk_bytes, &sink_buffer));
    SPDLOG_TRACE("Appending batch of {} bytes to offset {}", chunk_bytes, offset_bytes_);
    offset_bytes_ += chunk_bytes;
    int64_t batch_length = batch.length();
    for (int64_t row_idx = 0; row_idx < batch_length; row_idx++) {
      // Encode values
      for (auto& flat_encoder : flat_encoders) {
        flat_encoder.EncodeValue(&sink_buffer);
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
          SPDLOG_TRACE("Encoding validity byte (full byte)");
          sink_buffer.CopyInto(validity_byte);
        }
      }
      if (bitmask != 1) {
        SPDLOG_TRACE("Encoding validity byte (remainder)");
        sink_buffer.CopyInto(validity_byte);
      }
      SPDLOG_TRACE("Padding {} padidng bytes", schema_.row_padding);
      sink_buffer.FillZero(schema_.row_padding);
    }
    return Status::OK();
  }

  Status Finish() override { return sink_->Finish(); }

  Status Reset() override {
    offset_bytes_ = 0;
    return Status::OK();
  }

 private:
  const RowSchema schema_;
  Storage* storage_;
  std::unique_ptr<RandomAccessSink> sink_;
  int64_t row_id_counter_ = 0;
  int64_t offset_bytes_ = 0;

  std::vector<int32_t> flat_col_indices;
  std::vector<FlatEncoder> flat_encoders;
  std::vector<FlatEncoder> flat_encoders_may_have_nulls;
};

Status RowEncoder::Create(const SimpleSchema* schema, Storage* storage, bool direct_io,
                          std::unique_ptr<RowEncoder>* out) {
  int32_t row_alignment = sizeof(int64_t);
  if (direct_io) {
    row_alignment = 512;
  }
  RowSchema row_schema(row_alignment, sizeof(int64_t));
  QUIVER_RETURN_NOT_OK(row_schema.Initialize(*schema));
  auto impl = std::make_unique<RowEncoderImpl>(std::move(row_schema), storage);
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

  template <typename BufferLikeSource>
  void DecodeValue(BufferLikeSource& src, int64_t src_offset) {
    src.CopyDataInto(values_itr_, src_offset, field_->data_width_bytes);
    values_itr_ += field_->data_width_bytes;
  }

  void DecodeStagedValue(const uint8_t* src) {
    std::memcpy(values_itr_, src, field_->data_width_bytes);
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

class RowDecoderImpl : public RowDecoder {
 public:
  RowDecoderImpl(RowSchema schema, Storage* storage)
      : schema_(std::move(schema)), storage_(storage) {
    util::Tracer::RegisterCategory(util::tracecat::kRowDecoderDecode,
                                   "RowDecoder::Decode");
  }

  Status Initialize() {
    QUIVER_RETURN_NOT_OK(storage_->OpenRandomAccessSource(&source_));
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

  template <typename BufferLikeSource>
  Status DoLoad(BufferLikeSource src, std::span<int64_t> indices, Batch* out) {
    out->SetLength(static_cast<int32_t>(indices.size()));
    for (auto& flat_decoder : flat_decoders_) {
      flat_decoder.Prepare(static_cast<int32_t>(indices.size()), out);
    }
    for (int64_t index : indices) {
      int64_t field_offset = static_cast<int64_t>(schema_.fixed_length) * index;
      for (auto& flat_decoder : flat_decoders_) {
        flat_decoder.DecodeValue(src, field_offset);
        field_offset += flat_decoder.fixed_width();
      }
      // Load validity bytes
      src.CopyDataInto(validity_scratch_.data(), field_offset,
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

  Status Load(std::span<int64_t> indices, Batch* out) override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kRowDecoderDecode);
    switch (source_->kind()) {
      case RandomAccessSourceKind::kBuffer:
        return DoLoad(source_->AsBuffer(), indices, out);
      case quiver::RandomAccessSourceKind::kFile:
        return DoLoad(source_->AsFile(), indices, out);
      default:
        DCHECK(false) << "NotYetImplemented";
        return Status::OK();
    }
  }

 private:
  RowSchema schema_;
  Storage* storage_;
  std::unique_ptr<RandomAccessSource> source_;

  std::vector<FlatDecoder> flat_decoders_;
  std::vector<uint8_t> validity_scratch_;
};

class StagedRowDecoderImpl : public RowDecoder {
 public:
  StagedRowDecoderImpl(RowSchema schema, Storage* storage)
      : schema_(std::move(schema)), storage_(storage) {
    util::Tracer::RegisterCategory(util::tracecat::kRowDecoderDecode,
                                   "RowDecoder::Decode");
  }

  Status Initialize() {
    QUIVER_RETURN_NOT_OK(storage_->OpenRandomAccessSource(&source_));
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

  template <typename BufferLikeSource>
  Status DoLoad(BufferLikeSource src, std::span<int64_t> indices, Batch* out) {
    std::vector<uint8_t> staged_space(schema_.fixed_length);
    out->SetLength(static_cast<int32_t>(indices.size()));
    for (auto& flat_decoder : flat_decoders_) {
      flat_decoder.Prepare(static_cast<int32_t>(indices.size()), out);
    }
    for (int64_t index : indices) {
      int64_t row_offset = static_cast<int64_t>(schema_.fixed_length) * index;
      src.CopyDataInto(staged_space.data(), row_offset, schema_.fixed_length);
      const uint8_t* itr = staged_space.data();
      for (auto& flat_decoder : flat_decoders_) {
        flat_decoder.DecodeStagedValue(itr);
        itr += flat_decoder.fixed_width();
      }
      std::memcpy(validity_scratch_.data(), itr,
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

  Status Load(std::span<int64_t> indices, Batch* out) override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kRowDecoderDecode);
    switch (source_->kind()) {
      case RandomAccessSourceKind::kBuffer:
        return DoLoad(source_->AsBuffer(), indices, out);
      case quiver::RandomAccessSourceKind::kFile:
        return DoLoad(source_->AsFile(), indices, out);
      default:
        DCHECK(false) << "NotYetImplemented";
        return Status::OK();
    }
  }

 private:
  RowSchema schema_;
  Storage* storage_;
  std::unique_ptr<RandomAccessSource> source_;

  std::vector<FlatDecoder> flat_decoders_;
  std::vector<uint8_t> validity_scratch_;
};

class IoUringSource {
 public:
  IoUringSource(int file_descriptor, int32_t queue_depth, bool direct_io)
      : file_descriptor_(file_descriptor),
        queue_depth_(queue_depth),
        direct_io_(direct_io) {}

  void Init() {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    ring_descriptor_ = io_uring_setup(queue_depth_, &params);
    DCHECK_GE(ring_descriptor_, 0) << "io_uring_setup failed";
    /*
     * io_uring communication happens via 2 shared kernel-user space ring
     * buffers, which can be jointly mapped with a single mmap() call in
     * kernels >= 5.4.
     */
    uint64_t sring_sz = params.sq_off.array + params.sq_entries * sizeof(unsigned);
    uint64_t cring_sz =
        params.cq_off.cqes + params.cq_entries * sizeof(struct io_uring_cqe);
    /* Rather than check for kernel version, the recommended way is to
     * check the features field of the io_uring_params structure, which is a
     * bitmask. If IORING_FEAT_SINGLE_MMAP is set, we can do away with the
     * second mmap() call to map in the completion ring separately.
     */
    if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0U) {
      if (cring_sz > sring_sz) {
        sring_sz = cring_sz;
      }
      cring_sz = sring_sz;
    }
    /* Map in the submission and completion queue ring buffers.
     *  Kernels < 5.4 only map in the submission queue, though.
     */
    sq_ptr_ = reinterpret_cast<std::byte*>(mmap(nullptr, sring_sz, PROT_READ | PROT_WRITE,
                                                MAP_SHARED | MAP_POPULATE,
                                                ring_descriptor_, IORING_OFF_SQ_RING));
    DCHECK_NE(sq_ptr_, MAP_FAILED) << "sq mmap failed";

    if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0U) {
      cq_ptr_ = sq_ptr_;
    } else {
      /* Map in the completion queue ring buffer in older kernels separately */
      cq_ptr_ = reinterpret_cast<std::byte*>(
          mmap(nullptr, cring_sz, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE,
               ring_descriptor_, IORING_OFF_CQ_RING));
      DCHECK_NE(cq_ptr_, MAP_FAILED) << "cq mmap failed";
    }
    /* Save useful fields for later easy reference */
    sring_tail_ = reinterpret_cast<std::atomic<uint32_t>*>(sq_ptr_ + params.sq_off.tail);
    sring_mask_ = reinterpret_cast<uint32_t*>(sq_ptr_ + params.sq_off.ring_mask);
    sring_array_ = reinterpret_cast<uint32_t*>(sq_ptr_ + params.sq_off.array);
    /* Map in the submission queue entries array */
    sqes_ = reinterpret_cast<io_uring_sqe*>(
        mmap(nullptr, params.sq_entries * sizeof(io_uring_sqe), PROT_READ | PROT_WRITE,
             MAP_SHARED | MAP_POPULATE, ring_descriptor_, IORING_OFF_SQES));
    DCHECK_NE(sqes_, MAP_FAILED) << "sqes mmap failed";
    /* Save useful fields for later easy reference */
    cring_head_ = reinterpret_cast<std::atomic<uint32_t>*>(cq_ptr_ + params.cq_off.head);
    cring_tail_ = reinterpret_cast<uint32_t*>(cq_ptr_ + params.cq_off.tail);
    cring_mask_ = reinterpret_cast<uint32_t*>(cq_ptr_ + params.cq_off.ring_mask);
    cqes_ = reinterpret_cast<io_uring_cqe*>(cq_ptr_ + params.cq_off.cqes);
  }

  void StartRead(int64_t offset, int32_t len,
                 uint8_t* buf,  // NOLINT(readability-non-const-parameter)
                 uint64_t user_data) {
    /* Add our submission queue entry to the tail of the SQE ring buffer */
    uint32_t tail = *sring_tail_;
    uint32_t index = tail & *sring_mask_;
    io_uring_sqe* sqe = &sqes_[index];
    std::memset(sqe, 0, sizeof(io_uring_sqe));
    /* Fill in the parameters required for the read or write operation */
    sqe->opcode = IORING_OP_READ;
    sqe->fd = file_descriptor_;
    sqe->addr = reinterpret_cast<decltype(io_uring_sqe::addr)>(buf);
    sqe->len = len;
    sqe->off = offset;
    sqe->user_data = user_data;
    // If direct_io_ validate that they are reading the file correctly
    DCHECK((!direct_io_) || (len % 512 == 0));
    DCHECK((!direct_io_) || (offset % 512 == 0));
    DCHECK((!direct_io_) || (sqe->addr % 512 == 0));
    sring_array_[index] = index;
    tail++;
    /* Update the tail */
    std::atomic_store_explicit(sring_tail_, tail, std::memory_order_release);
    int ret = io_uring_enter(ring_descriptor_, 1, 0, 0);
    DCHECK_EQ(ret, 1) << "io_uring_enter failed";
  }

  struct ReadResult {
    int32_t len;
    uint64_t user_data;
  };

  ReadResult FinishRead() {
    struct io_uring_cqe* cqe;
    /* Read barrier */
    uint32_t head;
    while (true) {
      head = std::atomic_load_explicit(cring_head_, std::memory_order_acquire);
      /*
       * Remember, this is a ring buffer. If head == tail, it means that the
       * buffer is empty.
       * */
      if (head != *cring_tail_) {
        break;
      }
      // std::cout << "Pause" << std::endl;
      std::this_thread::yield();
    }
    /* Get the entry */
    cqe = &cqes_[head & (*cring_mask_)];
    DCHECK_GE(cqe->res, 0) << "Error: " << strerror(abs(cqe->res));
    DCHECK_EQ(cqe->flags, 0) << "Unexpted flags: " << cqe->flags;
    ReadResult result = {cqe->res, cqe->user_data};
    head++;
    /* Write barrier so that update to the head are made visible */
    std::atomic_store_explicit(cring_head_, head, std::memory_order_release);
    return result;
  }

 private:
  int file_descriptor_;
  int ring_descriptor_;
  int32_t queue_depth_;
  bool direct_io_;
  std::byte* sq_ptr_;
  std::byte* cq_ptr_;
  std::atomic_uint32_t* sring_tail_;
  uint32_t* sring_mask_;
  uint32_t* sring_array_;
  uint32_t* cring_tail_;
  uint32_t* cring_mask_;
  std::atomic_uint32_t* cring_head_;
  io_uring_sqe* sqes_;
  io_uring_cqe* cqes_;
};

class IoUringDecoderImpl : public RowDecoder {
 public:
  constexpr static int64_t kMiniBatchSize = 32;
  constexpr static int64_t kIoUringDepth = kMiniBatchSize * 2;

  IoUringDecoderImpl(RowSchema schema, Storage* storage)
      : storage_(storage), schema_(std::move(schema)), scratch_offsets_(kIoUringDepth) {
    util::Tracer::RegisterCategory(util::tracecat::kRowDecoderDecode,
                                   "RowDecoder::Decode");
    scratch_space_.reserve(kIoUringDepth);
    for (int i = 0; i < kIoUringDepth; i++) {
      scratch_space_.push_back(util::AllocateAligned(schema.fixed_length, 512));
      uint8_t* val = *scratch_space_.back();
      DCHECK_EQ(reinterpret_cast<intptr_t>(val) % 512, 0);
      scratch_offsets_[i] = -1;
    }
  }

  Status Initialize() {
    QUIVER_RETURN_NOT_OK(storage_->OpenRandomAccessSource(&source_));
    file_source_ = source_->AsFile();
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

  void StartSomeReads(IoUringSource src, std::span<int64_t> indices, int64_t* offset) {
    int64_t remaining = static_cast<int64_t>(indices.size()) - *offset;
    int64_t to_read = std::min(kMiniBatchSize, remaining);

    for (int64_t i = 0; i < to_read; i++) {
      int64_t loop_offset = *offset + i;
      int64_t index = indices[loop_offset];
      int64_t scratch_index = loop_offset % kIoUringDepth;
      DCHECK_EQ(scratch_offsets_[scratch_index], -1);
      scratch_offsets_[scratch_index] = 0;
      int64_t row_offset = static_cast<int64_t>(schema_.fixed_length) * index;
      src.StartRead(row_offset, schema_.fixed_length, *scratch_space_[scratch_index],
                    scratch_index);
    }

    *offset += to_read;
  }

  void FinishSomeReads(IoUringSource src, int64_t total_size, int64_t* offset) {
    int64_t remaining = total_size - *offset;
    int64_t to_read = std::min(kMiniBatchSize, remaining);

    for (int64_t i = 0; i < to_read; i++) {
      int64_t loop_offset = *offset + i;
      int64_t waiting_index = loop_offset % kIoUringDepth;
      while (scratch_offsets_[waiting_index] != schema_.fixed_length) {
        IoUringSource::ReadResult read = src.FinishRead();
        uint32_t scratch_index = read.user_data;
        scratch_offsets_[scratch_index] += read.len;
      }
      StagedDecode(*scratch_space_[waiting_index]);
      scratch_offsets_[waiting_index] = -1;
    }

    *offset += to_read;
  }

  void StagedDecode(const uint8_t* data) {
    const uint8_t* itr = data;
    for (auto& flat_decoder : flat_decoders_) {
      flat_decoder.DecodeStagedValue(itr);
      itr += flat_decoder.fixed_width();
    }
    std::memcpy(validity_scratch_.data(), itr,
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

  Status DoLoad(IoUringSource src, std::span<int64_t> indices, Batch* out) {
    out->SetLength(static_cast<int32_t>(indices.size()));
    for (auto& flat_decoder : flat_decoders_) {
      flat_decoder.Prepare(static_cast<int32_t>(indices.size()), out);
    }
    int64_t start_offset = 0;
    int64_t read_offset = 0;
    StartSomeReads(src, indices, &start_offset);
    while (read_offset < static_cast<int64_t>(indices.size())) {
      StartSomeReads(src, indices, &start_offset);
      FinishSomeReads(src, static_cast<int64_t>(indices.size()), &read_offset);
    }
    return Status::OK();
  }

  Status Load(std::span<int64_t> indices, Batch* out) override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kRowDecoderDecode);
    DCHECK(file_source_.has_value()) << "Call to Load without Initialize";
    IoUringSource src(
        file_source_->file_descriptor(),  // NOLINT(bugprone-unchecked-optional-access)
        kIoUringDepth, storage_->requires_alignment());
    src.Init();
    return DoLoad(src, indices, out);
  }

 private:
  Storage* storage_;
  std::unique_ptr<RandomAccessSource> source_;
  std::optional<FileSource> file_source_;
  RowSchema schema_;

  std::vector<FlatDecoder> flat_decoders_;
  std::vector<uint8_t> validity_scratch_;
  std::vector<std::shared_ptr<uint8_t*>> scratch_space_;
  std::vector<int32_t> scratch_offsets_;
};

Status RowDecoder::Create(const SimpleSchema* schema, Storage* storage,
                          std::unique_ptr<RowDecoder>* out) {
  int32_t row_alignment = sizeof(int64_t);
  if (storage->requires_alignment()) {
    row_alignment = storage->page_size();
  }
  RowSchema row_schema(row_alignment, sizeof(int64_t));
  QUIVER_RETURN_NOT_OK(row_schema.Initialize(*schema));
  auto impl = std::make_unique<RowDecoderImpl>(std::move(row_schema), storage);
  QUIVER_RETURN_NOT_OK(impl->Initialize());
  *out = std::move(impl);
  return Status::OK();
}

Status RowDecoder::CreateStaged(const SimpleSchema* schema, Storage* storage,
                                std::unique_ptr<RowDecoder>* out) {
  int32_t row_alignment = sizeof(int64_t);
  if (storage->requires_alignment()) {
    row_alignment = storage->page_size();
  }
  RowSchema row_schema(row_alignment, sizeof(int64_t));
  QUIVER_RETURN_NOT_OK(row_schema.Initialize(*schema));
  auto impl = std::make_unique<StagedRowDecoderImpl>(std::move(row_schema), storage);
  QUIVER_RETURN_NOT_OK(impl->Initialize());
  *out = std::move(impl);
  return Status::OK();
}

Status RowDecoder::CreateIoUring(const SimpleSchema* schema, Storage* storage,
                                 std::unique_ptr<RowDecoder>* out) {
  int32_t row_alignment = sizeof(int64_t);
  if (storage->requires_alignment()) {
    row_alignment = storage->page_size();
  }
  RowSchema row_schema(row_alignment, sizeof(int64_t));
  QUIVER_RETURN_NOT_OK(row_schema.Initialize(*schema));
  auto impl = std::make_unique<IoUringDecoderImpl>(std::move(row_schema), storage);
  QUIVER_RETURN_NOT_OK(impl->Initialize());
  *out = std::move(impl);
  return Status::OK();
}

}  // namespace quiver::row
