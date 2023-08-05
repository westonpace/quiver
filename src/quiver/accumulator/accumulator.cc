#include "quiver/accumulator/accumulator.h"

#include <array>
#include <cstring>
#include <limits>

#include "quiver/core/array.h"
#include "quiver/util/bit_util.h"
#include "quiver/util/bitmap_ops.h"
#include "quiver/util/tracing.h"

namespace quiver::accum {

static constexpr std::array<uint8_t, 8> kLowerBitsMask = {0xFF, 0x7F, 0x3F, 0x1F,
                                                          0x0F, 0x07, 0x03, 0x01};
static constexpr std::array<uint8_t, 8> kUpperBitsMask = {0xFF, 0xFE, 0xFC, 0xF8,
                                                          0xF0, 0xE0, 0xC0, 0x80};

class FlatColumnAccumulator {
 public:
  FlatColumnAccumulator(int32_t col_index) : col_index_(col_index) {}

  void Init(Batch* batch) {
    current_array_ = std::get<FlatArray>(batch->mutable_array(col_index_));
    index_in_batch_ = 0;
  }

  void Insert(ReadOnlyBatch* batch, int64_t offset, int32_t length) {
    auto src = std::get<ReadOnlyFlatArray>(batch->array(col_index_));
    int64_t width_bytes = src.width_bytes;
    DCHECK_EQ(width_bytes, current_array_.width_bytes);

    uint8_t* values_dst = current_array_.values.data() + index_in_batch_ * width_bytes;
    const uint8_t* values_src = src.values.data() + (offset * width_bytes);
    std::memcpy(values_dst, values_src, width_bytes * length);

    uint8_t* validity_dst = current_array_.validity.data();
    const uint8_t* validity_src = src.validity.data();
    util::CopyBitmap(validity_src, offset, length, validity_dst, index_in_batch_);

    index_in_batch_ += static_cast<int32_t>(length);
  }

  template <typename T, typename IndexType>
  void InsertValues(ReadOnlyFlatArray src, std::span<const IndexType> indices) {
    const T* src_vals = reinterpret_cast<const T*>(src.values.data());
    T* dst_vals = reinterpret_cast<T*>(current_array_.values.data());
    for (const IndexType idx : indices) {
      *dst_vals = src_vals[idx];
      dst_vals++;
    }
  }

  template <typename IndexType>
  void InsertValuesMemcpy(ReadOnlyFlatArray src,
                          std::span<const IndexType> indices) {  // NOLINT
    int64_t width_bytes = src.width_bytes;
    const auto* src_vals = src.values.data();
    uint8_t* dst_vals = current_array_.values.data();
    for (const IndexType idx : indices) {
      std::memcpy(dst_vals, src_vals + idx * width_bytes, width_bytes);
      dst_vals += width_bytes;
    }
  }

  template <typename IndexType>
  void Insert(ReadOnlyBatch* batch, std::span<const IndexType> indices) {
    auto src = std::get<ReadOnlyFlatArray>(batch->array(col_index_));
    int64_t width_bytes = src.width_bytes;
    DCHECK_EQ(width_bytes, current_array_.width_bytes);
    switch (width_bytes) {
      case 1:
        InsertValues<uint8_t>(src, indices);
        break;
      case 2:
        InsertValues<uint16_t>(src, indices);
        break;
      case 4:
        InsertValues<uint32_t>(src, indices);
        break;
      case 8:
        InsertValues<uint64_t>(src, indices);
        break;
      default:
        InsertValuesMemcpy(src, indices);
    }
    uint8_t* validity_dst = current_array_.validity.data();
    const uint8_t* validity_src = src.validity.data();
    if (validity_src == nullptr) {
      // TODO: Technically this could mean that everything is null.  We should handle
      // that case too
      bit_util::SetBitmap(validity_dst, index_in_batch_, src.length);
    } else {
      util::IndexedCopyBitmap(validity_src, indices, validity_dst, index_in_batch_);
    }
    index_in_batch_ += static_cast<int32_t>(indices.size());
  }

 private:
  const int32_t col_index_;
  int32_t index_in_batch_ = 0;
  FlatArray current_array_;
};

class FixedMemoryAccumulator : public Accumulator {
 public:
  FixedMemoryAccumulator(const SimpleSchema* schema, int32_t rows_per_batch,
                         std::function<Status(std::unique_ptr<ReadOnlyBatch>)> emit)
      : schema_(schema), rows_per_batch_(rows_per_batch), emit_(std::move(emit)) {
    util::Tracer::RegisterCategory(util::tracecat::kAccumulatorInsert,
                                   "Accumulator::Insert");
    util::Tracer::RegisterCategory(util::tracecat::kAccumulatorFinish,
                                   "Accumulator::Finish");
    DCHECK_LE(rows_per_batch_, static_cast<int64_t>(std::numeric_limits<int32_t>::max()));
    column_accumulators_.reserve(schema->num_fields());
    for (int i = 0; i < schema->num_fields(); i++) {
      const FieldDescriptor& field = schema->field(i);
      DCHECK_EQ(field.layout, LayoutKind::kFlat)
          << "Accumulator not yet implemented for non-flat";
      column_accumulators_.emplace_back(i);
    }
  }

  void Reset() {
    current_batch_ = Batch::CreateBasic(schema_);
    index_in_batch_ = 0;
    current_batch_->SetLength(rows_per_batch_);
    for (int i = 0; i < schema_->num_fields(); i++) {
      current_batch_->ResizeFixedParts(i, rows_per_batch_);
      column_accumulators_[i].Init(current_batch_.get());
    }
  }

  Status EmitAndReset() {
    QUIVER_RETURN_NOT_OK(emit_(std::move(current_batch_)));
    Reset();
    return Status::OK();
  }

  Status InsertBatch(ReadOnlyBatch* batch) override { return InsertRange(batch); }
  Status InsertRange(ReadOnlyBatch* batch, int64_t row_start = 0,
                     int64_t length = -1) override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kAccumulatorInsert);
    int64_t offset = row_start;
    int64_t remaining = (length < 0) ? batch->length() - row_start : length;
    while (remaining > 0) {
      int32_t available = rows_per_batch_ - index_in_batch_;
      int32_t iter_length;
      if (remaining > available) {
        iter_length = available;
      } else {
        iter_length = static_cast<int32_t>(remaining);
      }
      for (int i = 0; i < schema_->num_fields(); i++) {
        column_accumulators_[i].Insert(batch, offset, iter_length);
      }
      offset += iter_length;
      index_in_batch_ += iter_length;
      remaining -= iter_length;
      if (index_in_batch_ == rows_per_batch_) {
        QUIVER_RETURN_NOT_OK(EmitAndReset());
      }
    }
    return Status::OK();
  }

  template <typename IndexType>
  Status InsertIndexedHelper(ReadOnlyBatch* batch, std::span<const IndexType> indices) {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kAccumulatorInsert);
    int64_t offset = 0;
    auto remaining = static_cast<int64_t>(indices.size());
    while (remaining > 0) {
      int32_t available = rows_per_batch_ - index_in_batch_;
      int32_t iter_length;
      if (remaining > available) {
        iter_length = available;
      } else {
        iter_length = static_cast<int32_t>(remaining);
      }
      std::span<const IndexType> iter_indices = indices.subspan(offset, iter_length);
      for (int i = 0; i < schema_->num_fields(); i++) {
        column_accumulators_[i].Insert(batch, iter_indices);
      }
      offset += iter_length;
      index_in_batch_ += iter_length;
      remaining -= iter_length;
      if (index_in_batch_ == rows_per_batch_) {
        QUIVER_RETURN_NOT_OK(EmitAndReset());
      }
    }
    return Status::OK();
  }

  Status InsertIndexed(ReadOnlyBatch* batch, std::span<const int32_t> indices) override {
    return InsertIndexedHelper(batch, indices);
  }

  Status InsertIndexed(ReadOnlyBatch* batch, std::span<const int64_t> indices) override {
    return InsertIndexedHelper(batch, indices);
  }

  Status Finish() override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kAccumulatorFinish);
    if (current_batch_ && index_in_batch_ > 0) {
      current_batch_->SetLength(index_in_batch_);
      for (int i = 0; i < schema_->num_fields(); i++) {
        current_batch_->ResizeFixedParts(i, index_in_batch_);
      }
      QUIVER_RETURN_NOT_OK(emit_(std::move(current_batch_)));
      current_batch_.reset();
    }
    return Status::OK();
  }

 private:
  const SimpleSchema* schema_;
  const int32_t rows_per_batch_;
  std::vector<FlatColumnAccumulator> column_accumulators_;

  std::unique_ptr<Batch> current_batch_;
  std::function<Status(std::unique_ptr<ReadOnlyBatch>)> emit_;
  int32_t index_in_batch_ = 0;
};

std::unique_ptr<Accumulator> Accumulator::FixedMemory(
    const SimpleSchema* schema, int64_t rows_per_batch,
    std::function<Status(std::unique_ptr<ReadOnlyBatch>)> emit) {
  std::unique_ptr<FixedMemoryAccumulator> accum =
      std::make_unique<FixedMemoryAccumulator>(schema, rows_per_batch, std::move(emit));
  accum->Reset();
  return accum;
}

}  // namespace quiver::accum
