#include "quiver/map/hashmap.h"

#include <memory>
#include <numeric>
#include <thread>

#include "quiver/accumulator/accumulator.h"
#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/hash/hasher.h"
#include "quiver/row/row_p.h"
#include "quiver/util/bit_util.h"
#include "quiver/util/local_allocator_p.h"
#include "quiver/util/tracing.h"

namespace quiver::map {

class KeyPayloadBatch : public ReadOnlyBatch {
 public:
  KeyPayloadBatch(const SimpleSchema* key_schema, const SimpleSchema* payload_schema,
                  const SimpleSchema* combined_schema, ReadOnlyBatch* key_batch,
                  ReadOnlyBatch* payload_batch)
      : key_schema_(key_schema),
        payload_schema_(payload_schema),
        combined_schema_(combined_schema),
        key_batch_(key_batch),
        payload_batch_(payload_batch),
        num_key_fields_(key_schema_->num_fields()) {
    DCHECK_EQ(key_batch_->length(), payload_batch_->length());
    DCHECK_EQ(key_batch_->schema()->num_fields(), key_schema_->num_fields());
    DCHECK_EQ(key_batch_->schema()->num_types(), key_schema_->num_types());
    DCHECK_EQ(payload_batch_->schema()->num_fields(), payload_schema_->num_fields());
    DCHECK_EQ(payload_batch_->schema()->num_types(), payload_schema_->num_types());
  }

  [[nodiscard]] ReadOnlyArray array(int32_t index) const override {
    if (index >= num_key_fields_) {
      return payload_batch_->array(index - num_key_fields_);
    }
    return key_batch_->array(index);
  }
  [[nodiscard]] const SimpleSchema* schema() const override { return combined_schema_; }
  [[nodiscard]] int64_t length() const override { return key_batch_->length(); }
  Status ExportToArrow(ArrowArray* /*out*/) && override {
    return Status::NotImplemented("KeyPayloadBatch::ExportToArrow");
  }

 private:
  const SimpleSchema* key_schema_;
  const SimpleSchema* payload_schema_;
  const SimpleSchema* combined_schema_;
  ReadOnlyBatch* key_batch_;
  ReadOnlyBatch* payload_batch_;
  int num_key_fields_;
};

class HashMapImpl : public HashMap {
 public:
  HashMapImpl(const SimpleSchema* key_schema, const SimpleSchema* build_payload_schema,
              const SimpleSchema* probe_payload_schema,
              std::unique_ptr<hash::Hasher> hasher,
              std::unique_ptr<hashtable::HashTable> hashtable)
      : key_schema_(key_schema),
        build_payload_schema_(build_payload_schema),
        probe_payload_schema_(probe_payload_schema),
        build_schema_(SimpleSchema::AllColumnsFrom(*key_schema_, *build_payload_schema_)),
        probe_schema_(),
        joined_schema_(),
        hasher_(std::move(hasher)),
        hashtable_(std::move(hashtable)) {
    util::Tracer::RegisterCategory(util::tracecat::kHashMapInsert, "HashMapImpl::Insert");
    util::Tracer::RegisterCategory(util::tracecat::kHashMapLookup, "HashMapImpl::Lookup");
    util::Tracer::RegisterCategory(util::tracecat::kHashMapInnerJoin,
                                   "HashMapImpl::InnerJoin");
    if (probe_payload_schema_ != nullptr) {
      probe_schema_ = SimpleSchema::AllColumnsFrom(*key_schema_, *probe_payload_schema_);
      joined_schema_ =
          SimpleSchema::AllColumnsFrom(build_schema_, *probe_payload_schema_);
    }
    for (const auto& field : joined_schema_.top_level_types) {
      row_width_bytes_ += field.data_width_bytes;
    }
  }

  Status Init(Storage* storage) {
    QUIVER_RETURN_NOT_OK(
        row::RowEncoder::Create(&build_schema_, storage, false, &row_encoder_));
    return row::RowDecoder::Create(&build_schema_, storage, &row_decoder_);
  }

  Status Insert(ReadOnlyBatch* keys, ReadOnlyBatch* payload) override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kHashMapInsert);
    util::local_ptr<std::span<int64_t>> hashes =
        local_alloc_.AllocateSpan<int64_t>(keys->length());
    QUIVER_RETURN_NOT_OK(hasher_->HashBatch(keys, *hashes));

    KeyPayloadBatch key_payload(key_schema_, build_payload_schema_, &build_schema_, keys,
                                payload);

    int64_t row_id_start;
    QUIVER_RETURN_NOT_OK(row_encoder_->Append(key_payload, &row_id_start));

    util::local_ptr<std::span<int64_t>> row_ids =
        local_alloc_.AllocateSpan<int64_t>(keys->length());
    std::iota(row_ids->begin(), row_ids->end(), row_id_start);

    hashtable_->Encode(*hashes, *row_ids);

    return Status::OK();
  }

  Status InsertCombinedBatch(ReadOnlyBatch* batch) override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kHashMapInsert);
    std::vector<int32_t> key_indices(key_schema_->num_fields());
    std::iota(key_indices.begin(), key_indices.end(), 0);
    std::unique_ptr<ReadOnlyBatch> keys =
        batch->SelectView(std::move(key_indices), key_schema_);
    util::local_ptr<std::span<int64_t>> hashes =
        local_alloc_.AllocateSpan<int64_t>(keys->length());
    QUIVER_RETURN_NOT_OK(hasher_->HashBatch(keys.get(), *hashes));

    int64_t row_id_start;
    QUIVER_RETURN_NOT_OK(row_encoder_->Append(*batch, &row_id_start));

    util::local_ptr<std::span<int64_t>> row_ids =
        local_alloc_.AllocateSpan<int64_t>(keys->length());
    std::iota(row_ids->begin(), row_ids->end(), row_id_start);

    hashtable_->Encode(*hashes, *row_ids);

    return Status::OK();
  }

  Status Lookup(ReadOnlyBatch* keys, Batch* out) override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kHashMapLookup);
    util::local_ptr<std::span<int64_t>> hashes =
        local_alloc_.AllocateSpan<int64_t>(keys->length());
    QUIVER_RETURN_NOT_OK(hasher_->HashBatch(keys, *hashes));

    util::local_ptr<std::span<int64_t>> out_row_ids =
        local_alloc_.AllocateSpan<int64_t>(keys->length());
    util::local_ptr<std::span<int32_t>> in_row_ids =
        local_alloc_.AllocateSpan<int32_t>(keys->length());
    int64_t bucket_offset = 0;
    int64_t hash_offset = 0;
    int64_t length_out = 0;
    bool all_found = hashtable_->Decode(*hashes, *in_row_ids, *out_row_ids, &length_out,
                                        &hash_offset, &bucket_offset);
    if (!all_found) {
      return Status::NotImplemented("Hashtable paging");
    }

    std::span<int64_t> relevent_row_ids = out_row_ids->subspan(0, length_out);
    return row_decoder_->Load(relevent_row_ids, out);
  }

  struct CombinedAccumulator {
    std::unique_ptr<accum::Accumulator> build;
    std::unique_ptr<accum::Accumulator> probe_payload;
    std::unique_ptr<ReadOnlyBatch> staged_build;

    CombinedAccumulator(int64_t rows_per_batch, const SimpleSchema* build_schema,
                        const SimpleSchema* probe_payload_schema,
                        const SimpleSchema* join_schema,
                        std::function<Status(std::unique_ptr<ReadOnlyBatch>)> consumer) {
      build = accum::Accumulator::FixedMemory(build_schema, rows_per_batch,
                                              [&](std::unique_ptr<ReadOnlyBatch> batch) {
                                                DCHECK_EQ(staged_build, nullptr);
                                                staged_build = std::move(batch);
                                                return Status::OK();
                                              });
      probe_payload = accum::Accumulator::FixedMemory(
          probe_payload_schema, rows_per_batch,
          [this, join_schema,
           consumer = std::move(consumer)](std::unique_ptr<ReadOnlyBatch> batch) {
            DCHECK_NE(staged_build, nullptr);
            auto* build_as_batch = static_cast<Batch*>(staged_build.get());
            auto* probe_as_batch = static_cast<Batch*>(batch.get());
            QUIVER_RETURN_NOT_OK(
                build_as_batch->Combine(std::move(*probe_as_batch), join_schema));
            QUIVER_RETURN_NOT_OK(consumer(std::move(staged_build)));
            staged_build.reset();
            return Status::OK();
          });
    }
  };

  Status InnerJoin(
      ReadOnlyBatch* keys, ReadOnlyBatch* payload, int32_t rows_per_batch,
      std::function<Status(std::unique_ptr<ReadOnlyBatch>)> consumer) override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kHashMapInnerJoin);
    if (probe_payload_schema_ == nullptr) {
      return Status::Invalid(
          "Probe schema was not provided at construction.  InnerJoin cannot be used.");
    }
    util::local_ptr<std::span<int64_t>> hashes =
        local_alloc_.AllocateSpan<int64_t>(keys->length());
    QUIVER_RETURN_NOT_OK(hasher_->HashBatch(keys, *hashes));

    constexpr int64_t kDefaultBatchSizeBytes = 16_MiLL;
    if (rows_per_batch < 0) {
      rows_per_batch = static_cast<int32_t>(
          bit_util::FloorDiv(kDefaultBatchSizeBytes, row_width_bytes_));
      if (rows_per_batch < 2_KiLL) {
        rows_per_batch = 2_KiLL;
      }
    }

    constexpr int64_t kMiniBatchSize = 1024;

    std::unique_ptr<Batch> scratch = Batch::CreateBasic(&build_schema_);
    util::local_ptr<std::span<int64_t>> out_row_ids =
        local_alloc_.AllocateSpan<int64_t>(kMiniBatchSize);
    util::local_ptr<std::span<int32_t>> in_row_ids =
        local_alloc_.AllocateSpan<int32_t>(kMiniBatchSize);
    CombinedAccumulator accumulator(rows_per_batch, &build_schema_, probe_payload_schema_,
                                    &joined_schema_, std::move(consumer));
    int64_t bucket_offset = 0;
    int64_t hash_offset = 0;
    int64_t length_out = 0;
    bool all_found = false;
    while (!all_found) {
      all_found = hashtable_->Decode(*hashes, *in_row_ids, *out_row_ids, &length_out,
                                     &hash_offset, &bucket_offset);

      std::span<int64_t> key_row_ids = out_row_ids->subspan(0, length_out);
      QUIVER_RETURN_NOT_OK(row_decoder_->Load(key_row_ids, scratch.get()));
      QUIVER_RETURN_NOT_OK(accumulator.build->InsertRange(scratch.get()));

      std::span<int32_t> payload_row_ids = in_row_ids->subspan(0, length_out);
      QUIVER_RETURN_NOT_OK(
          accumulator.probe_payload->InsertIndexed(payload, payload_row_ids));
    }

    QUIVER_RETURN_NOT_OK(accumulator.build->Finish());
    QUIVER_RETURN_NOT_OK(accumulator.probe_payload->Finish());

    return Status::OK();
  }

 private:
  const SimpleSchema* key_schema_;
  const SimpleSchema* build_payload_schema_;
  const SimpleSchema* probe_payload_schema_;
  const SimpleSchema build_schema_;
  SimpleSchema probe_schema_;
  SimpleSchema joined_schema_;
  std::unique_ptr<hash::Hasher> hasher_;
  std::unique_ptr<row::RowEncoder> row_encoder_;
  std::unique_ptr<row::RowDecoder> row_decoder_;
  std::unique_ptr<hashtable::HashTable> hashtable_;
  util::LocalAllocator local_alloc_;
  int64_t row_width_bytes_ = 0;
};

Status HashMap::Create(const SimpleSchema* key_schema,
                       const SimpleSchema* build_payload_schema,
                       const SimpleSchema* probe_payload_schema,
                       std::unique_ptr<hash::Hasher> hasher, Storage* storage,
                       std::unique_ptr<hashtable::HashTable> hashtable,
                       std::unique_ptr<HashMap>* out) {
  auto hash_map = std::make_unique<HashMapImpl>(key_schema, build_payload_schema,
                                                probe_payload_schema, std::move(hasher),
                                                std::move(hashtable));
  QUIVER_RETURN_NOT_OK(hash_map->Init(storage));
  *out = std::move(hash_map);
  return Status::OK();
}

}  // namespace quiver::map
