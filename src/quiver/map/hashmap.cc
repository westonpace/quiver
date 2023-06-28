#include "quiver/map/hashmap.h"

#include <numeric>

#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/hash/hasher.h"
#include "quiver/row/row_p.h"
#include "quiver/util/local_allocator_p.h"

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
    DCHECK_EQ(key_batch_->schema(), key_schema_);
    DCHECK_EQ(payload_batch_->schema(), payload_schema_);
  }

  [[nodiscard]] ReadOnlyArray array(int32_t index) const override {
    if (index >= num_key_fields_) {
      return payload_batch_->array(index - num_key_fields_);
    }
    return key_batch_->array(index);
  }
  [[nodiscard]] const SimpleSchema* schema() const override { return combined_schema_; }
  [[nodiscard]] int64_t length() const override { return key_batch_->length(); }
  [[nodiscard]] int64_t num_bytes() const override {
    return key_batch_->num_bytes() + payload_batch_->num_bytes();
  };
  [[nodiscard]] int32_t num_buffers() const override {
    return key_batch_->num_buffers() + payload_batch_->num_buffers();
  }
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
  HashMapImpl(const SimpleSchema* key_schema, const SimpleSchema* payload_schema,
              std::unique_ptr<hash::Hasher> hasher,
              std::unique_ptr<hashtable::HashTable> hashtable)
      : key_schema_(key_schema),
        payload_schema_(payload_schema),
        combined_schema_(SimpleSchema::AllColumnsFrom(*key_schema_, *payload_schema_)),
        hasher_(std::move(hasher)),
        hashtable_(std::move(hashtable)) {}

  Status Init(StreamSink* sink, RandomAccessSource* source) {
    QUIVER_RETURN_NOT_OK(
        row::RowEncoder::Create(&combined_schema_, sink, false, &row_encoder_));
    return row::RowDecoder::Create(&combined_schema_, source, &row_decoder_);
  }

  Status Insert([[maybe_unused]] ReadOnlyBatch* keys,
                [[maybe_unused]] ReadOnlyBatch* payload) override {
    util::local_ptr<std::span<int64_t>> hashes =
        local_alloc_.AllocateSpan<int64_t>(keys->length());
    QUIVER_RETURN_NOT_OK(hasher_->HashBatch(keys, *hashes));

    KeyPayloadBatch key_payload(key_schema_, payload_schema_, &combined_schema_, keys,
                                payload);

    int64_t row_id_start;
    QUIVER_RETURN_NOT_OK(row_encoder_->Append(key_payload, &row_id_start));

    util::local_ptr<std::span<int64_t>> row_ids =
        local_alloc_.AllocateSpan<int64_t>(keys->length());
    std::iota(row_ids->begin(), row_ids->end(), row_id_start);

    hashtable_->Encode(*hashes, *row_ids);

    return Status::OK();
  }

  Status Lookup([[maybe_unused]] ReadOnlyBatch* keys,
                [[maybe_unused]] Batch* out) override {
    util::local_ptr<std::span<int64_t>> hashes =
        local_alloc_.AllocateSpan<int64_t>(keys->length());
    QUIVER_RETURN_NOT_OK(hasher_->HashBatch(keys, *hashes));

    util::local_ptr<std::span<int64_t>> row_ids =
        local_alloc_.AllocateSpan<int64_t>(keys->length());
    int64_t bucket_offset = 0;
    int64_t hash_offset = 0;
    int64_t length_out = 0;
    bool all_found =
        hashtable_->Decode(*hashes, *row_ids, &length_out, &hash_offset, &bucket_offset);
    if (!all_found) {
      return Status::NotImplemented("Hashtable paging");
    }

    std::span<int64_t> relevent_row_ids = row_ids->subspan(0, length_out);
    return row_decoder_->Load(relevent_row_ids, out);
  }

 private:
  const SimpleSchema* key_schema_;
  const SimpleSchema* payload_schema_;
  const SimpleSchema combined_schema_;
  std::unique_ptr<hash::Hasher> hasher_;
  std::unique_ptr<row::RowEncoder> row_encoder_;
  std::unique_ptr<row::RowDecoder> row_decoder_;
  std::unique_ptr<hashtable::HashTable> hashtable_;
  util::LocalAllocator local_alloc_;
};

Status HashMap::Create(const SimpleSchema* key_schema, const SimpleSchema* payload_schema,
                       std::unique_ptr<hash::Hasher> hasher, StreamSink* sink,
                       RandomAccessSource* source,
                       std::unique_ptr<hashtable::HashTable> hashtable,
                       std::unique_ptr<HashMap>* out) {
  auto hash_map = std::make_unique<HashMapImpl>(key_schema, payload_schema,
                                                std::move(hasher), std::move(hashtable));
  QUIVER_RETURN_NOT_OK(hash_map->Init(sink, source));
  *out = std::move(hash_map);
  return Status::OK();
}

}  // namespace quiver::map
