#include "quiver/hash/hasher.h"

#include "quiver/util/logging_p.h"

namespace quiver::hash {

namespace {

class IdentityHasherImpl : public Hasher {
 public:
  IdentityHasherImpl(int32_t hash_width_bytes) : hash_width_bytes_(hash_width_bytes) {}

  int32_t hash_width_bytes() const override { return hash_width_bytes_; }
  Status HashBatch(ReadOnlyBatch* batch, FlatArray out) override {
    if (batch->schema()->num_fields() != 1) {
      return Status::Invalid("The identity hasher only works for single-column batches");
    }
    ReadOnlyArray hash_arr = batch->array(0);
    const FieldDescriptor& hash_type = batch->schema()->top_level_types[0];
    if (hash_type.layout != LayoutKind::kFlat) {
      return Status::Invalid(
          "The identity hasher only works for batches with a single FLAT column");
    }
    ReadOnlyFlatArray hash_flat_arr = std::get<ReadOnlyFlatArray>(batch->array(0));
    if (hash_flat_arr.width_bytes != hash_width_bytes_) {
      return Status::Invalid(
          "The identity hasher was configured to expect a hash width of ",
          hash_width_bytes_, " bytes but was given a single flat column with a width of ",
          hash_flat_arr.width_bytes, " bytes");
    }
    if (out.length != batch->length()) {
      return Status::Invalid(
          "The identity hasher was given a batch of length ", batch->length(),
          " but the output hashes array was only configured with length ", out.length);
    }
    DCHECK_EQ(out.validity.size(), hash_flat_arr.validity.size());
    DCHECK_EQ(out.values.size(), hash_flat_arr.values.size());
    std::memcpy(out.validity.data(), hash_flat_arr.validity.data(),
                hash_flat_arr.validity.size());
    std::memcpy(out.values.data(), hash_flat_arr.values.data(),
                hash_flat_arr.values.size());
    return Status::OK();
  }

 private:
  int32_t hash_width_bytes_;
};

}  // namespace

std::unique_ptr<Hasher> Hasher::CreateIdentityHasher(int32_t hash_width_bytes) {
  return std::make_unique<IdentityHasherImpl>(hash_width_bytes);
}

}  // namespace quiver::hash
