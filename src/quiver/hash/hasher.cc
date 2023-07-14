#include "quiver/hash/hasher.h"

#include <cstring>

#include "quiver/core/array.h"
#include "quiver/util/logging_p.h"
#include "quiver/util/tracing.h"

namespace quiver::hash {

namespace {

class IdentityHasherImpl : public Hasher {
 public:
  IdentityHasherImpl() {
    util::Tracer::RegisterCategory(util::tracecat::kHasherHash, "Hasher::Hash");
  }

  Status HashBatch(ReadOnlyBatch* batch, std::span<int64_t> out) override {
    auto scope = util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kHasherHash);
    if (batch->schema()->num_fields() == 0) {
      return Status::Invalid(
          "The identity hasher cannot hash a batch with no columns.  The first column "
          "must be the hashes.");
    }
    const FieldDescriptor& hash_type = batch->schema()->top_level_types[0];
    if (hash_type.layout != LayoutKind::kFlat) {
      return Status::Invalid(
          "The identity hasher only works for batches with a FLAT hashes column");
    }
    ReadOnlyFlatArray hash_flat_arr = std::get<ReadOnlyFlatArray>(batch->array(0));
    if (static_cast<int64_t>(out.size()) != batch->length()) {
      return Status::Invalid(
          "The identity hasher was given a batch of length ", batch->length(),
          " but the output hashes span was only configured with size ", out.size());
    }
    if (hash_flat_arr.width_bytes != sizeof(int64_t)) {
      return Status::Invalid(
          "The identity hasher only works for if the first (hashes) column is a FLAT "
          "column with a width of",
          sizeof(int64_t), " bytes");
    }
    DCHECK(!array::HasNulls(hash_flat_arr));
    std::span<uint8_t> out_bytes{reinterpret_cast<uint8_t*>(out.data()),
                                 out.size_bytes()};
    // Copy the values
    std::memcpy(out_bytes.data(), hash_flat_arr.values.data(),
                hash_flat_arr.values.size());
    return Status::OK();
  }
};

}  // namespace

std::unique_ptr<Hasher> Hasher::CreateIdentityHasher() {
  return std::make_unique<IdentityHasherImpl>();
}

}  // namespace quiver::hash
