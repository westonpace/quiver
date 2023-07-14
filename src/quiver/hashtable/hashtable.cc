#include "quiver/hashtable/hashtable.h"

#include <memory>
#include <unordered_map>

#include "quiver/core/array.h"
#include "quiver/util/literals.h"
#include "quiver/util/logging_p.h"
#include "quiver/util/tracing.h"

namespace quiver::hashtable {

constexpr std::size_t kInitialBucketCount = util::kMi;

struct IdentityHash {
  std::size_t operator()(const int64_t& value) const { return value; }
};

class StlHashTable : public HashTable {
 public:
  StlHashTable() : map_(kInitialBucketCount) {
    util::Tracer::RegisterCategory(util::tracecat::kHashTableEncode, "HashTable::Encode");
    util::Tracer::RegisterCategory(util::tracecat::kHashTableDecode, "HashTable::Decode");
  }

  void Encode(std::span<const int64_t> hashes,
              std::span<const int64_t> row_ids) override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kHashTableEncode);
    DCHECK_EQ(hashes.size(), row_ids.size());
    auto hash_itr = hashes.begin();
    auto row_id_itr = row_ids.begin();
    while (hash_itr != hashes.end()) {
      map_.insert({*hash_itr, *row_id_itr});
      hash_itr++;
      row_id_itr++;
    }
  }

  bool Decode(std::span<const int64_t> hashes, std::span<int32_t> hash_idx_out,
              std::span<int64_t> row_ids_out, int64_t* length_out,
              int64_t* hash_idx_offset, int64_t* bucket_idx_offset) override {
    auto trace_scope =
        util::Tracer::GetCurrent()->ScopeActivity(util::tracecat::kHashTableDecode);
    DCHECK(!row_ids_out.empty());
    DCHECK_NE(hash_idx_offset, nullptr);
    DCHECK_NE(bucket_idx_offset, nullptr);
    DCHECK_EQ(hash_idx_out.size(), row_ids_out.size());

    auto hash_itr = hashes.begin() + *hash_idx_offset;
    auto hash_idx_itr = hash_idx_out.begin();
    auto out_itr = row_ids_out.begin();

    bool first = true;
    while (hash_itr != hashes.end()) {
      auto range = map_.equal_range(*hash_itr);
      auto bucket_itr = range.first;
      int64_t bucket_idx = 0;
      if (first) {
        first = false;
        for (int i = 0; i < *bucket_idx_offset; i++) {
          bucket_itr++;
          bucket_idx++;
        }
      }
      while (bucket_itr != range.second && out_itr != row_ids_out.end()) {
        *out_itr = static_cast<int64_t>(bucket_itr->second);
        *hash_idx_itr = static_cast<int32_t>(hash_itr - hashes.begin());
        out_itr++;
        hash_idx_itr++;
        bucket_itr++;
        bucket_idx++;
      }
      if (bucket_itr == range.second) {
        hash_itr++;
        bucket_idx = 0;
      }
      if (out_itr == row_ids_out.end() && hash_itr != hashes.end()) {
        *length_out = out_itr - row_ids_out.begin();
        *hash_idx_offset = hash_itr - hashes.begin();
        *bucket_idx_offset = bucket_idx;
        return false;
      }
    }
    *length_out = out_itr - row_ids_out.begin();
    return true;
  };

 private:
  std::unordered_multimap<int64_t, int64_t, IdentityHash> map_;
};

std::unique_ptr<HashTable> HashTable::MakeStl() {
  return std::make_unique<StlHashTable>();
}

}  // namespace quiver::hashtable
