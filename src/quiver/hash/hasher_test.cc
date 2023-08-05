#include "quiver/hash/hasher.h"

#include <gtest/gtest.h>

#include "quiver/core/array.h"
#include "quiver/testutil/test_util.h"
#include "quiver/util/bit_util.h"
#include "quiver/util/local_allocator_p.h"

namespace quiver::hash {

TEST(Hasher, Identity) {
  util::LocalAllocator local_alloc;

  SchemaAndBatch data = TestBatch({Int64Array({1, 2, 5})});
  std::unique_ptr<Hasher> identity_hasher = Hasher::CreateIdentityHasher();

  util::local_ptr<std::span<int64_t>> hashes =
      local_alloc.AllocateSpan<int64_t>(data.batch->length());

  AssertOk(identity_hasher->HashBatch(data.batch.get(), *hashes));

  std::span<const uint8_t> hash_bytes{reinterpret_cast<const uint8_t*>(hashes->data()),
                                      hashes->size_bytes()};

  ReadOnlyFlatArray expected_hashes = std::get<ReadOnlyFlatArray>(data.batch->array(0));

  ASSERT_TRUE(buffer::BinaryEquals(hash_bytes, expected_hashes.values));
}

}  // namespace quiver::hash
