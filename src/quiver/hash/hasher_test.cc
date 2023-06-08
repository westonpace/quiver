#include "quiver/hash/hasher.h"

#include <gtest/gtest.h>

#include "quiver/testutil/test_util.h"
#include "quiver/util/bit_util.h"
#include "quiver/util/local_allocator_p.h"

namespace quiver::hash {

TEST(Hasher, Identity) {
  SchemaAndBatch data = TestBatch({Int8Array({1, 2, {}})});
  std::unique_ptr<Hasher> identity_hasher = Hasher::CreateIdentityHasher(1);

  util::LocalAllocator local_alloc;
  util::local_ptr<FlatArray> out_arr =
      local_alloc.AllocateFlatArray(1, data.batch->length());

  assert_ok(identity_hasher->HashBatch(data.batch.get(), *out_arr));
  ASSERT_TRUE(array::BinaryEquals(data.batch->array(0), array::ArrayView(*out_arr)));
}

}  // namespace quiver::hash
