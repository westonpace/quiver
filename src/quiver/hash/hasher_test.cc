#include "quiver/hash/hasher.h"

#include <gtest/gtest.h>

#include "quiver/testutil/test_util.h"
#include "quiver/util/bit_util.h"

namespace quiver::hash {

TEST(Hasher, Identity) { SchemaAndBatch data = TestBatch({Int8Array({1, 2, {}})});
  std::unique_ptr<Hasher> identity_hasher = Hasher::CreateIdentityHasher(1);
  std::unique_ptr<Batch> output =
      Batch::CreateInitializedBasic(&data.schema, 1024LL * 1024LL);

  output->SetLength(data.batch->length());
  output->ResizeBufferBytes(0, 0, bit_util::CeilDiv(data.batch->length(), 8LL));
  output->ResizeBufferBytes(0, 1, data.batch->length());

  assert_ok(identity_hasher->HashBatch(data.batch.get(), std::get<FlatArray>(output->mutable_array(0))));
  ASSERT_TRUE(array::BinaryEquals(data.batch->array(0), output->array(0)));
}

}  // namespace quiver::hash
