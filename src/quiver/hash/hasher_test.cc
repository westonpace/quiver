#include "quiver/hash/hasher.h"

#include <gtest/gtest.h>

#include "quiver/testutil/test_util.h"

namespace quiver::hash {

TEST(Hasher, Identity) { SchemaAndBatch data = TestBatch({Int8Array({1, 2, {}})});
  std::unique_ptr<Hasher> identity_hasher = Hasher::CreateIdentityHasher(1);

}

}  // namespace quiver::hash
