#include "quiver/map/equality.h"

#include <gtest/gtest.h>

#include "quiver/core/array.h"
#include "quiver/testutil/test_util.h"
#include "quiver/util/local_allocator_p.h"

namespace quiver::map {

TEST(BinaryEquality, Basic) {
  SchemaAndBatch data =
      TestBatch({Int16Array({1, 2, {}, 1000, {}}), Int16Array({1, 100, 1000, 1000, {}})});
  SchemaAndBatch expected = TestBatch({BoolArray({true, false, false, true, true})});

  util::LocalAllocator local_alloc;
  util::local_ptr<FlatArray> out_arr =
      local_alloc.AllocateFlatArray(0, data.batch->length(), /*allocate_validity=*/false);

  std::unique_ptr<EqualityComparer> comparer =
      EqualityComparer::MakeSimpleBinaryEqualityComparer();
  AssertOk(comparer->CompareEquality(data.batch->array(0), data.batch->array(1),
                                     out_arr->values));

  ASSERT_TRUE(array::BinaryEquals(expected.batch->array(0), array::ArrayView(*out_arr)));
}

}  // namespace quiver::map