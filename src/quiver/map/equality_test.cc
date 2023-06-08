#pragma once

#include "quiver/map/equality.h"

#include <gtest/gtest.h>

#include "quiver/testutil/test_util.h"

namespace quiver::map {

TEST(BinaryEquality, Basic) {
  SchemaAndBatch data =
      TestBatch({Int16Array({1, 2, {}, 1000, {}}), Int16Array({1, 100, 1000, 1000, {}})});
  SchemaAndBatch expected = TestBatch({BoolArray({true, false, false, true, true})});
  std::cout << "Expected: "
            << array::ToString(expected.batch->array(0),
                               expected.schema.top_level_types[0])
            << std::endl;

  std::unique_ptr<Batch> output =
    Batch::CreateInitializedBasic(&expected.schema, 1024LL * 1024LL);
  output->ResizeFixedParts(0, data.batch->length());
  FlatArray out_arr = std::get<FlatArray>(output->mutable_array(0));

  std::unique_ptr<EqualityComparer> comparer = EqualityComparer::MakeSimpleBinaryEqualityComparer();
  assert_ok(comparer->CompareEquality(data.batch->array(0), data.batch->array(1),
                                      out_arr.values));

  ASSERT_TRUE(array::BinaryEquals(expected.batch->array(0), array::ArrayView(out_arr)));
}

}  // namespace quiver::map