#include "quiver/sort/sort.h"

#include <gtest/gtest.h>

#include "quiver/core/array.h"
#include "quiver/testutil/test_util.h"

namespace quiver::sort {

// Given two batches of the same data (but in different orders) verify they
// normalize to the same thing
void CheckNormalizer(const SchemaAndBatch& left, const SchemaAndBatch& right) {
  std::unique_ptr<Storage> storage = TestStorage();

  std::unique_ptr<AllColumnsBinarySorter> sorter;
  AssertOk(AllColumnsBinarySorter::Create(storage.get(), &sorter));

  std::unique_ptr<Batch> left_sorted = Batch::CreateBasic(left.schema.get());
  AssertOk(sorter->Sort(*left.batch, left_sorted.get()));

  std::unique_ptr<Batch> right_sorted = Batch::CreateBasic(right.schema.get());
  AssertOk(sorter->Sort(*right.batch, right_sorted.get()));

  ASSERT_TRUE(left_sorted->BinaryEquals(*right_sorted));
}

TEST(AllColumnsBinarySorter, Sort) {
  SchemaAndBatch testdata =
      TestBatch({Int64Array({1, 2, 0, 1000, 0}), Int16Array({1, 100, 1000, 57, 500})});

  CheckNormalizer(testdata, TestBatch({Int64Array({0, 0, 1, 2, 1000}),
                                       Int16Array({500, 1000, 1, 100, 57})}));
  CheckNormalizer(testdata, TestBatch({Int64Array({0, 1000, 0, 2, 1}),
                                       Int16Array({500, 57, 1000, 100, 1})}));
  CheckNormalizer(testdata, TestBatch({Int64Array({1, 0, 2, 0, 1000}),
                                       Int16Array({1, 500, 100, 1000, 57})}));
}

}  // namespace quiver::sort
