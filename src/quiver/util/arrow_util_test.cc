#include "quiver/util/arrow_util.h"

#include <gtest/gtest.h>

#include <cstdint>

namespace quiver::util {

void MockRelease(ArrowArray* arr) {
  if (arr->private_data != nullptr) {
    auto* pdata = reinterpret_cast<int32_t*>(arr->private_data);
    *pdata = 42;
  }
}

TEST(OwnedArrowArray, Basic) {
  int foo = 7;
  {
    OwnedArrowArray arr = AllocateArrowArray();
    // These should be initialized to nullptr to avoid mistakes on delete if never used
    ASSERT_EQ(arr->release, nullptr);
    ASSERT_EQ(arr->private_data, nullptr);
    arr->release = MockRelease;

    arr->private_data = &foo;
    ASSERT_TRUE(static_cast<bool>(arr));

    arr.reset();
    ASSERT_EQ(foo, 42);
    foo = 7;
  }
  ASSERT_EQ(foo, 7);

  OwnedArrowArray arr2 = AllocateArrowArray();
  arr2.reset();
}

}  // namespace quiver::util
