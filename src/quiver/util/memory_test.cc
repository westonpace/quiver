// SPDX-License-Identifier: Apache-2.0

#include "quiver/util/memory.h"

#include <gtest/gtest.h>

#include <memory>

namespace quiver::util {

TEST(Memory, TotalMemory) {
#if defined(_WIN32) || defined(__APPLE__) || defined(__linux__)
  ASSERT_GT(GetTotalMemoryBytes(), 0);
#else
  ASSERT_EQ(GetTotalMemoryBytes(), 0);
#endif
}

}  // namespace quiver::util
