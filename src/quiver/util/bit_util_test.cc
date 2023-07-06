// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "quiver/util/bitmap_ops.h"
#include "quiver/util/literals.h"

using namespace quiver::util::literals;

namespace quiver::util {

TEST(CopyBitmap, Basic) {
  BitsLiteral src = "00110011 00000000 00001111"_bits;
  BitsLiteral dst = "11110000 10101010 11001100"_bits;

  CopyBitmap(src.bytes.data(), 2, 8, dst.bytes.data(), 4);

  BitsLiteral res = "11111100 11001010 11001100"_bits;
  ASSERT_TRUE(BitmapEquals(dst.bytes.data(), 0, res.bytes.data(), 0, dst.num_bits));
}

}  // namespace quiver::util
