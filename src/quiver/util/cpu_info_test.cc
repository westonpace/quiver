// SPDX-License-Identifier: Apache-2.0

#include "quiver/util/cpu_info.h"

#include <gtest/gtest.h>

#include <memory>

namespace quiver::util {

TEST(CpuInfo, Basic) {
  const CpuInfo* cpu_info = CpuInfo::GetInstance();

  constexpr int64_t kMinReasonableL1 = 4LL * 1024LL;
  constexpr int64_t kMaxReasonableL1 = 512LL * 1024LL;
  constexpr int64_t kMinReasonableL2 = 32LL * 1024LL;
  constexpr int64_t kMaxReasonableL2 = 8LL * 1024LL * 1024LL;
  constexpr int64_t kMinReasonableL3 = 256LL * 1024LL;
  constexpr int64_t kMaxReasonableL3 = 1024LL * 1024LL * 1024LL;

  const auto l1_size = cpu_info->CacheSize(CpuInfo::CacheLevel::kL1);
  const auto l2_size = cpu_info->CacheSize(CpuInfo::CacheLevel::kL2);
  const auto l3_size = cpu_info->CacheSize(CpuInfo::CacheLevel::kL3);
  ASSERT_TRUE(l1_size >= kMinReasonableL1 && l1_size <= kMaxReasonableL1)
      << "unexpected L1_SIZE size: " << l1_size;
  ASSERT_TRUE(l2_size >= kMinReasonableL2 && l2_size <= kMaxReasonableL2)
      << "unexpected L2_SIZE size: " << l2_size;
  ASSERT_TRUE(l3_size >= kMinReasonableL3 && l3_size <= kMaxReasonableL3)
      << "unexpected L3_SIZE size: " << l3_size;
  ASSERT_LE(l1_size, l2_size) << "L1_SIZE cache size " << l1_size
                              << " larger than L2_SIZE " << l2_size;
  ASSERT_LE(l2_size, l3_size) << "L2_SIZE cache size " << l2_size
                              << " larger than L3_SIZE " << l3_size;
}

}  // namespace quiver::util
