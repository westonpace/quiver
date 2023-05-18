// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#pragma once

#include <cstdint>

namespace quiver::util {
class CpuInfo {
 public:
  enum class CacheLevel { kL1 = 0, kL2, kL3, kLast = kL3 };

  [[nodiscard]] virtual int64_t CacheSize(CacheLevel level) const = 0;

  [[nodiscard]] static const CpuInfo* GetInstance();
};

}  // namespace quiver::util
