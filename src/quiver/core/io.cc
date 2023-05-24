// SPDX-License-Identifier: Apache-2.0

#include "quiver/core/io.h"

#include <span>

namespace quiver {

Sink FromFixedSizeSpan(std::span<uint8_t> span) {
  return Sink(span.data(), span.size(),
              [start = span.data(), len = static_cast<int32_t>(span.size())](uint8_t* buf, int32_t* remaining) {
                *remaining = len;
                return start;
              });
}

}  // namespace quiver