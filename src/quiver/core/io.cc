// SPDX-License-Identifier: Apache-2.0

#include "quiver/core/io.h"

#include <limits>
#include <span>

#include "quiver/util/logging_p.h"

namespace quiver {

Sink Sink::FromFixedSizeSpan(std::span<uint8_t> span) {
  QUIVER_CHECK_LE(span.size(), std::numeric_limits<int32_t>::max());
  return {span.data(), static_cast<int32_t>(span.size()),
          [start = span.data(), len = static_cast<int32_t>(span.size())](
              uint8_t*, int32_t* remaining) {
            *remaining = len;
            return start;
          }};
}

}  // namespace quiver