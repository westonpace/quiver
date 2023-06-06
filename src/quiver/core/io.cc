// SPDX-License-Identifier: Apache-2.0

#include "quiver/core/io.h"

#include <limits>
#include <span>

#include "quiver/util/logging_p.h"

namespace quiver {

StreamSink::StreamSink(uint8_t* initial_buf, int32_t initial_len,
                       std::function<uint8_t*(uint8_t*, int32_t*)> swap)
    : itr_(initial_buf), remaining_(initial_len), swap_(std::move(swap)) {}

void StreamSink::CopyInto(const uint8_t* src, int32_t len) {
  while (len > 0) {
    int32_t to_write = std::min(len, remaining_);
    len -= to_write;
    remaining_ -= to_write;
    std::memcpy(itr_, src, to_write);
    if (remaining_ == 0) {
      itr_ = swap_(itr_, &remaining_);
    } else {
      itr_ += to_write;
    }
  }
}

void StreamSink::CopyInto(uint8_t byte) {
  *itr_ = byte;
  itr_++;
  remaining_--;
  if (remaining_ == 0) {
    itr_ = swap_(itr_, &remaining_);
  }
}

StreamSink StreamSink::FromFixedSizeSpan(std::span<uint8_t> span) {
  QUIVER_CHECK_LE(span.size(), std::numeric_limits<int32_t>::max());
  return {span.data(), static_cast<int32_t>(span.size()),
          [start = span.data(), len = static_cast<int32_t>(span.size())](
              uint8_t*, int32_t* remaining) {
            *remaining = len;
            return start;
          }};
}

void RandomAccessSource::CopyFrom(uint8_t* dest, int32_t offset, int32_t len) {
  std::memcpy(dest, buf_ + offset, len);
}

RandomAccessSource RandomAccessSource::WrapSpan(std::span<uint8_t> span) {
  return RandomAccessSource(span.data());
}

RandomAccessSource::RandomAccessSource(uint8_t* buf) : buf_(buf) {}

}  // namespace quiver