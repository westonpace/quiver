// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <span>

namespace quiver {

class StreamSink {
 public:

  StreamSink(uint8_t* initial_buf, int32_t initial_len, std::function<uint8_t*(uint8_t*, int32_t*)> swap);
  void CopyInto(const uint8_t* src, int32_t len);
  void CopyInto(uint8_t byte);

  // Creates a sink that writes to a buffer and wraps
  // if it ever needs to swap
  static StreamSink FromFixedSizeSpan(std::span<uint8_t> span);

 private:
  uint8_t* itr_;
  int32_t remaining_;
  std::function<uint8_t*(uint8_t*, int32_t*)> swap_;
};

class RandomAccessSource {
 public:
  void CopyFrom(uint8_t* dest, int32_t offset, int32_t len);
  static RandomAccessSource WrapSpan(std::span<uint8_t> span);

 private:
  RandomAccessSource(uint8_t* buf);
  uint8_t* buf_;
};

}  // namespace quiver