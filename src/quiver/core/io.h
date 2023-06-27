// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <cstring>
#include <functional>
#include <span>

namespace quiver {

class StreamSink {
 public:
  StreamSink(uint8_t* initial_buf, int32_t initial_len,
             std::function<uint8_t*(uint8_t*, int32_t*)> swap);
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

class BufferSource {
 public:
  explicit BufferSource(uint8_t* buf) : buf_(buf) {}
  void CopyDataInto(uint8_t* dest, int64_t offset, int32_t len) {
    std::memcpy(dest, buf_ + offset, len);
  }

 private:
  uint8_t* buf_;
};

enum class RandomAccessSourceKind { kBuffer = 0, kFile = 1 };

class RandomAccessSource {
 public:
  explicit RandomAccessSource(RandomAccessSourceKind kind) : kind_(kind) {}
  virtual BufferSource AsBuffer() = 0;
  RandomAccessSourceKind kind() const { return kind_; }

  static std::unique_ptr<RandomAccessSource> FromSpan(std::span<uint8_t> span);

 private:
  RandomAccessSourceKind kind_;
};

}  // namespace quiver