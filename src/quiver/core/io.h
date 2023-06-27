// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <span>

#include "quiver/util/literals.h"

using namespace quiver::util::literals;

namespace quiver {

class StreamSink {
 public:
  StreamSink(uint8_t* initial_buf, int32_t initial_len,
             std::function<uint8_t*(uint8_t*, int32_t, int32_t*)> swap)
      : itr_(initial_buf), remaining_(initial_len), swap_(std::move(swap)) {}
  void CopyInto(const uint8_t* src, int32_t len) {
    while (len > 0) {
      int32_t to_write = std::min(len, remaining_);
      len -= to_write;
      remaining_ -= to_write;
      written_ += to_write;
      std::memcpy(itr_, src, to_write);
      if (remaining_ == 0) {
        itr_ = swap_(itr_, written_, &remaining_);
        written_ = 0;
      } else {
        itr_ += to_write;
      }
    }
  }

  void CopyInto(uint8_t byte) {
    *itr_ = byte;
    itr_++;
    remaining_--;
    if (remaining_ == 0) {
      itr_ = swap_(itr_, written_, &remaining_);
      written_ = 0;
    }
  }

  void Finish() { itr_ = swap_(itr_, written_, &remaining_); }

  // Creates a sink that writes to a buffer and wraps
  // if it ever needs to swap
  static StreamSink FromFixedSizeSpan(std::span<uint8_t> span);
  // Creates a sink that periodically flushes to a file
  static StreamSink FromFile(std::FILE* file, int32_t write_buffer_size = 16_Ki);

 private:
  uint8_t* itr_;
  int32_t remaining_;
  int32_t written_ = 0;
  std::function<uint8_t*(uint8_t*, int32_t, int32_t*)> swap_;
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

class FileSource {
 public:
  explicit FileSource(std::FILE* file) : file_(file) {}
  void CopyDataInto(uint8_t* dest, int64_t offset, int32_t len) {
    std::fseek(file_, offset, SEEK_SET);
    auto num_read = static_cast<int32_t>(std::fread(dest, 1, len, file_));
    assert(num_read == len);
  }

 private:
  std::FILE* file_;
};

enum class RandomAccessSourceKind { kBuffer = 0, kFile = 1 };

class RandomAccessSource {
 public:
  explicit RandomAccessSource(RandomAccessSourceKind kind) : kind_(kind) {}
  virtual BufferSource AsBuffer() = 0;
  virtual FileSource AsFile() = 0;
  [[nodiscard]] RandomAccessSourceKind kind() const { return kind_; }

  static std::unique_ptr<RandomAccessSource> FromSpan(std::span<uint8_t> span);
  static std::unique_ptr<RandomAccessSource> FromFile(std::FILE* file,
                                                      bool close_on_destruct);
  static std::unique_ptr<RandomAccessSource> FromPath(std::string_view path);

 private:
  RandomAccessSourceKind kind_;
};

}  // namespace quiver