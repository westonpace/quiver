// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <fmt/core.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <span>

#include "quiver/core/array.h"
#include "quiver/util/literals.h"
#include "quiver/util/uri.h"

using namespace quiver::util::literals;

namespace quiver {

class StreamSink {
 public:
  StreamSink(uint8_t* initial_buf, int32_t initial_len,
             std::function<uint8_t*(uint8_t*, int32_t, int32_t*)> swap)
      : buf_(initial_buf),
        itr_(initial_buf),
        remaining_(initial_len),
        swap_(std::move(swap)) {}

  StreamSink(const StreamSink& other) = delete;
  StreamSink& operator=(const StreamSink& other) = delete;
  StreamSink(StreamSink&& other) = default;
  StreamSink& operator=(StreamSink&& other) = default;

  void CopyInto(const uint8_t* src, int32_t len) {
    while (len > 0) {
      int32_t to_write = std::min(len, remaining_);
      len -= to_write;
      remaining_ -= to_write;
      written_ += to_write;
      std::memcpy(itr_, src, to_write);
      if (remaining_ == 0) {
        itr_ = swap_(buf_, written_, &remaining_);
        buf_ = itr_;
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
    written_++;
    if (remaining_ == 0) {
      itr_ = swap_(buf_, written_, &remaining_);
      buf_ = itr_;
      written_ = 0;
    }
  }

  void FillZero(int32_t len) {
    while (len > 0) {
      int32_t to_write = std::min(len, remaining_);
      len -= to_write;
      remaining_ -= to_write;
      written_ += to_write;
      std::memset(itr_, 0, to_write);
      if (remaining_ == 0) {
        itr_ = swap_(buf_, written_, &remaining_);
        buf_ = itr_;
        written_ = 0;
      } else {
        itr_ += to_write;
      }
    }
  }

  void Finish() {
    itr_ = swap_(buf_, written_, &remaining_);
    buf_ = itr_;
    written_ = 0;
  }

  // Creates a sink that writes to a buffer and wraps
  // if it ever needs to swap
  static StreamSink FromFixedSizeSpan(std::span<uint8_t> span);
  // Creates a sink that periodically flushes to a file
  static StreamSink FromPath(const std::string& path, bool direct_io, bool append,
                             int32_t write_buffer_size = 16_Ki);

 private:
  uint8_t* buf_;
  uint8_t* itr_;
  int32_t remaining_;
  int32_t written_ = 0;
  std::function<uint8_t*(uint8_t*, int32_t, int32_t*)> swap_;
};

class RandomAccessSink;
class SinkBuffer {
 public:
  SinkBuffer() = default;
  SinkBuffer(std::span<uint8_t> buf, int64_t offset, RandomAccessSink* sink)
      : buf_(buf),
        offset_(offset),
        itr_(buf.data()),
        remaining_(static_cast<int64_t>(buf_.size())),
        sink_(sink) {}
  SinkBuffer(const SinkBuffer&) = delete;
  SinkBuffer& operator=(const SinkBuffer&) = delete;
  SinkBuffer(SinkBuffer&&) noexcept;
  SinkBuffer& operator=(SinkBuffer&&) noexcept;

  ~SinkBuffer() noexcept;

#ifndef NDEBUG
  void UpdateRemaining(int64_t len);
#endif

  void CopyInto(const uint8_t* src, int32_t len) {
#ifndef NDEBUG
    UpdateRemaining(static_cast<int64_t>(len));
#endif
    std::memcpy(itr_, src, len);
    itr_ += len;
  }

  void CopyInto(uint8_t byte) {
#ifndef NDEBUG
    UpdateRemaining(1UL);
#endif
    *itr_ = byte;
    itr_++;
  }

  void FillZero(int32_t len) {
#ifndef NDEBUG
    UpdateRemaining(static_cast<int64_t>(len));
#endif
    std::memset(itr_, 0, len);
    itr_ += len;
  }

  [[nodiscard]] std::span<uint8_t> buf() const { return buf_; }
  [[nodiscard]] int64_t offset() const { return offset_; }

 private:
  std::span<uint8_t> buf_;
  int64_t offset_ = 0;
  uint8_t* itr_ = nullptr;
  int64_t remaining_ = 0;
  RandomAccessSink* sink_ = nullptr;
};

class RandomAccessSink {
 public:
  virtual ~RandomAccessSink() = default;
  virtual Status ReserveChunkAt(int64_t offset, int64_t len, SinkBuffer* out) = 0;
  virtual Status Finish() = 0;
  static std::unique_ptr<RandomAccessSink> FromBuffer(std::span<uint8_t> span);
  static std::unique_ptr<RandomAccessSink> FromFileDescriptor(int file_descriptor);
  static Status FromPath(std::string_view path, bool direct_io,
                         std::unique_ptr<RandomAccessSink>* out);

 private:
  virtual void FinishSinkBuffer(SinkBuffer* sink_buffer) = 0;
  friend SinkBuffer;
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
  explicit FileSource(int file_descriptor) : file_descriptor_(file_descriptor) {}
  [[nodiscard]] int file_descriptor() const { return file_descriptor_; }
  void CopyDataInto(uint8_t* dest, int64_t offset, int32_t len) const {
    lseek(file_descriptor_, offset, SEEK_SET);
    auto num_read = static_cast<int32_t>(read(file_descriptor_, dest, len));
    assert(num_read == len);
  }

 private:
  int file_descriptor_;
};

enum class RandomAccessSourceKind { kBuffer = 0, kFile = 1 };

class RandomAccessSource {
 public:
  explicit RandomAccessSource(RandomAccessSourceKind kind) : kind_(kind) {}
  virtual BufferSource AsBuffer() = 0;
  virtual FileSource AsFile() = 0;
  [[nodiscard]] RandomAccessSourceKind kind() const { return kind_; }

  static std::unique_ptr<RandomAccessSource> FromSpan(std::span<uint8_t> span);
  static std::unique_ptr<RandomAccessSource> FromFile(int file_descriptor,
                                                      bool close_on_destruct);
  static std::unique_ptr<RandomAccessSource> FromPath(std::string_view path,
                                                      bool is_direct);

 private:
  RandomAccessSourceKind kind_;
};

class Storage {
 public:
  virtual ~Storage() = default;
  virtual Status OpenRandomAccessSource(std::unique_ptr<RandomAccessSource>* out) = 0;
  virtual Status OpenStreamSink(std::unique_ptr<StreamSink>* out) = 0;
  virtual Status OpenRandomAccessSink(std::unique_ptr<RandomAccessSink>* out) = 0;
  static Status FromSpecifier(const util::Uri& specifier, std::unique_ptr<Storage>* out);
  virtual std::span<uint8_t> DebugBuffer() = 0;

  /// Return true if the storage requires reads and writes to operate on page-aligned
  /// buffers
  [[nodiscard]] virtual bool requires_alignment() const = 0;
  /// The page_size of the storage.  This is needed if the storage requires alignment
  [[nodiscard]] virtual int32_t page_size() const = 0;
};

}  // namespace quiver