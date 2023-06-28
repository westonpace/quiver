// SPDX-License-Identifier: Apache-2.0

#include "quiver/core/io.h"

#include <fcntl.h>

#include <iostream>
#include <limits>
#include <span>

#include "quiver/core/array.h"
#include "quiver/util/logging_p.h"

namespace quiver {

StreamSink StreamSink::FromFixedSizeSpan(std::span<uint8_t> span) {
  QUIVER_CHECK_LE(span.size(), std::numeric_limits<int32_t>::max());
  return {span.data(), static_cast<int32_t>(span.size()),
          [start = span.data(), len = static_cast<int32_t>(span.size())](
              uint8_t*, int32_t, int32_t* remaining) {
            *remaining = len;
            return start;
          }};
}

StreamSink StreamSink::FromFile(int file_descriptor, int32_t write_buffer_size) {
  uint8_t* aligned_buf;
  int ret = posix_memalign((void**)&aligned_buf, 512, write_buffer_size);
  DCHECK_EQ(ret, 0) << "posix_memalign failed";
  std::shared_ptr<uint8_t> buf_ptr(aligned_buf, [](uint8_t* ptr) { free(ptr); });
  DCHECK_EQ(reinterpret_cast<intptr_t>(buf_ptr.get()) % 512, 0)
      << " buffer must be aligned to 512 bytes";
  std::function<uint8_t*(uint8_t*, int32_t, int32_t*)> flush =
      [file_descriptor, buf = std::move(buf_ptr), size = write_buffer_size](
          uint8_t* start, int32_t len, int32_t* remaining) mutable -> uint8_t* {
    DCHECK_EQ(len % 512, 0);
    DCHECK_EQ(reinterpret_cast<intptr_t>(start) % 512, 0);
    auto written = static_cast<int32_t>(write(file_descriptor, start, len));
    DCHECK_EQ(len, written) << "expected to write " << len << " bytes, but wrote "
                            << written << " file=" << file_descriptor
                            << " error: " << strerror(errno);
    *remaining = static_cast<int32_t>(size);
    return buf.get();
  };
  return {aligned_buf, write_buffer_size, std::move(flush)};
}

class SpanSource : public RandomAccessSource {
 public:
  SpanSource(std::span<uint8_t> span)
      : RandomAccessSource(RandomAccessSourceKind::kBuffer), span_(span) {}
  BufferSource AsBuffer() override { return BufferSource(span_.data()); }
  FileSource AsFile() override {
    QUIVER_CHECK(false) << "Invalid attempt to access SpanSource as file";
    return FileSource(-1);
  }

 private:
  std::span<uint8_t> span_;
};

class CFileSource : public RandomAccessSource {
 public:
  CFileSource(int file_descriptor, bool close_on_destruct)
      : RandomAccessSource(RandomAccessSourceKind::kFile),
        file_descriptor_(file_descriptor),
        close_on_destruct_(close_on_destruct) {}
  ~CFileSource() {
    if (close_on_destruct_) {
      int err = close(file_descriptor_);
      DCHECK_EQ(0, err);
    }
  }
  BufferSource AsBuffer() override {
    QUIVER_CHECK(false) << "Invalid attempt to access FileSource as buffer";
    return BufferSource(nullptr);
  }
  FileSource AsFile() override { return FileSource(file_descriptor_); }

 private:
  int file_descriptor_;
  bool close_on_destruct_;
};

std::unique_ptr<RandomAccessSource> RandomAccessSource::FromSpan(
    std::span<uint8_t> span) {
  return std::make_unique<SpanSource>(span);
}

std::unique_ptr<RandomAccessSource> RandomAccessSource::FromFile(int file_descriptor,
                                                                 bool close_on_destruct) {
  return std::make_unique<CFileSource>(file_descriptor, close_on_destruct);
}

std::unique_ptr<RandomAccessSource> RandomAccessSource::FromPath(std::string_view path) {
  int file_descriptor = open(path.data(), 0, O_RDONLY);
  DCHECK_GE(file_descriptor, 0) << "failed to open file " << path;
  return RandomAccessSource::FromFile(file_descriptor, /*close_on_destruct=*/true);
}

}  // namespace quiver