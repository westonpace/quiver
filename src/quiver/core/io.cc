// SPDX-License-Identifier: Apache-2.0

#include "quiver/core/io.h"

#include <limits>
#include <span>

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

StreamSink StreamSink::FromFile(std::FILE* file, int32_t write_buffer_size) {
  std::vector<uint8_t> buffer(write_buffer_size);
  uint8_t* buf_data = buffer.data();
  std::function<uint8_t*(uint8_t*, int32_t, int32_t*)> flush =
      [file, buf = std::move(buffer)](uint8_t* start, int32_t len,
                                      int32_t* remaining) mutable -> uint8_t* {
    auto written = static_cast<int32_t>(std::fwrite(start, 1, len, file));
    DCHECK_EQ(len, written);
    *remaining = static_cast<int32_t>(buf.size());
    return buf.data();
  };
  return {buf_data, write_buffer_size, std::move(flush)};
}

class SpanSource : public RandomAccessSource {
 public:
  SpanSource(std::span<uint8_t> span)
      : RandomAccessSource(RandomAccessSourceKind::kBuffer), span_(span) {}
  BufferSource AsBuffer() override { return BufferSource(span_.data()); }
  FileSource AsFile() override {
    QUIVER_CHECK(false) << "Invalid attempt to access SpanSource as file";
    return FileSource(nullptr);
  }

 private:
  std::span<uint8_t> span_;
};

class CFileSource : public RandomAccessSource {
 public:
  CFileSource(std::FILE* file, bool close_on_destruct)
      : RandomAccessSource(RandomAccessSourceKind::kFile),
        file_(file),
        close_on_destruct_(close_on_destruct) {}
  ~CFileSource() {
    if (close_on_destruct_) {
      int err = std::fclose(file_);
      DCHECK_EQ(0, err);
    }
  }
  BufferSource AsBuffer() override {
    QUIVER_CHECK(false) << "Invalid attempt to access FileSource as buffer";
    return BufferSource(nullptr);
  }
  FileSource AsFile() override { return FileSource(file_); }

 private:
  std::FILE* file_;
  bool close_on_destruct_;
};

std::unique_ptr<RandomAccessSource> RandomAccessSource::FromSpan(
    std::span<uint8_t> span) {
  return std::make_unique<SpanSource>(span);
}

std::unique_ptr<RandomAccessSource> RandomAccessSource::FromFile(std::FILE* file,
                                                                 bool close_on_destruct) {
  return std::make_unique<CFileSource>(file, close_on_destruct);
}

std::unique_ptr<RandomAccessSource> RandomAccessSource::FromPath(std::string_view path) {
  std::FILE* file = std::fopen(path.data(), "rb");
  return RandomAccessSource::FromFile(file, /*close_on_destruct=*/true);
}

}  // namespace quiver