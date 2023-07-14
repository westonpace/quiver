// SPDX-License-Identifier: Apache-2.0

#include "quiver/core/io.h"

#include <fcntl.h>

#include <iostream>
#include <limits>
#include <span>
#include <sstream>

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

StreamSink StreamSink::FromPath(const std::string& path, bool direct_io, bool append,
                                int32_t write_buffer_size) {
  int flags = O_WRONLY | O_CREAT;
  if (direct_io) {
    flags |= O_DIRECT;
  }
  if (append) {
    flags |= O_APPEND;
  } else {
    flags |= O_TRUNC;
  }
  int file_descriptor = open(path.data(), flags, 0644);
  uint8_t* aligned_buf;
  int ret = posix_memalign((void**)&aligned_buf, 512, write_buffer_size);
  DCHECK_EQ(ret, 0) << "posix_memalign failed";
  std::shared_ptr<uint8_t> buf_ptr(aligned_buf, [](uint8_t* ptr) { free(ptr); });
  DCHECK_EQ(reinterpret_cast<intptr_t>(buf_ptr.get()) % 512, 0)
      << " buffer must be aligned to 512 bytes";
  std::function<uint8_t*(uint8_t*, int32_t, int32_t*)> flush =
      [direct_io, file_descriptor, buf = std::move(buf_ptr), size = write_buffer_size](
          uint8_t* start, int32_t len, int32_t* remaining) mutable -> uint8_t* {
    DCHECK((!direct_io) || (len % 512 == 0));
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

std::unique_ptr<RandomAccessSource> RandomAccessSource::FromPath(std::string_view path,
                                                                 bool is_direct) {
  int flags = O_RDONLY;
  if (is_direct) {
    flags |= O_DIRECT;
  }
  int file_descriptor = open(path.data(), flags, 0644);
  DCHECK_GE(file_descriptor, 0) << "failed to open file " << path;
  return RandomAccessSource::FromFile(file_descriptor, /*close_on_destruct=*/true);
}

class RamStorage : public Storage {
 public:
  RamStorage(int64_t size_bytes) : data_(size_bytes) {}
  Status OpenRandomAccessSource(std::unique_ptr<RandomAccessSource>* out) override {
    *out = RandomAccessSource::FromSpan(std::span<uint8_t>(data_));
    return Status::OK();
  }

  Status OpenStreamSink(std::unique_ptr<StreamSink>* out) override {
    *out = std::make_unique<StreamSink>(
        StreamSink::FromFixedSizeSpan(std::span<uint8_t>(data_)));
    return Status::OK();
  }

  [[nodiscard]] bool requires_alignment() const override { return false; }
  // TODO(#3) return actual value
  [[nodiscard]] int32_t page_size() const override { return 1; }

 private:
  std::vector<uint8_t> data_;
};

class FileStorage : public Storage {
 public:
  FileStorage(std::string path, bool direct_io)
      : path_(std::move(path)), direct_io_(direct_io) {}

  Status OpenRandomAccessSource(std::unique_ptr<RandomAccessSource>* out) override {
    *out = RandomAccessSource::FromPath(path_, direct_io_);
    return Status::OK();
  }

  Status OpenStreamSink(std::unique_ptr<StreamSink>* out) override {
    *out = std::make_unique<StreamSink>(
        StreamSink::FromPath(path_, direct_io_, /*append=*/false));
    return Status::OK();
  }

  [[nodiscard]] bool requires_alignment() const override { return direct_io_; }
  // TODO(#4) determine actual sector size
  [[nodiscard]] int32_t page_size() const override { return 4096; }

 private:
  const std::string path_;
  const bool direct_io_;
};

Status Storage::FromSpecifier(const util::Uri& specifier, std::unique_ptr<Storage>* out) {
  if (specifier.scheme == "ram") {
    auto size_bytes_str = specifier.query.find("size_bytes");
    if (size_bytes_str == specifier.query.end()) {
      return Status::Invalid("RAM specifier must include size_bytes query parameter: ",
                             specifier.ToString());
    }
    std::stringstream size_bytes_stream(size_bytes_str->second);
    int64_t size_bytes = -1;
    size_bytes_stream >> size_bytes;
    if (size_bytes < 0) {
      return Status::Invalid("RAM specifier does not specify a valid size_bytes: ",
                             specifier.ToString());
    }
    *out = std::make_unique<RamStorage>(size_bytes);
    return Status::OK();
  }
  if (specifier.scheme == "file") {
    bool direct_io = false;
    auto direct_str = specifier.query.find("direct");
    if (direct_str != specifier.query.end()) {
      if (direct_str->second == "true") {
        direct_io = true;
      } else if (direct_str->second == "false") {
        direct_io = false;
      } else {
        return Status::Invalid(
            "file specifier does not specify a valid value for the direct query "
            "parameter (must be \"true\" or \"false\"): ",
            specifier.ToString());
      }
    }
    *out = std::make_unique<FileStorage>(specifier.path, direct_io);
    return Status::OK();
  }
  return Status::Invalid("Unrecognized storage specifier: ", specifier.ToString());
}

}  // namespace quiver