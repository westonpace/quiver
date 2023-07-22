// SPDX-License-Identifier: Apache-2.0

#include "quiver/core/io.h"

#include <fcntl.h>
#include <spdlog/spdlog.h>

#include <deque>
#include <limits>
#include <span>
#include <sstream>
#include <vector>

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

SinkBuffer::SinkBuffer(SinkBuffer&& other) noexcept
    : buf_(other.buf_),
      offset_(other.offset_),
      itr_(other.itr_),
      remaining_(other.remaining_),
      sink_(other.sink_) {
  other.itr_ = nullptr;
  other.sink_ = nullptr;
  other.offset_ = -1;
}

SinkBuffer& SinkBuffer::operator=(SinkBuffer&& other) noexcept {
  buf_ = other.buf_;
  offset_ = other.offset_;
  itr_ = other.itr_;
  remaining_ = other.remaining_;
  sink_ = other.sink_;
  other.itr_ = nullptr;
  other.sink_ = nullptr;
  other.offset_ = -1;
  return *this;
}

SinkBuffer::~SinkBuffer() noexcept {
  if (sink_ != nullptr) {
    sink_->FinishSinkBuffer(this);
  }
}

#ifndef NDEBUG
void SinkBuffer::UpdateRemaining(int64_t len) {
  remaining_ -= len;
  DCHECK_GE(remaining_, 0) << "Attempt to write past the end of a sink buffer";
}
#endif

class RamSink : public RandomAccessSink {
 public:
  RamSink(std::span<uint8_t> data) : data_(data) {}
  Status ReserveChunkAt(int64_t offset, int64_t len, SinkBuffer* out) override {
    DCHECK_GE(offset, 0);
    DCHECK_GT(len, 0);
    if (offset + len > static_cast<int64_t>(data_.size())) {
      return Status::Invalid("Attempted to write past end of RAM sink");
    }
    *out = SinkBuffer({data_.data() + offset, static_cast<uint64_t>(len)}, offset, this);
    return Status::OK();
  }
  Status Finish() override { return Status::OK(); }

 private:
  void FinishSinkBuffer(SinkBuffer* /*sink_buffer*/) override {}
  std::span<uint8_t> data_;
};

std::unique_ptr<RandomAccessSink> RandomAccessSink::FromBuffer(std::span<uint8_t> span) {
  return std::make_unique<RamSink>(span);
}

class FileSink : public RandomAccessSink {
 public:
  FileSink(int file_descriptor) : file_descriptor_(file_descriptor) {}

  Status ReserveChunkAt(int64_t offset, int64_t len, SinkBuffer* out) override {
    if (!last_error_.ok()) {
      return last_error_;
    }
    DCHECK_GE(offset, 0);
    DCHECK_GT(len, 0);
    std::vector<uint8_t> page(len);
    std::vector<uint8_t>* page_ref;
    {
      std::lock_guard lock(mutex_);
      auto inserted = pages_.insert({offset, std::move(page)});
      if (!inserted.second) {
        return Status::Invalid("Multiple FileSink writes to offset ", offset);
      }
      page_ref = &(inserted.first->second);
    }
    *out = {std::span<uint8_t>(*page_ref), offset, this};
    return Status::OK();
  }

  Status Finish() override {
    if (!pages_.empty()) {
      return Status::UnknownError(
          "Finish called on FileSink but some pages still existed");
    }
    return last_error_;
  }

 private:
  void FinishSinkBuffer(SinkBuffer* sink_buffer) override {
    uint64_t written = pwrite64(file_descriptor_, sink_buffer->buf().data(),
                                sink_buffer->buf().size(), sink_buffer->offset());
    if (written != sink_buffer->buf().size()) {
      last_error_ = Status::IOError(
          "Received " + std::to_string(written) + " from pwrite64 and expected " +
          std::to_string(sink_buffer->buf().size()) + strerror(errno));
    }

    std::lock_guard lock(mutex_);
    pages_.erase(sink_buffer->offset());
  }

  std::unordered_map<int64_t, std::vector<uint8_t>> pages_;
  std::mutex mutex_;

  int file_descriptor_;
  Status last_error_;
};

std::unique_ptr<RandomAccessSink> RandomAccessSink::FromFileDescriptor(
    int file_descriptor) {
  return std::make_unique<FileSink>(file_descriptor);
}

Status RandomAccessSink::FromPath(std::string_view path, bool direct_io,
                                  std::unique_ptr<RandomAccessSink>* out) {
  int flags = O_WRONLY | O_CREAT;
  if (direct_io) {
    flags |= O_DIRECT;
  }
  flags |= O_TRUNC;
  int file_descriptor = open(path.data(), flags, 0644);
  if (file_descriptor < 0) {
    return Status::IOError("Could not open file sink at ", path, ": ", strerror(errno));
  }
  *out = std::make_unique<FileSink>(file_descriptor);
  return Status::OK();
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
  RamStorage(int64_t size_bytes) : data_(size_bytes) {
    spdlog::debug("Allocated {} bytes of RAM storage", size_bytes);
  }
  ~RamStorage() override {
    spdlog::debug("Released {} bytes of RAM storage", data_.size());
  }
  Status OpenRandomAccessSource(std::unique_ptr<RandomAccessSource>* out) override {
    *out = RandomAccessSource::FromSpan(std::span<uint8_t>(data_));
    return Status::OK();
  }

  Status OpenStreamSink(std::unique_ptr<StreamSink>* out) override {
    *out = std::make_unique<StreamSink>(
        StreamSink::FromFixedSizeSpan(std::span<uint8_t>(data_)));
    return Status::OK();
  }

  Status OpenRandomAccessSink(std::unique_ptr<RandomAccessSink>* out) override {
    *out = RandomAccessSink::FromBuffer(std::span<uint8_t>(data_));
    return Status::OK();
  }

  std::span<uint8_t> DebugBuffer() override { return data_; }

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

  Status OpenRandomAccessSink(std::unique_ptr<RandomAccessSink>* out) override {
    return RandomAccessSink::FromPath(path_, direct_io_, out);
  }

  std::span<uint8_t> DebugBuffer() override { return {}; }

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