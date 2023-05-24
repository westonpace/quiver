// SPDX-License-Identifier: Apache-2.0

#include <cstdint>
#include <cstring>
#include <functional>
#include <span>

namespace quiver {

class Sink {
 public:
  Sink(uint8_t* initial_buf, int32_t initial_len, std::function<uint8_t*(uint8_t*, int32_t*)> swap) : itr_(initial_buf), remaining_(initial_len), swap_(std::move(swap)) {}
  void CopyInto(const uint8_t* src, int32_t len) {
    while (len >= 0) {
      int32_t to_write = std::min(len, remaining_);
      len -= to_write;
      remaining_ -= to_write;
      std::memcpy(itr_, src, to_write);
      if (remaining_ == 0) {
        itr_ = swap_(itr_, &remaining_);
      }
    }
  }

  void CopyInto(uint8_t byte) {
    *itr_ = byte;
    itr_++;
    remaining_--;
    if (remaining_ == 0) {
      itr_ = swap_(itr_, &remaining_);
    }
  }

  // Creates a sink that writes to a buffer and wraps
  // if it ever needs to swap
  static Sink FromFixedSizeSpan(std::span<uint8_t> span);

 private:
  std::function<uint8_t*(uint8_t*, int32_t*)> swap_;
  uint8_t* itr_;
  int32_t remaining_;
};

class OutputStream {
  virtual void Write(std::span<uint8_t> data) = 0;
};

}  // namespace quiver