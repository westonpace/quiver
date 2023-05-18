// SPDX-License-Identifier: Apache-2.0

#include "quiver/store/mmap_store.h"

#include <cmath>
#include <limits>
#include <utility>

#include "quiver/util/constants.h"
#include "quiver/util/cpu_info.h"
#include "quiver/util/logging_p.h"
#include "quiver/util/memory.h"

namespace quiver::store::mmap {

namespace {

constexpr int32_t kInvalidBlockSizeBytes = -1;
constexpr int64_t kInvalidTotalSizeBytes = -1LL;
constexpr int64_t kFallbackDefaultTotalBytesSize = 4LL * kGiL;

class MmapStoreImpl : public MmapStore {
 public:
  Status Init(const MmapStoreOptions& options) {
    if (options.block_size_bytes < 0) {
      return Status::Invalid("block_size_bytes must be > 0");
    }
    if (options.block_size_bytes == MmapStoreOptions::kDefaultBlockSizeBytes) {
      block_size_bytes_ = CalculateDefaultBlockSizeBytes();
    } else {
      block_size_bytes_ = options.block_size_bytes;
    }
    if (options.total_size_bytes < 0) {
      return Status::Invalid("total_size_bytes must be > 0");
    }
    if (options.block_size_bytes == MmapStoreOptions::kDefaultTotalSizeBytes) {
      total_size_bytes_ = CalculateDefaultTotalSizeBytes();
    } else {
      total_size_bytes_ = options.total_size_bytes;
    }
    if (total_size_bytes_ < block_size_bytes_) {
      return Status::Invalid("total_size_bytes must be > block_size_bytes");
    }
    return Status::OK();
  }

  [[nodiscard]] int32_t block_size_bytes() const override { return block_size_bytes_; }
  [[nodiscard]] int64_t total_size_bytes() const override { return total_size_bytes_; }

 private:
  static int32_t CalculateDefaultBlockSizeBytes() {
    // Each block should be "almost L2" leaving some space for control data
    constexpr double kWiggleRoomMultiplier = 0.8;
    const int64_t l2_size =
        util::CpuInfo::GetInstance()->CacheSize(util::CpuInfo::CacheLevel::kL2);
    DCHECK_LE(l2_size, std::numeric_limits<int32_t>::max());
    const double l2_size_minus_wiggle_room =
        kWiggleRoomMultiplier * static_cast<double>(l2_size);
    return static_cast<int32_t>(std::floor(l2_size_minus_wiggle_room));
  }
  static int64_t CalculateDefaultTotalSizeBytes() {
    const int64_t total_ram_bytes = util::GetTotalMemoryBytes();
    if (total_ram_bytes == 0) {
      // No idea how much RAM is on the system so just take a wild guess
      QUIVER_LOG(kWarning) << "could not detect system RAM, defaulting to 4GiB store";
      return kFallbackDefaultTotalBytesSize;
    }
    return total_ram_bytes / 4LL;
  }

  int32_t block_size_bytes_ = kInvalidBlockSizeBytes;
  int64_t total_size_bytes_ = kInvalidTotalSizeBytes;
};

}  // namespace

Status MmapStore::Create(const MmapStoreOptions& options,
                         std::unique_ptr<MmapStore>* out) {
  auto new_store = std::make_unique<MmapStoreImpl>();
  QUIVER_RETURN_NOT_OK(new_store->Init(options));
  *out = std::move(new_store);
  return Status::OK();
}

}  // namespace quiver::store::mmap
