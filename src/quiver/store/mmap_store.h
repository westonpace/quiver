// SPDX-License-Identifier: Apache-2.0

#include <cstdint>
#include <memory>

#include "quiver/util/status.h"

namespace quiver::store::mmap {

struct MmapStoreOptions {
  /// default value for the block size
  ///
  /// Will default to 0.8 * L2 cache size
  static constexpr int32_t kDefaultBlockSizeBytes = 0;
  /// default value for the total size of the heap
  ///
  /// Will default to 1/4 system RAM or 4GiB if system RAM cannot be deduced
  static constexpr int32_t kDefaultTotalSizeBytes = 0;

  int32_t block_size_bytes = kDefaultBlockSizeBytes;
  int64_t total_size_bytes = kDefaultTotalSizeBytes;
};

class MmapStore {
 public:
  virtual ~MmapStore() = default;
  static Status Create(const MmapStoreOptions& options, std::unique_ptr<MmapStore>* out);
  [[nodiscard]] virtual int32_t block_size_bytes() const = 0;
  [[nodiscard]] virtual int64_t total_size_bytes() const = 0;
};

}  // namespace quiver::store::mmap
