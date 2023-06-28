// SPDX-License-Identifier: Apache-2.0

#include <cstdint>
#include <memory>

namespace quiver::util {

/// \brief Get the total memory available to the system in bytes
///
/// This function supports Windows, Linux, and Mac and will return 0 otherwise
int64_t GetTotalMemoryBytes();

std::shared_ptr<uint8_t*> AllocateAligned(int64_t size, int64_t alignment);

}  // namespace quiver::util
