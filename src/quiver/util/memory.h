// SPDX-License-Identifier: Apache-2.0

#include <cstdint>

namespace quiver::util {

/// \brief Get the total memory available to the system in bytes
///
/// This function supports Windows, Linux, and Mac and will return 0 otherwise
int64_t GetTotalMemoryBytes();

}  // namespace quiver::util
