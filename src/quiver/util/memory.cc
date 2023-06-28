#include "quiver/util/memory.h"

#include <cerrno>
#include <cstring>

#include "quiver/util/logging_p.h"

#ifdef _WIN32
#include "windows_p.h"
#undef WIN32_LEAN_AND_MEAN

#elif __APPLE__
#include <mach/mach.h>
#include <sys/sysctl.h>

#elif __linux__
#include <sys/sysinfo.h>
#endif

namespace quiver::util {

int64_t GetTotalMemoryBytes() {
#if defined(_WIN32)
  ULONGLONG result_kb;
  if (!GetPhysicallyInstalledSystemMemory(&result_kb)) {
    QUIVER_LOG(kWarning) << "Failed to resolve total RAM size: "
                         << std::strerror(GetLastError());
    return -1;
  }
  return static_cast<int64_t>(result_kb * 1024);
#elif defined(__APPLE__)
  int64_t result;
  size_t size = sizeof(result);
  if (sysctlbyname("hw.memsize", &result, &size, nullptr, 0) == -1) {
    ARROW_LOG(WARNING) << "Failed to resolve total RAM size";
    return -1;
  }
  return result;
#elif defined(__linux__)
  struct sysinfo info;
  if (sysinfo(&info) == -1) {
    QUIVER_LOG(kWarning) << "Failed to resolve total RAM size: " << std::strerror(errno);
    return -1;
  }
  return static_cast<int64_t>(info.totalram * info.mem_unit);
#else
  return 0;
#endif
}

struct DeleteAligned {
  void operator()(uint8_t** data) const {
    free(*data);
    delete data;
  }
};

std::shared_ptr<uint8_t*> AllocateAligned(int64_t size, int64_t alignment) {
  uint8_t** aligned_buf = new uint8_t*();  // NOLINT(modernize-use-auto)
  int ret = posix_memalign((void**)aligned_buf, alignment, size);
  DCHECK_EQ(ret, 0) << "posix_memalign failed";
  DCHECK_EQ(reinterpret_cast<intptr_t>(*aligned_buf) % alignment, 0)
      << " buffer must be aligned to " << alignment << " bytes";
  return {aligned_buf, DeleteAligned()};
}

}  // namespace quiver::util
