// SPDX-License-Identifier: Apache-2.0

#include "quiver/util/cpu_info.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cmath>
#include <fstream>

#include "quiver/util/constants.h"
#include "quiver/util/logging_p.h"

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#ifndef _MSC_VER
#include <unistd.h>
#endif

#ifdef _WIN32
#include "windows_p.h"
#endif

namespace quiver::util {

namespace {

constexpr int kCacheLevels = static_cast<int>(CpuInfo::CacheLevel::kLast) + 1;

#if defined(_WIN32)
//------------------------------ WINDOWS ------------------------------//
void OsRetrieveCacheSize(std::array<int64_t, kCacheLevels>* cache_sizes) {
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer = nullptr;
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer_position = nullptr;
  DWORD buffer_size = 0;
  size_t offset = 0;
  typedef BOOL(WINAPI * GetLogicalProcessorInformationFuncPointer)(void*, void*);
  GetLogicalProcessorInformationFuncPointer func_pointer =
      (GetLogicalProcessorInformationFuncPointer)GetProcAddress(
          GetModuleHandle("kernel32"), "GetLogicalProcessorInformation");

  if (!func_pointer) {
    QUIVER_LOG(kWarning) << "Failed to find procedure GetLogicalProcessorInformation";
    return;
  }

  // Get buffer size
  if (func_pointer(buffer, &buffer_size) && GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
    QUIVER_LOG(kWarning) << "Failed to get size of processor information buffer";
    return;
  }

  buffer = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION)malloc(buffer_size);
  if (!buffer) {
    return;
  }

  if (!func_pointer(buffer, &buffer_size)) {
    QUIVER_LOG(kWarning) << "Failed to get processor information";
    free(buffer);
    return;
  }

  buffer_position = buffer;
  while (offset + sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION) <= buffer_size) {
    if (RelationCache == buffer_position->Relationship) {
      PCACHE_DESCRIPTOR cache = &buffer_position->Cache;
      if (cache->Level >= 1 && cache->Level <= kCacheLevels) {
        const int64_t current = (*cache_sizes)[cache->Level - 1];
        (*cache_sizes)[cache->Level - 1] = std::max<int64_t>(current, cache->Size);
      }
    }
    offset += sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION);
    buffer_position++;
  }

  free(buffer);
}
#elif defined(__APPLE__)
void OsRetrieveCacheSize(std::array<int64_t, kCacheLevels>* cache_sizes) {
  static_assert(kCacheLevels >= 3, "");
  auto c = IntegerSysCtlByName("hw.l1dcachesize");
  if (c.has_value()) {
    (*cache_sizes)[0] = *c;
  }
  c = IntegerSysCtlByName("hw.l2cachesize");
  if (c.has_value()) {
    (*cache_sizes)[1] = *c;
  }
  c = IntegerSysCtlByName("hw.l3cachesize");
  if (c.has_value()) {
    (*cache_sizes)[2] = *c;
  }
}
#else
//------------------------------ LINUX ------------------------------//
// Get cache size, return 0 on error
int64_t LinuxGetCacheSize(int level) {
  // get cache size by sysconf()
#ifdef _SC_LEVEL1_DCACHE_SIZE
  constexpr std::array<int, kCacheLevels> kCacheSizeConf = {
      _SC_LEVEL1_DCACHE_SIZE,
      _SC_LEVEL2_CACHE_SIZE,
      _SC_LEVEL3_CACHE_SIZE,
  };

  errno = 0;
  const int64_t cache_size = sysconf(kCacheSizeConf[level]);
  if (errno == 0 && cache_size > 0) {
    return cache_size;
  }
#endif

  // get cache size from sysfs if sysconf() fails or not supported
  constexpr std::array<const char*, kCacheLevels> kCacheSizeSysfs = {
      "/sys/devices/system/cpu/cpu0/cache/index0/size",  // l1d (index1 is l1i)
      "/sys/devices/system/cpu/cpu0/cache/index2/size",  // l2
      "/sys/devices/system/cpu/cpu0/cache/index3/size",  // l3
  };

  std::ifstream cacheinfo(kCacheSizeSysfs[level], std::ios::in);
  if (!cacheinfo) {
    return 0;
  }
  // cacheinfo is one line like: 65536, 64K, 1M, etc.
  uint64_t size = 0;
  char unit = '\0';
  cacheinfo >> size >> unit;
  if (unit == 'K') {
    size <<= kBitsKi;
  } else if (unit == 'M') {
    size <<= kBitsMi;
  } else if (unit == 'G') {
    size <<= kBitsGi;
  } else if (unit != '\0') {
    return 0;
  }
  return static_cast<int64_t>(size);
}

void OsRetrieveCacheSize(std::array<int64_t, kCacheLevels>* cache_sizes) {
  for (int i = 0; i < kCacheLevels; ++i) {
    const int64_t cache_size = LinuxGetCacheSize(i);
    if (cache_size > 0) {
      (*cache_sizes)[i] = cache_size;
    }
  }
}
#endif  // WINDOWS, MACOS, LINUX

class CpuInfoImpl : public CpuInfo {
 public:
  CpuInfoImpl() { OsRetrieveCacheSize(&cache_sizes_); };

  [[nodiscard]] int64_t CacheSize(CacheLevel level) const override {
    constexpr std::array<int64_t, kCacheLevels> kDefaultCacheSizes = {
        32LL * 1024LL,    // Level 1: 32K
        256LL * 1024LL,   // Level 2: 256K
        3072LL * 1024LL,  // Level 3: 3M
    };

    static_assert(static_cast<int>(CacheLevel::kL1) == 0);
    const int level_idx = static_cast<int>(level);
    if (cache_sizes_[level_idx] > 0) {
      return cache_sizes_[level_idx];
    }
    if (level_idx == 0) {
      return kDefaultCacheSizes[0];
    }
    // l3 may be not available, return maximum of l2 or default size
    return std::max(kDefaultCacheSizes[level_idx], cache_sizes_[level_idx - 1]);
  }

 private:
  std::array<int64_t, kCacheLevels> cache_sizes_{};
};

}  // namespace

const CpuInfo* CpuInfo::GetInstance() {
  static CpuInfoImpl cpu_info;
  return &cpu_info;
}

}  // namespace quiver::util
