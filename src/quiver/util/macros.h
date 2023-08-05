// SPDX-License-Identifier: Apache-2.0

// Adapted from Apache Arrow

#define QUIVER_EXPAND(x) x
#define QUIVER_STRINGIFY(x) #x
#define QUIVER_CONCAT(x, y) x##y
#define QUIVER_LITTLE_ENDIAN 1

#if defined(__GNUC__)
#define QUIVER_UNLIKELY(x) (__builtin_expect(!!(x), 0))
#define QUIVER_LIKELY(x) (__builtin_expect(!!(x), 1))
#elif defined(_MSC_VER)
#define QUIVER_UNLIKELY(x) (x)
#define QUIVER_LIKELY(x) (x)
#else
#define QUIVER_UNLIKELY(x) (x)
#define QUIVER_LIKELY(x) (x)
#endif
