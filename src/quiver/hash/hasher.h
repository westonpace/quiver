#pragma once

#include "quiver/core/array.h"
#include "quiver/util/status.h"

namespace quiver::hash {

class Hasher {
 public:
  virtual ~Hasher() = default;
  /// <summary>
  /// Compute hashes for the values in batch
  /// </summary>
  /// Will store one output hash for each row in the batch.  All columns will participate
  /// in the hashing
  ///
  /// Expects that `out` has the same length as `batch` and has
  /// a width of hash_width_bytes().  It is the caller's responsibility
  /// to allocate output before calling this method.
  /// <param name="batch"></param>
  /// <param name="out"></param>
  virtual Status HashBatch(ReadOnlyBatch* batch, std::span<int64_t> out) = 0;

  /// <summary>
  /// Create a simple "identity" hasher
  /// </summary>
  ///
  /// This hasher expects that the input batch is a single flat array with
  /// a width of `hash_width_bytes`.  The column itself is then used as a hash.
  ///
  /// This is useful for testing or for cases where the user has already computed
  /// a hash with some external library.
  static std::unique_ptr<Hasher> CreateIdentityHasher();
};

constexpr int32_t HashWidthBytes() { return sizeof(int64_t); }

}  // namespace quiver::hash