#pragma once

#include "quiver/core/array.h"
#include "quiver/util/status.h"

namespace quiver::hash {

class Hasher {
 public:
  /// <summary>
  /// The width of a single hash value, in bytes
  /// </summary>
  virtual int32_t hash_width_bytes() const = 0;
  /// <summary>
  /// Compute hashes for the values in batch
  /// </summary>
  /// Will store one output hash for each row in the batch
  ///
  /// Expects that `out` has the same length as `batch` and has
  /// a width of hash_width_bytes().  It is the caller's responsibility
  /// to allocate output before calling this method.
  /// <param name="batch"></param>
  /// <param name="out"></param>
  virtual Status HashBatch(ReadOnlyBatch* batch, FlatArray out) = 0;

  /// <summary>
  /// Create a simple "identity" hasher
  /// </summary>
  ///
  /// This hasher expects that the input batch is a single flat array with
  /// a width of `hash_width_bytes`.  The column itself is then used as a hash.
  ///
  /// This is useful for testing or for cases where the user has already computed
  /// a hash with some external library.
  static std::unique_ptr<Hasher> CreateIdentityHasher(int32_t hash_width_bytes);
};

}  // namespace quiver