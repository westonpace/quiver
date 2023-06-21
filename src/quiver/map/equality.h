#pragma once

#include <memory>

#include "quiver/core/array.h"

namespace quiver::map {

class EqualityComparer {
 public:
  virtual ~EqualityComparer() = default;
  virtual Status CompareEquality(ReadOnlyArray lhs, ReadOnlyArray rhs,
                                 std::span<uint8_t> out) const = 0;

  static std::unique_ptr<EqualityComparer> MakeSimpleBinaryEqualityComparer();
};

}  // namespace quiver::map
