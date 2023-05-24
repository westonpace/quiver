#pragma once

#include <memory>

#include "quiver/core/arrow.h"

namespace quiver::util {

struct ArrayDeleter {
  void operator()(ArrowArray* array);
};

using OwnedArrowArray = std::unique_ptr<ArrowArray, ArrayDeleter>;

OwnedArrowArray AllocateArrowArray();

}  // namespace quiver::util