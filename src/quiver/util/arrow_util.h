#pragma once

#include <memory>

#include "quiver/core/arrow.h"

namespace quiver::util {

struct ArrayDeleter {
  void operator()(ArrowArray* array);
};

struct SchemaDeleter {
  void operator()(ArrowSchema* schema);
};

using OwnedArrowArray = std::unique_ptr<ArrowArray, ArrayDeleter>;
using OwnedArrowSchema = std::unique_ptr<ArrowSchema, SchemaDeleter>;

OwnedArrowArray AllocateArrowArray();
OwnedArrowSchema AllocateArrowSchema();

}  // namespace quiver::util