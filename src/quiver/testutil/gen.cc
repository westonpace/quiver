#include "quiver/testutil/gen.h"

#include <random>

#include "quiver/util/logging_p.h"

namespace quiver::util {

    namespace {
        void RandomizeFlatArray() {

}
}

void RandomizeArray(Batch* batch, int array_index, int array_size = -1) {
  const FieldDescriptor& type = batch->schema()->top_level_types[array_index];
  switch (type.layout) {
    case LayoutKind::kFlat: {
    }
    default:
      DCHECK(false) << "Not yet implemented";
  }
}

}  // namespace quiver::util
