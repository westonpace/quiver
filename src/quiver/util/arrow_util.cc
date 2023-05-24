#include "quiver/util/arrow_util.h"

#include "quiver/core/arrow.h"

namespace quiver::util {

void ArrayDeleter::operator()(ArrowArray* array) {
  if (array != nullptr) {
    if (array->release != nullptr) {
      array->release(array);
    }
    delete array;
  }
}

void SchemaDeleter::operator()(ArrowSchema* schema) {
  if (schema != nullptr) {
    if (schema->release != nullptr) {
      schema->release(schema);
    }
    delete schema;
  }
}

OwnedArrowArray AllocateArrowArray() {
  OwnedArrowArray new_array = OwnedArrowArray(new ArrowArray());
  new_array->release = nullptr;
  new_array->private_data = nullptr;
  return new_array;
}

OwnedArrowSchema AllocateArrowSchema() {
  OwnedArrowSchema new_schema = OwnedArrowSchema(new ArrowSchema());
  new_schema->release = nullptr;
  new_schema->private_data = nullptr;
  return new_schema;
}

}  // namespace quiver::util
