#include "quiver/util/arrow_util.h"

namespace quiver::util {

void ArrayDeleter::operator()(ArrowArray* array) {
  if (array != nullptr) {
    if (array->release != nullptr) {
      array->release(array);
    }
    delete array;
  }
}

OwnedArrowArray AllocateArrowArray() {
  OwnedArrowArray new_array = OwnedArrowArray(new ArrowArray());
  new_array->release = nullptr;
  new_array->private_data = nullptr;
  return new_array;
}

}  // namespace quiver::util
