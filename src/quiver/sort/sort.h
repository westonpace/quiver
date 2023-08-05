#include "quiver/core/array.h"
#include "quiver/core/io.h"

namespace quiver::sort {

class AllColumnsBinarySorter {
 public:
  virtual Status Sort(const ReadOnlyBatch& unsorted, Batch* sorted) = 0;
  static Status Create(Storage* storage, std::unique_ptr<AllColumnsBinarySorter>* out);
};

}  // namespace quiver::sort
