// SPDX-License-Identifier: Apache-2.0

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "quiver/core/arrow.h"

namespace quiver {

struct FieldDescriptor {
  std::string_view format;
  std::string_view name;
  std::string_view metadata;
  bool nullable;
  bool dict_indices_ordered;
  bool map_keys_sorted;
  bool has_dictionary;
  int32_t num_children;
  int32_t num_buffers;
  int32_t index;

  static FieldDescriptor CopyArrowSchema(const ArrowSchema& schema);
};

struct Buffer {
  uint8_t* bytes;
  int64_t num_bytes;
  FieldDescriptor* descriptor;
};

struct FlatBatch {
 public:
  ~FlatBatch();

  static FlatBatch FromArrow(ArrowArray array, const ArrowSchema& schema);

 private:
  std::vector<Buffer> buffers;
  // If this flat batch was sourced from an external array we
  // keep a pointer to it so we can release it when we're done
  std::optional<ArrowArray> root;
  int64_t metadata_size;
  int64_t total_size;
};

}  // namespace quiver
