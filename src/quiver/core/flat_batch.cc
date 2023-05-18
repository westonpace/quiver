// SPDX-License-Identifier: Apache-2.0

#include "quiver/core/flat_batch.h"

namespace quiver {

namespace {

int32_t CountNumBuffers(const ArrowArray& array) {
  int32_t num_buffers = 0;
  num_buffers += array.n_buffers;
  if (array.dictionary != nullptr) {
    num_buffers += CountNumBuffers(*array.dictionary);
  }
  for (int32_t child_idx = 0; child_idx < array.n_children; child_idx++) {
    num_buffers += CountNumBuffers(*(array.children[child_idx]));
  }
  return num_buffers;
}

}  // namespace

FieldDescriptor FieldDescriptor::CopyArrowSchema(const ArrowSchema& schema) {
  FieldDescriptor descriptor;
  descriptor.format = std::string_view(schema.format);
  descriptor.metadata = std::string_view(schema.metadata);
  descriptor.name = std::string_view(schema.name);
  descriptor.num_children = schema.n_children;
  descriptor.nullable = schema.flags & ARROW_FLAG_NULLABLE;
  descriptor.map_keys_sorted = schema.flags & ARROW_FLAG_MAP_KEYS_SORTED;
  descriptor.dict_indices_ordered = schema.flags & ARROW_FLAG_DICTIONARY_ORDERED;
}

FlatBatch::~FlatBatch() {
  if (root) {
    root->release(&(*root));
  }
}

FlatBatch FlatBatch::FromArrow(ArrowArray array, const ArrowSchema& schema) {}

}  // namespace quiver