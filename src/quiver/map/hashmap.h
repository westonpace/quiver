#pragma once

#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/hash/hasher.h"
#include "quiver/hashtable/hashtable.h"
#include "quiver/util/status.h"

namespace quiver::map {

class HashMap {
 public:
  virtual ~HashMap() = default;
  virtual Status Insert(ReadOnlyBatch* keys, ReadOnlyBatch* payload) = 0;
  virtual Status Lookup(ReadOnlyBatch* keys, Batch* out) = 0;

  static Status Create(const SimpleSchema* key_schema, const SimpleSchema* payload_schema,
                       std::unique_ptr<hash::Hasher> hasher, StreamSink* sink,
                       RandomAccessSource* source,
                       std::unique_ptr<hashtable::HashTable> hashtable,
                       std::unique_ptr<HashMap>* out);
};

}  // namespace quiver::map
