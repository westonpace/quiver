#pragma once

#include <functional>

#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/hash/hasher.h"
#include "quiver/hashtable/hashtable.h"
#include "quiver/pch.h"
#include "quiver/util/status.h"

namespace quiver::map {

class HashMap {
 public:
  virtual ~HashMap() = default;
  /// Inserts rows into the map
  ///
  /// \param keys the keys to insert, must have same length as payload
  /// \param payload the payload to insert, must have same length as keys
  ///
  /// \return Status::OK() if the insert succeeded, otherwise an error
  virtual Status Insert(ReadOnlyBatch* keys, ReadOnlyBatch* payload) = 0;

  /// Inserts rows into the map but accepts data as a single batch of
  /// key and payload
  ///
  /// \param bathc the batch to insert
  ///
  /// \return Status::OK() if the insert succeeded, otherwise an error
  virtual Status InsertCombinedBatch(ReadOnlyBatch* keys) = 0;
  /// Performs a lookup into the map
  ///
  /// For each row in the given keys an output row will be generated for each
  /// matching row inserted into the map.  If there are no matches for a given
  /// input row then there will be no output rows generated for it.  If there
  /// are multiple matches for a given input row then there will be multiple
  /// output rows generated.
  virtual Status Lookup(ReadOnlyBatch* keys, Batch* out) = 0;
  /// Performs a lookup into the map and joins payloads
  ///
  /// This performs the same lookup as Lookup however it also joins the payload.
  /// The output rows will have a schema of keys | input payload | match payload
  ///
  /// `payload` can have any schema, however it must have the same number of rows
  /// as `keys`.
  ///
  /// A single input row could generate billions of output rows.  As a result, there
  /// is no way we can efficiently return a single batch.  Instead, we accumulate batches
  /// and deliver them to a consumer.
  virtual Status InnerJoin(
      ReadOnlyBatch* keys, ReadOnlyBatch* payload, int32_t rows_per_batch,
      std::function<Status(std::unique_ptr<ReadOnlyBatch>)> consumer) = 0;

  static Status Create(const SimpleSchema* key_schema,
                       const SimpleSchema* build_payload_schema,
                       const SimpleSchema* probe_payload_schema,
                       std::unique_ptr<hash::Hasher> hasher, Storage* storage,
                       std::unique_ptr<hashtable::HashTable> hashtable,
                       std::unique_ptr<HashMap>* out);
};

}  // namespace quiver::map
