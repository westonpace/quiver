#include "quiver/map/hashmap.h"

#include <gtest/gtest.h>

#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/hashtable/hashtable.h"
#include "quiver/row/row_p.h"
#include "quiver/testutil/test_util.h"
#include "quiver/util/local_allocator_p.h"

namespace quiver::map {

Status CreateHashMap(const SimpleSchema* key_schema,
                     const SimpleSchema* build_payload_schema,
                     const SimpleSchema* probe_payload_schema, Storage* storage,
                     std::unique_ptr<HashMap>* out) {
  std::unique_ptr<hash::Hasher> hasher = hash::Hasher::CreateIdentityHasher();
  std::unique_ptr<hashtable::HashTable> hashtable = hashtable::HashTable::MakeStl();
  return HashMap::Create(key_schema, build_payload_schema, probe_payload_schema,
                         std::move(hasher), storage, std::move(hashtable), out);
}

TEST(HashMap, Lookup) {
  constexpr int64_t kEnoughBytesForScratch = 1024LL * 1024LL;
  std::unique_ptr<Storage> storage = TestStorage(kEnoughBytesForScratch);

  SchemaAndBatch keys =
      TestBatch({Int64Array({1, 2, 0, 1000, 0}), Int16Array({1, 100, 1000, 57, {}})});
  SchemaAndBatch payload =
      TestBatch({Int32Array({1, 2, 3, 4, 5}), Int16Array({1, 1, 1, 2, 2})});

  util::LocalAllocator local_allocator;

  std::unique_ptr<HashMap> hashmap;
  AssertOk(CreateHashMap(&keys.schema, &payload.schema, /*probe_payload_schema=*/nullptr,
                         storage.get(), &hashmap));

  AssertOk(hashmap->Insert(keys.batch.get(), payload.batch.get()));

  SchemaAndBatch lookup_keys = TestBatch({Int64Array({1000, 1}), Int16Array({57, 1})});
  SchemaAndBatch expected = TestBatch({Int64Array({1000, 1}), Int16Array({57, 1}),
                                       Int32Array({4, 1}), Int16Array({2, 1})});

  std::unique_ptr<Batch> actual = Batch::CreateBasic(&expected.schema);
  AssertOk(hashmap->Lookup(lookup_keys.batch.get(), actual.get()));

  ASSERT_TRUE(expected.batch->BinaryEquals(*actual));
}

TEST(HashMap, InnerJoin) {
  constexpr int64_t kEnoughBytesForScratch = 1024LL * 1024LL;
  std::unique_ptr<Storage> storage = TestStorage(kEnoughBytesForScratch);

  SchemaAndBatch build_keys =
      TestBatch({Int64Array({1, 2, 0, 1000, 0}), Int16Array({1, 100, 1000, 57, {}})});
  SchemaAndBatch build_payload =
      TestBatch({Int32Array({1, 2, 3, 4, 5}), Int16Array({1, 1, 1, 2, 2})});
  SchemaAndBatch probe_keys = TestBatch({Int64Array({1000, 1}), Int16Array({57, 1})});
  SchemaAndBatch probe_payload = TestBatch({Int16Array({80, 81})});
  SchemaAndBatch expected =
      TestBatch({Int64Array({1000, 1}), Int16Array({57, 1}), Int32Array({4, 1}),
                 Int16Array({2, 1}), Int16Array({80, 81})});

  util::LocalAllocator local_allocator;

  std::unique_ptr<HashMap> hashmap;
  AssertOk(CreateHashMap(&build_keys.schema, &build_payload.schema, &probe_payload.schema,
                         storage.get(), &hashmap));

  AssertOk(hashmap->Insert(build_keys.batch.get(), build_payload.batch.get()));

  std::vector<std::unique_ptr<ReadOnlyBatch>> actual_batches;
  AssertOk(hashmap->InnerJoin(probe_keys.batch.get(), probe_payload.batch.get(),
                              /*rows_per_batch=*/1_Mi,
                              [&](std::unique_ptr<ReadOnlyBatch> batch) {
                                actual_batches.push_back(std::move(batch));
                                return Status::OK();
                              }));
  ASSERT_EQ(1, actual_batches.size());
  ASSERT_TRUE(expected.batch->BinaryEquals(*actual_batches[0]));
}

}  // namespace quiver::map
