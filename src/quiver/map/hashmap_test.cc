#include "quiver/map/hashmap.h"

#include <gtest/gtest.h>

#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/hashtable/hashtable.h"
#include "quiver/row/row_p.h"
#include "quiver/sort/sort.h"
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
  AssertOk(CreateHashMap(keys.schema.get(), payload.schema.get(),
                         /*probe_payload_schema=*/nullptr, storage.get(), &hashmap));

  AssertOk(hashmap->Insert(keys.batch.get(), payload.batch.get()));

  SchemaAndBatch lookup_keys = TestBatch({Int64Array({1000, 1}), Int16Array({57, 1})});
  SchemaAndBatch expected = TestBatch({Int64Array({1000, 1}), Int16Array({57, 1}),
                                       Int32Array({4, 1}), Int16Array({2, 1})});

  std::unique_ptr<Batch> actual = Batch::CreateBasic(expected.schema.get());
  AssertOk(hashmap->Lookup(lookup_keys.batch.get(), actual.get()));

  ASSERT_TRUE(expected.batch->BinaryEquals(*actual));
}

void CheckInnerJoin(const SchemaAndBatch& build_keys, const SchemaAndBatch& build_payload,
                    const SchemaAndBatch& probe_keys, const SchemaAndBatch& probe_payload,
                    const SchemaAndBatch& expected) {
  std::unique_ptr<Storage> storage = TestStorage();
  util::LocalAllocator local_allocator;

  std::unique_ptr<HashMap> hashmap;
  AssertOk(CreateHashMap(build_keys.schema.get(), build_payload.schema.get(),
                         probe_payload.schema.get(), storage.get(), &hashmap));

  AssertOk(hashmap->Insert(build_keys.batch.get(), build_payload.batch.get()));

  std::vector<std::unique_ptr<ReadOnlyBatch>> actual_batches;
  AssertOk(hashmap->InnerJoin(probe_keys.batch.get(), probe_payload.batch.get(),
                              /*rows_per_batch=*/1_Mi,
                              [&](std::unique_ptr<ReadOnlyBatch> batch) {
                                actual_batches.push_back(std::move(batch));
                                return Status::OK();
                              }));
  ASSERT_EQ(1, actual_batches.size());

  std::unique_ptr<ReadOnlyBatch> actual = std::move(actual_batches[0]);

  std::unique_ptr<sort::AllColumnsBinarySorter> sorter;
  AssertOk(sort::AllColumnsBinarySorter::Create(storage.get(), &sorter));

  std::unique_ptr<Batch> expected_sorted = Batch::CreateBasic(expected.schema.get());
  AssertOk(sorter->Sort(*expected.batch, expected_sorted.get()));

  std::unique_ptr<Batch> actual_sorted = Batch::CreateBasic(actual->schema());
  AssertOk(sorter->Sort(*actual, actual_sorted.get()));

  ASSERT_TRUE(expected_sorted->BinaryEquals(*actual_sorted));
}

TEST(HashMap, InnerJoin) {
  SchemaAndBatch build_keys =
      TestBatch({Int64Array({1, 2, 0, 1000, 0}), Int16Array({1, 100, 1000, 57, {}})});
  SchemaAndBatch build_payload =
      TestBatch({Int32Array({1, 2, 3, 4, 5}), Int16Array({1, 1, 1, 2, 2})});
  SchemaAndBatch probe_keys = TestBatch({Int64Array({1000, 1}), Int16Array({57, 1})});
  SchemaAndBatch probe_payload = TestBatch({Int16Array({80, 81})});
  SchemaAndBatch expected =
      TestBatch({Int64Array({1000, 1}), Int16Array({57, 1}), Int32Array({4, 1}),
                 Int16Array({2, 1}), Int16Array({80, 81})});

  CheckInnerJoin(build_keys, build_payload, probe_keys, probe_payload, expected);

  build_keys = TestBatch({Int64Array({1, 2, 2})});
  build_payload = TestBatch({Int32Array({1, 2, 3}), Int16Array({10, 20, 30})});
  probe_keys = TestBatch({Int64Array({2, 1, 3})});
  probe_payload = TestBatch({Int16Array({-1, -2, -3})});
  expected = TestBatch({Int64Array({2, 2, 1}), Int32Array({2, 3, 1}),
                        Int16Array({20, 30, 10}), Int16Array({-1, -1, -2})});

  CheckInnerJoin(build_keys, build_payload, probe_keys, probe_payload, expected);
}

}  // namespace quiver::map
