#include "quiver/map/hashmap.h"

#include <gtest/gtest.h>

#include "quiver/core/array.h"
#include "quiver/core/io.h"
#include "quiver/hashtable/hashtable.h"
#include "quiver/row/row_p.h"
#include "quiver/testutil/test_util.h"
#include "quiver/util/local_allocator_p.h"

namespace quiver::map {

Status CreateHashMap(const SimpleSchema* key_schema, const SimpleSchema* payload_schema,
                     StreamSink* sink, RandomAccessSource* source,
                     std::unique_ptr<HashMap>* out) {
  std::unique_ptr<hash::Hasher> hasher = hash::Hasher::CreateIdentityHasher();
  std::unique_ptr<hashtable::HashTable> hashtable = hashtable::HashTable::MakeStl();
  return HashMap::Create(key_schema, payload_schema, std::move(hasher), sink, source,
                         std::move(hashtable), out);
}

TEST(HashMap, Basic) {
  constexpr int64_t kEnoughBytesForScratch = 1024LL * 1024LL;
  std::vector<uint8_t> scratch_buffer(kEnoughBytesForScratch);
  std::span<uint8_t> scratch{scratch_buffer.data(), scratch_buffer.size()};
  StreamSink sink = StreamSink::FromFixedSizeSpan(scratch);
  std::unique_ptr<RandomAccessSource> source = RandomAccessSource::FromSpan(scratch);

  SchemaAndBatch keys =
      TestBatch({Int64Array({1, 2, 0, 1000, 0}), Int16Array({1, 100, 1000, 57, {}})});
  SchemaAndBatch payload =
      TestBatch({Int32Array({1, 2, 3, 4, 5}), Int16Array({1, 1, 1, 2, 2})});

  util::LocalAllocator local_allocator;

  std::unique_ptr<HashMap> hashmap;
  AssertOk(CreateHashMap(&keys.schema, &payload.schema, &sink, source.get(), &hashmap));

  AssertOk(hashmap->Insert(keys.batch.get(), payload.batch.get()));

  SchemaAndBatch lookup_keys = TestBatch({Int64Array({1000, 1}), Int16Array({57, 1})});
  SchemaAndBatch expected = TestBatch({Int64Array({1000, 1}), Int16Array({57, 1}),
                                       Int32Array({4, 1}), Int16Array({2, 1})});

  std::unique_ptr<Batch> actual = Batch::CreateBasic(&expected.schema);
  AssertOk(hashmap->Lookup(lookup_keys.batch.get(), actual.get()));

  ASSERT_TRUE(expected.batch->BinaryEquals(*actual));
}

}  // namespace quiver::map
