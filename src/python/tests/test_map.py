import pyarrow as pa

import pyquiver


def build_map():
    key_fields = [pa.field("key", pa.int64())]
    payload_fields = [pa.field("x", pa.int32()), pa.field("y", pa.int16())]
    keys_schema = pa.schema(key_fields)
    payload_schema = pa.schema(payload_fields)
    combined_schema = pa.schema(key_fields + payload_fields)

    map = pyquiver.HashMap(keys_schema, payload_schema)

    keys = pa.array([5, 4, 1, 2], pa.int64())
    x = pa.array([None, 1, 17, 0], pa.int32())
    y = pa.array([7, None, 12, 13], pa.int16())

    build_batch = pa.record_batch([keys, x, y], schema=combined_schema)

    map.insert(build_batch)
    return map, keys_schema, payload_schema, combined_schema


def test_lookup():
    map, keys_schema, _, combined_schema = build_map()

    subkeys = pa.array([1, 5], pa.int64())
    lookup_batch = pa.record_batch([subkeys], schema=keys_schema)

    retrieved = map.lookup(lookup_batch)

    expected = pa.record_batch(
        [
            pa.array([1, 5], pa.int64()),
            pa.array([17, None], pa.int32()),
            pa.array([12, 7], pa.int16()),
        ],
        schema=combined_schema,
    )

    assert expected.equals(retrieved)


def test_inner_join():
    map, keys_schema, payload_schema, combined_schema = build_map()

    subkeys = pa.array([1, 5], pa.int64())
    probe_x = pa.array([None, 200], pa.int32())
    probe_y = pa.array([300, None], pa.int16())
    probe_batch = pa.record_batch([subkeys, probe_x, probe_y], schema=combined_schema)

    batches_received = []

    def callback(batch):
        batches_received.append(batch)

    map.inner_join(probe_batch, callback)

    assert len(batches_received) == 1

    joined_schema = pa.schema(
        list(keys_schema) + list(payload_schema) + list(payload_schema)
    )
    expected = pa.record_batch(
        [
            pa.array([1, 5], pa.int64()),
            pa.array([17, None], pa.int32()),
            pa.array([12, 7], pa.int16()),
            pa.array([None, 200], pa.int32()),
            pa.array([300, None], pa.int16()),
        ],
        schema=joined_schema,
    )

    print(expected)
    print(batches_received[0])
    assert expected.equals(batches_received[0])
