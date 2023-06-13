import pyarrow as pa

import pyquiver


def test_basic():
    key_fields = [pa.field("key", pa.int64())]
    payload_fields = [pa.field("x", pa.int32()), pa.field("y", pa.int16())]
    keys_schema = pa.schema(key_fields)
    payload_schema = pa.schema(payload_fields)
    combined_schema = pa.schema(key_fields + payload_fields)

    map = pyquiver.HashMap(keys_schema, payload_schema)

    keys = pa.array([5, 4, 1, 2], pa.int64())
    x = pa.array([None, 1, 17, 0], pa.int32())
    y = pa.array([7, None, 12, 13], pa.int16())

    build_keys = pa.record_batch([keys], schema=keys_schema)
    build_values = pa.record_batch([x, y], schema=payload_schema)

    map.insert(build_keys, build_values)

    subkeys = pa.array([1, 5], pa.int64())
    probe_keys = pa.record_batch([subkeys], schema=keys_schema)

    retrieved = map.lookup(probe_keys)

    expected = pa.record_batch(
        [
            pa.array([1, 5], pa.int64()),
            pa.array([17, None], pa.int32()),
            pa.array([12, 7], pa.int16()),
        ],
        schema=combined_schema,
    )

    assert expected.equals(retrieved)
