import pyarrow as pa

from pyquiver.collections import HashMap
import pyquiver

pyquiver.set_log_level("trace")


class Schemas(object):
    def __init__(self):
        key_fields = [pa.field("key", pa.int64())]
        build_payload_fields = [pa.field("x", pa.int32()), pa.field("y", pa.int16())]
        probe_payload_fields = [pa.field("z", pa.int8())]
        self.keys = pa.schema(key_fields)
        self.build_payload = pa.schema(build_payload_fields)
        self.probe_payload = pa.schema(probe_payload_fields)
        self.build = pa.schema(key_fields + build_payload_fields)
        self.probe = pa.schema(key_fields + probe_payload_fields)
        self.join = pa.schema(list(self.build) + probe_payload_fields)


def build_empty_map():
    schemas = Schemas()

    map = HashMap(schemas.keys, schemas.build_payload, schemas.probe_payload)
    return map


def build_basic_map():
    schemas = Schemas()

    map = HashMap(schemas.keys, schemas.build_payload, schemas.probe_payload)

    keys = pa.array([5, 4, 1, 2], pa.int64())
    x = pa.array([None, 1, 17, 0], pa.int32())
    y = pa.array([7, None, 12, 13], pa.int16())

    build_batch = pa.record_batch([keys, x, y], schema=schemas.build)

    map.insert(build_batch)
    return map


def test_lookup():
    map = build_basic_map()
    schemas = Schemas()

    subkeys = pa.array([1, 5], pa.int64())
    lookup_batch = pa.record_batch([subkeys], schema=schemas.keys)

    retrieved = map.lookup(lookup_batch)

    expected = pa.record_batch(
        [
            pa.array([1, 5], pa.int64()),
            pa.array([17, None], pa.int32()),
            pa.array([12, 7], pa.int16()),
        ],
        schema=schemas.build,
    )

    assert expected.equals(retrieved)


def test_inner_join():
    map = build_basic_map()
    schemas = Schemas()

    subkeys = pa.array([1, 5], pa.int64())
    probe_z = pa.array([100, None], pa.int8())
    probe_batch = pa.record_batch([subkeys, probe_z], schema=schemas.probe)

    batches_received = []

    def callback(batch):
        batches_received.append(batch)

    map.inner_join(probe_batch, callback)

    assert len(batches_received) == 1

    expected = pa.record_batch(
        [
            pa.array([1, 5], pa.int64()),
            pa.array([17, None], pa.int32()),
            pa.array([12, 7], pa.int16()),
            pa.array([100, None], pa.int8()),
        ],
        schema=schemas.join,
    )

    print(expected)
    print(batches_received[0])
    assert expected.equals(batches_received[0])


def test_inner_join_example():
    map = build_empty_map()
    schemas = Schemas()

    build = pa.Table.from_pydict(
        {"key": [1, 2, 2], "x": [1, 2, 3], "y": [10, 20, 30]}, schema=schemas.build
    )

    probe = pa.Table.from_pydict(
        {"key": [2, 1, 3], "z": [-1, -2, -3]}, schema=schemas.probe
    )

    batches_received = []

    def callback(batch):
        batches_received.append(batch)

    map.insert(build)
    map.inner_join(probe, callback)

    assert len(batches_received) == 1
    expected = pa.RecordBatch.from_pydict(
        {"key": [2, 2, 1], "x": [2, 3, 1], "y": [20, 30, 10], "z": [-1, -1, -2]},
        schema=schemas.join,
    )
    print(batches_received[0].column(2))
    print(expected.column(2))
    # TODO: Expose normalizing batches and uncomment this
    # assert batches_received[0] == expected
