import pyarrow as pa

from pyquiver.collections import Accumulator


def test_basic():
    schema = pa.schema([pa.field("x", pa.int32()), pa.field("y", pa.int32())])
    accumulated_batches = []

    def callback(batch):
        accumulated_batches.append(batch)

    accumulator = Accumulator(schema, 5000, callback)

    data = [1, None, 2, None, 3, 4, None, 5, 6, 7]

    x = pa.array(data, pa.int32())
    batch = pa.record_batch([x, x], schema=schema)

    for _ in range(1200):
        accumulator.insert(batch)
    accumulator.finish()

    assert len(accumulated_batches) == 3
    assert accumulated_batches[0].num_rows == 5000
    assert accumulated_batches[1].num_rows == 5000
    assert accumulated_batches[2].num_rows == 2000

    assert accumulated_batches[0].column(0).to_pylist() == (data * 500)
    assert accumulated_batches[1].column(0).to_pylist() == (data * 500)
    assert accumulated_batches[0].column(1).to_pylist() == (data * 500)
    assert accumulated_batches[1].column(1).to_pylist() == (data * 500)
    assert accumulated_batches[2].column(0).to_pylist() == (data * 200)
    assert accumulated_batches[2].column(1).to_pylist() == (data * 200)
