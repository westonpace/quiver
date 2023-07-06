import math
import numpy as np
import polars
import pyarrow as pa
import pyquiver
import pytest

### Data shape ###
# Number of columns to use as keys
NUM_KEYS = [1, 2, 4, 8]
# The width of each row, in bytes
ROW_WIDTH_BYTES = [64, 4096, 16384]
# Note, the actual fields will be randomly generated and have a width
# of 1, 2, 4, or 8 bytes each.  The total number of fields is thus random
# but we know there will be at least ROW_WIDTH_BYTES / 8 fields.  So the
# largest value of NUM_KEYS should be less than or equal to the smallest
# value of ROW_WIDTH_BYTES divided by 8.
if (max(NUM_KEYS) * 8) > min(ROW_WIDTH_BYTES):
    raise Exception("NUM_KEYS is too large for ROW_WIDTH_BYTES")

### Data size ###
# The size of the build table, in bytes
BUILD_SIZE_BYTES = [1 * 1024 * 1024]
# The size of the probe table, in bytes
PROBE_SIZE_BYTES = [4 * 1024 * 1024]

### Join parameters
# Percentage of probe rows that match a build row
OVERLAP = [1.0, 0.8, 0.6]

### Strategy ###
# The hash is pre-computed (e.g. no hashing in the benchmark)
HASH_STRATEGY_IDENTITY = "IDENTITY"
HASH_STRATEGIES = [HASH_STRATEGY_IDENTITY]
# std::unordered_multimap is used
MAP_STRATEGY_STL = "STL"
MAP_STRATEGIES = [MAP_STRATEGY_STL]
# Rows are stored in RAM
STORAGE_STRATEGY_RAM = "RAM"
# Rows are stored on disk
STORAGE_STRATEGY_DISK = "DISK"
STORAGE_STRATEGIES = [STORAGE_STRATEGY_RAM, STORAGE_STRATEGY_DISK]

scenarios = []
for num_keys in NUM_KEYS:
    for rwb in ROW_WIDTH_BYTES:
        for bsb in BUILD_SIZE_BYTES:
            for psb in PROBE_SIZE_BYTES:
                for hs in HASH_STRATEGIES:
                    for overlap in OVERLAP:
                        for ms in MAP_STRATEGIES:
                            for ss in STORAGE_STRATEGIES:
                                scenarios.append(
                                    (
                                        num_keys,
                                        rwb,
                                        bsb,
                                        psb,
                                        hs,
                                        overlap,
                                        ms,
                                        ss,
                                    )
                                )

RNG = np.random.default_rng(seed=42)


def generate_combined_schema(row_width_bytes, num_keys):
    bytes_remaining = row_width_bytes
    fields = []
    while bytes_remaining > 0:
        field_width_bytes = RNG.choice([1, 2, 4, 8])
        if field_width_bytes > bytes_remaining:
            field_width_bytes = bytes_remaining
        bytes_remaining -= field_width_bytes
        if field_width_bytes == 1:
            field_type = pa.int8()
        elif field_width_bytes == 2:
            field_type = pa.int16()
        elif field_width_bytes == 4:
            field_type = pa.int32()
        elif field_width_bytes == 8:
            field_type = pa.int64()
        else:
            raise Exception("Invalid field width ${field_width_bytes}")
        if len(fields) < num_keys:
            name = f"key{len(fields)}"
        else:
            name = f"payload{len(fields)}"
        fields.append(pa.field(name, field_type))
    return pa.schema(fields)


def generate_key_schema(combined_schema, num_keys):
    fields = list(combined_schema)[:num_keys]
    return pa.schema(fields)


def generate_payload_schema(combined_schema, num_keys):
    fields = list(combined_schema)[num_keys:]
    return pa.schema(fields)


def generate_random_table(schema, num_rows):
    arrays = []
    for field in schema:
        if field.type == pa.int8():
            array = pa.array(
                RNG.integers(-1 * (2**7), 2**7 - 1, num_rows, dtype=np.int8)
            )
        elif field.type == pa.int16():
            array = pa.array(
                RNG.integers(-1 * (2**15), 2**15 - 1, num_rows, dtype=np.int16)
            )
        elif field.type == pa.int32():
            array = pa.array(
                RNG.integers(-1 * (2**31), 2**31 - 1, num_rows, dtype=np.int32)
            )
        elif field.type == pa.int64():
            array = pa.array(
                RNG.integers(-1 * (2**63), 2**63 - 1, num_rows, dtype=np.int64)
            )
        arrays.append(array)
    return pa.Table.from_arrays(arrays, schema=schema)


def sample_build_keys(build_data, key_schema, num_keys):
    indices = RNG.choice(len(build_data), num_keys)
    return build_data.select(key_schema.names).take(indices)


def all_columns_from(keys, payload):
    field_names = keys.schema.names + payload.schema.names
    columns = keys.columns + payload.columns
    return pa.Table.from_arrays(columns, names=field_names)


def shuffle_table(table):
    indices = np.arange(len(table))
    RNG.shuffle(indices)
    return table.take(indices)


# Most rows in the probe data (% determined by overlap) will match a row in the build data.  This means
# the value of the key columns will be the same.  The payload columns will still be random.
def generate_probe_data(
    combined_schema,
    key_schema,
    payload_schema,
    build_data,
    overlap,
    probe_size_bytes,
    row_width_bytes,
):
    num_probe_rows = probe_size_bytes // row_width_bytes
    num_overlap_rows = math.floor(num_probe_rows * overlap)

    print(
        f"Generating {num_probe_rows} rows of probe data with {num_overlap_rows} overlap"
    )

    overlap_keys = sample_build_keys(build_data, key_schema, num_overlap_rows)
    overlap_payload = generate_random_table(payload_schema, num_overlap_rows)
    overlap_data = all_columns_from(overlap_keys, overlap_payload)

    non_overlap_data = generate_random_table(
        combined_schema, num_probe_rows - num_overlap_rows
    )
    probe_data = pa.concat_tables([overlap_data, non_overlap_data])
    return shuffle_table(probe_data)


def generate_build_data(combined_schema, build_size_bytes, row_width_bytes):
    num_build_rows = build_size_bytes // row_width_bytes
    if num_build_rows <= 0:
        raise Exception("Invalid build_size_bytes or row_width_bytes")
    print(f"Generating {num_build_rows} rows of build data")
    return generate_random_table(combined_schema, num_build_rows)


def create_hashmap(key_schema, payload_schema):
    return pyquiver.HashMap(key_schema, payload_schema)


def insert_hashes(table, key_schema):
    key_table = table.select(key_schema.names)
    key_table_pl = polars.from_arrow(key_table)
    hashes = key_table_pl.hash_rows().to_arrow()
    return table.add_column(0, "hash", hashes)


def simulate_inner_join(hashmap, build_data, probe_data):
    hashmap.insert(build_data)

    def consume(batch):
        print(
            f"Join returned batch with {batch.num_rows} rows and {batch.num_columns} columns"
        )

    return hashmap.inner_join(probe_data, consume)


@pytest.mark.parametrize(
    "num_keys,row_width_bytes,build_size_bytes,probe_size_bytes,hash_strategy,overlap,map_strategy,storage_strategy",
    scenarios,
)
def test_join_scenario(
    num_keys,
    row_width_bytes,
    build_size_bytes,
    probe_size_bytes,
    hash_strategy,
    overlap,
    map_strategy,
    storage_strategy,
    benchmark,
):
    combined_schema = generate_combined_schema(row_width_bytes, num_keys)
    key_schema = generate_key_schema(combined_schema, num_keys)
    payload_schema = generate_payload_schema(combined_schema, num_keys)
    build_data = generate_build_data(combined_schema, build_size_bytes, row_width_bytes)
    probe_data = generate_probe_data(
        combined_schema,
        key_schema,
        payload_schema,
        build_data,
        overlap,
        probe_size_bytes,
        row_width_bytes,
    )
    if hash_strategy == HASH_STRATEGY_IDENTITY:
        print("Calculating hashes")
        build_data = insert_hashes(build_data, key_schema)
        probe_data = insert_hashes(probe_data, key_schema)
        key_schema = key_schema.insert(0, pa.field("hash", pa.int64()))

    hashmap = create_hashmap(key_schema, payload_schema)
    args = [hashmap, build_data, probe_data]
    print("Hashing")
    benchmark.pedantic(
        simulate_inner_join, args=args, rounds=3, iterations=1, warmup_rounds=1
    )


class MockBenchmark(object):
    def pedantic(self, func, args, rounds, iterations, warmup_rounds):
        func(*args)


for scenario in scenarios[:1]:
    print(f"Testing scenario {scenario}")
    test_join_scenario(*scenario, benchmark=MockBenchmark())