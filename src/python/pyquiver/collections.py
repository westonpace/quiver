import pyarrow as pa
import urllib.parse
from typing import Callable

from ._quiver import CHashMap, CAccumulator


DEFAULT_RAM_STORAGE_SIZE = 64 * 1024 * 1024


def _ensure_compatible_schema(
    item: pa.Table | pa.RecordBatch, desired_schema: pa.Schema
):
    given_schema = item.schema
    if len(given_schema) != len(desired_schema):
        raise ValueError(
            f"Schema mismatch: expected {len(desired_schema)} columns, got {len(given_schema)}"
        )
    for i in range(len(desired_schema)):
        if given_schema[i].type != desired_schema[i].type:
            raise ValueError(
                f"Schema mismatch: column {i} type mismatch: expected {desired_schema[i].type}, got {given_schema[i].type}"
            )


class HashMap(object):
    def __init__(
        self,
        key_schema: pa.Schema,
        build_payload_schema: pa.Schema,
        probe_payload_schema: pa.Schema | None = None,
        storage_descriptor: str = None,
    ):
        if storage_descriptor is None:
            scheme = "ram"
            path = "/"
            query_dict = {"size_bytes": str(DEFAULT_RAM_STORAGE_SIZE)}
        else:
            (scheme, _, path, _, query, _) = urllib.parse.urlparse(storage_descriptor)
            query_dict = urllib.parse.parse_qs(query)
            for key in query_dict.keys():
                current = query_dict[key]
                if len(current) > 1:
                    raise ValueError("Duplicate query parameter: " + key)
                query_dict[key] = current[0]
        self._map = CHashMap(
            key_schema,
            build_payload_schema,
            probe_payload_schema,
            scheme,
            path,
            query_dict,
        )
        self.key_schema = key_schema
        self.build_payload_schema = build_payload_schema
        self.probe_payload_schema = probe_payload_schema
        self.build_schema = pa.schema(list(key_schema) + list(build_payload_schema))
        self.probe_schema = pa.schema(list(key_schema) + list(probe_payload_schema))

    def insert(self, key_values: pa.RecordBatch | pa.Table) -> None:
        _ensure_compatible_schema(key_values, self.build_schema)
        self._map.insert(key_values)

    def __do_merge_batches(
        self, left: pa.RecordBatch, right: pa.RecordBatch
    ) -> pa.RecordBatch:
        cols = left.columns + right.columns
        fields = list(left.schema) + list(right.schema)
        return pa.RecordBatch.from_arrays(cols, schema=pa.schema(fields))

    def __do_merge_tables(self, left: pa.Table, right: pa.Table) -> pa.Table:
        cols = left.columns + right.columns
        fields = list(left.schema) + list(right.schema)
        return pa.Table.from_arrays(cols, schema=pa.schema(fields))

    def __merge_tables(
        self, left: pa.RecordBatch | pa.Table, right: pa.RecordBatch | pa.Table
    ) -> pa.RecordBatch | pa.Table:
        if isinstance(left, pa.RecordBatch):
            if isinstance(right, pa.RecordBatch):
                return self.__do_merge_batches(left, right)
            else:
                left_as_table = pa.Table.from_batches([left])
                return self.__do_merge_tables(left_as_table, right)
        else:
            if isinstance(right, pa.RecordBatch):
                right_as_table = pa.Table.from_batches([right])
                return self.__do_merge_tables(left, right_as_table)
            else:
                return self.__do_merge_tables(left, right)

    def insert2(
        self, keys: pa.RecordBatch | pa.Table, value: pa.RecordBatch | pa.Table
    ) -> None:
        _ensure_compatible_schema(keys, self.key_schema)
        _ensure_compatible_schema(value, self.build_payload_schema)
        key_value = self.__merge_tables(keys, value)
        return self.insert(key_value)

    def lookup(self, keys: pa.RecordBatch | pa.Table) -> None:
        return self._map.lookup(keys)

    def inner_join(
        self,
        key_values: pa.RecordBatch | pa.Table,
        callback: Callable[[pa.RecordBatch], None],
        rows_per_batch: int = -1,
    ) -> None:
        _ensure_compatible_schema(key_values, self.probe_schema)
        return self._map.inner_join(key_values, callback, rows_per_batch)

    def inner_join2(
        self,
        keys: pa.RecordBatch | pa.Table,
        values: pa.RecordBatch | pa.Table,
        callback: Callable[[pa.RecordBatch], None],
        rows_per_batch: int = -1,
    ) -> None:
        _ensure_compatible_schema(keys, self.key_schema)
        _ensure_compatible_schema(values, self.probe_payload_schema)
        key_value = self.__merge_tables(keys, values)
        return self.inner_join(key_value, callback, rows_per_batch)


class Accumulator(object):
    def __init__(
        self,
        schema: pa.Schema,
        rows_per_batch: int,
        callback: Callable[[pa.RecordBatch], None],
    ):
        self._accumulator = CAccumulator(schema, rows_per_batch, callback)
        self.schema = schema

    def insert(self, batch: pa.RecordBatch | pa.Table) -> None:
        _ensure_compatible_schema(batch, self.schema)
        self._accumulator.insert(batch)

    def finish(self) -> None:
        self._accumulator.finish()
