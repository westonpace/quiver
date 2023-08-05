import pyarrow as pa
import urllib.parse
from typing import Callable, Union

from ._quiver import CHashMap, CAccumulator


DEFAULT_RAM_STORAGE_SIZE = 64 * 1024 * 1024


def _ensure_compatible_schema(
    item: Union[pa.Table, pa.RecordBatch], desired_schema: pa.Schema
) -> None:
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
    """
    A HashMap is very similar to a python dict/set/multimap except that it is designed to operate
    on batches of data instead of individual items and it may be backed by disk instead
    of memory.

    The HashMap can serve as both a map (hashmap) and a set (hashset).  If you would like
    to use the HashMap as a set then set the probe payload schema to None.

    The HashMap currently requires that the schema of the keys, build payload, and probe
    payload be the same for all batches.

    The HashMap is append-only.  Once a batch is inserted it cannot be removed.

    The HashMap is thread safe.  It supports either "concurrent insertions" or
    "concurrent lookups" but does not support combining the two operations
    concurrently (yet).

    The HashMap releases the GIL during insertions and lookups and using threading is
    required to achieve maximum performance.

    Currently the hashmap only suports the flat arrow types.  Support is planned, but not
    yet implemented for:

      * String types
      * Nested types
      * List types
      * Dictionary types
      * RLE
      * Other (we aim to support all types eventually)
    """

    def __init__(
        self,
        key_schema: pa.Schema,
        build_payload_schema: pa.Schema,
        probe_payload_schema: Union[pa.Schema, None] = None,
        storage_descriptor: str = None,
    ):
        """
        Creates a HashMap

        The key_schema is the schema for the key columns.  For a map/join the key columns
        must be identical between the build side and the probe side.

        The build payload schema represents the columns on the insertion side that are not
        key columns.

        The probe payload schema represents the columns on the lookup side that are not
        key columns.
        """
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

    def insert(self, key_values: Union[pa.RecordBatch, pa.Table]) -> None:
        """
        Inserts a batch of data into the map.

        The batch must contain both key columns and payload columns, in that order.

        Parameters
        ----------
        key_values : pa.RecordBatch or pa.Table
            A batch of data to insert into the map.  Must consist of key columns
            followed by build payload columns

        Returns
        -------
        None
        """
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
        self,
        left: Union[pa.RecordBatch, pa.Table],
        right: Union[pa.RecordBatch, pa.Table],
    ) -> Union[pa.RecordBatch, pa.Table]:
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
        self,
        keys: Union[pa.RecordBatch, pa.Table],
        value: Union[pa.RecordBatch, pa.Table],
    ) -> None:
        """
        Inserts a batch of data into the map.

        This method is similar to `insert` but allows the key columns and payload
        columns to be separate batches for convenience.  The performance should be
        the same.

        Parameters
        ----------
        keys : pa.RecordBatch or pa.Table
            A batch of key columns to insert into the map.
            The schema should match the key schema of the map.
        value : pa.RecordBatch or pa.Table
            A batch of payload columns to insert into the map.
            The schema should match the build payload schema of the map.

        Returns
        -------
        None
        """
        _ensure_compatible_schema(keys, self.key_schema)
        _ensure_compatible_schema(value, self.build_payload_schema)
        key_value = self.__merge_tables(keys, value)
        return self.insert(key_value)

    def lookup(self, keys: Union[pa.RecordBatch, pa.Table]) -> pa.RecordBatch:
        """
        Fetches a batch of data that has previously been inserted into the map.

        If a row has multiple matches in the map then an output row will be generated
        for each match.

        Parameters
        ----------
        keys : pa.RecordBatch or pa.Table
            A batch of key columns to lookup in the map.
            The schema should match the key schema of the map.

        Returns
        -------
        pa.RecordBatch
            A batch of data that was previously inserted into the map.
            The schema will be the combined key schema and build payload
            schema.
        """
        return self._map.lookup(keys)

    def inner_join(
        self,
        key_values: Union[pa.RecordBatch, pa.Table],
        callback: Callable[[pa.RecordBatch], None],
        rows_per_batch: int = -1,
    ) -> pa.RecordBatch:
        """
        Joins a batch of input data with a batch of data in the map.

        If a row has multiple matches in the map then an output row will
        be returned for each match.

        If a row has no match in the map then there will be no corresponding
        output rows.

        Returned batches will have the same key order as the input.  If there
        are multiple matches to the key then there is no defined order for the
        rows that have the same key.

        Parameters
        ----------
        key_values : pa.RecordBatch or pa.Table
            A batch of key columns to lookup in the map.
            The schema should be the key schema followed by the probe
            payload schema.
        callback : callable
            A function that will be called with each batch of joined data.
        rows_per_batch : int, optional
            The maximum number of rows to include in each batch.  If this is
            less than zero then a suitable default will be chosen.

        Returns
        -------
        pa.RecordBatch
            A batch representing the input data joined with the data in the map.
            The schema will be the combined key schema, probe payload schema,
            and build payload schema.
        """
        _ensure_compatible_schema(key_values, self.probe_schema)
        return self._map.inner_join(key_values, callback, rows_per_batch)

    def inner_join2(
        self,
        keys: Union[pa.RecordBatch, pa.Table],
        values: Union[pa.RecordBatch, pa.Table],
        callback: Callable[[pa.RecordBatch], None],
        rows_per_batch: int = -1,
    ) -> pa.RecordBatch:
        """
        Joins a batch of input data with a batch of data in the map.

        This is a convenience function that has the same behavior as inner_join but
        allows for the keys and values to be specified as two separate batches.  It
        has no other advantage over inner_join.

        Parameters
        ----------
        keys : pa.RecordBatch or pa.Table
            A batch of key columns to lookup in the map.
            The schema should match the key schema of the map.
        values : pa.RecordBatch or pa.Table
            A batch of payload columns to lookup in the map.
            The schema should match the probe payload schema of the map.
        callback : callable
            A function that will be called with each batch of joined data.
        rows_per_batch : int, optional
            The maximum number of rows to include in each batch.  If this is
            less than zero then a suitable default will be chosen.

        Returns
        -------
        pa.RecordBatch
            A batch representing the input data joined with the data in the map.
            The schema will be the combined key schema, probe payload schema,
            and build payload schema.
        """
        _ensure_compatible_schema(keys, self.key_schema)
        _ensure_compatible_schema(values, self.probe_payload_schema)
        key_value = self.__merge_tables(keys, values)
        return self.inner_join(key_value, callback, rows_per_batch)


class Accumulator(object):
    """
    An accumulator accumulates smaller batches into larger batches.

    In many cases we want to work with large batches to get the best performance.
    However, we may not be able to control the size of the batches that we receive.
    The Accumulator class allows us to solve this problem by efficiently accumulating
    smaller batches into larger batches.
    """

    def __init__(
        self,
        schema: pa.Schema,
        rows_per_batch: int,
        callback: Callable[[pa.RecordBatch], None],
    ):
        """
        Constructs an Accumulator.

        Parameters
        ----------
        schema : pa.Schema
            The schema of the batches that will be accumulated.
        rows_per_batch : int
            The number of rows to include in each output batch.
        callback : callable
            A function that will be called with each complete batch of accumulated data.
        """
        self._accumulator = CAccumulator(schema, rows_per_batch, callback)
        self.schema = schema

    def insert(self, batch: Union[pa.RecordBatch, pa.Table]) -> None:
        """
        Inserts a batch of data into the accumulator.

        This call may trigger one or more calls to the callback function.
        """
        _ensure_compatible_schema(batch, self.schema)
        self._accumulator.insert(batch)

    def finish(self) -> None:
        """
        Indicates that no more data will be inserted into the accumulator.

        If there are any rows remaining in the accumulator then they will be
        flushed to the callback function as part of this call.

        The accumulator should not be used any further after this call.
        """
        self._accumulator.finish()
