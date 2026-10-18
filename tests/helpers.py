import random

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import LongType, NestedField, StringType

SCHEMA = Schema(
    NestedField(1, "id", LongType()),
    NestedField(2, "payload", StringType()),
)


def create_table(catalog: Catalog) -> Table:
    return catalog.create_table("db.events", schema=SCHEMA)


def append_files(table: Table, count: int = 1, rows: int = 100) -> None:
    for index in range(count):
        rng = random.Random(index)
        start_id = index * rows
        table.append(
            pa.Table.from_pylist(
                [{"id": i, "payload": rng.randbytes(16).hex()} for i in range(start_id, start_id + rows)],
                schema=table.schema().as_arrow(),
            )
        )
