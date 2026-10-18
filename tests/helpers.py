import random

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import (
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

SCHEMA = Schema(
    NestedField(1, "id", LongType()),
    NestedField(2, "category", StringType()),
    NestedField(3, "payload", StringType()),
)
BY_CATEGORY = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category"))


def create_table(catalog: Catalog, spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC) -> Table:
    return catalog.create_table("db.events", schema=SCHEMA, partition_spec=spec)


def append_files(table: Table, count: int = 1, rows: int = 100, category: str = "a") -> None:
    for index in range(count):
        rng = random.Random(index)
        start_id = index * rows
        table.append(
            pa.Table.from_pylist(
                [{"id": i, "category": category, "payload": rng.randbytes(16).hex()} for i in range(start_id, start_id + rows)],
                schema=table.schema().as_arrow(),
            )
        )
