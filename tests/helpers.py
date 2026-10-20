import random
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import (
    UNPARTITIONED_PARTITION_SPEC,
    PartitionField,
    PartitionSpec,
)
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import LongType, NestedField, StringType

SCHEMA = Schema(
    NestedField(1, "id", LongType()),
    NestedField(2, "category", StringType()),
    NestedField(3, "payload", StringType()),
)
BY_CATEGORY = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category"))


def create_table(catalog: Catalog, spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC) -> Table:
    return catalog.create_table("db.events", schema=SCHEMA, partition_spec=spec)


def append_files(table: Table, count: int = 1, rows: int = 100, category: str = "a", branch: str = "main") -> None:
    for index in range(count):
        rng = random.Random(index)
        start_id = index * rows
        table.append(
            pa.Table.from_pylist(
                [{"id": i, "category": category, "payload": rng.randbytes(16).hex()} for i in range(start_id, start_id + rows)],
                schema=table.schema().as_arrow(),
            ),
            branch=branch,
        )


def list_live_data_files(table: Table) -> list[DataFile]:
    return [
        entry.data_file
        for manifest in table.current_snapshot().manifests(table.io)
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True)
        if entry.data_file.content == DataFileContent.DATA
    ]


def write_file(table: Table, content: DataFileContent, rows: pa.Table, partition: Record, equality_ids=None) -> DataFile:
    path = f"{table.location()}/data/{content.name.lower()}-{uuid.uuid4()}.parquet"
    pq.write_table(rows, path.removeprefix("file://"))
    return DataFile.from_args(
        content=content,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=partition,
        record_count=rows.num_rows,
        file_size_in_bytes=len(table.io.new_input(path)),
        spec_id=table.spec().spec_id,
        equality_ids=equality_ids,
    )


def commit_files(table: Table, *files: DataFile) -> None:
    with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
        for file in files:
            append.append_data_file(file)


def add_delete_file(table: Table, content: DataFileContent, rows: pa.Table, partition: Record, equality_ids=None) -> None:
    commit_files(table, write_file(table, content, rows, partition, equality_ids))


def add_position_deletes(table: Table, data_file: DataFile, positions: list[int]) -> None:
    schema = Schema(
        NestedField(2147483546, "file_path", StringType(), required=True),
        NestedField(2147483545, "pos", LongType(), required=True),
    )
    rows = pa.Table.from_pydict(
        {"file_path": [data_file.file_path] * len(positions), "pos": positions}, schema=schema_to_pyarrow(schema)
    )
    add_delete_file(table, DataFileContent.POSITION_DELETES, rows, data_file.partition)


def add_equality_deletes(table: Table, column: str, values: list, partition: Record | None = None) -> None:
    key_field = table.schema().find_field(column)
    rows = pa.Table.from_pydict({column: values}, schema=schema_to_pyarrow(Schema(key_field)))
    add_delete_file(table, DataFileContent.EQUALITY_DELETES, rows, partition or Record(), equality_ids=[key_field.field_id])


def upsert_row(table: Table, row: dict) -> None:
    """Replace the row with the same id: a new data file and an equality delete on id, in one commit."""
    key_field = table.schema().find_field("id")
    data_file = write_file(table, DataFileContent.DATA, pa.Table.from_pylist([row], schema=table.schema().as_arrow()), Record())
    delete_rows = pa.Table.from_pydict({"id": [row["id"]]}, schema=schema_to_pyarrow(Schema(key_field)))
    delete_file = write_file(table, DataFileContent.EQUALITY_DELETES, delete_rows, Record(), equality_ids=[key_field.field_id])
    commit_files(table, data_file, delete_file)
