import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from helpers import append_files, create_table
from pyiceberg.types import StringType

from iceberg_compaction import Execution, Planner
from iceberg_compaction.engines import DuckDBEngine


@pytest.fixture
def engine():
    with DuckDBEngine() as engine:
        yield engine


def read_rows(result):
    io = result.plan.table.io
    return pa.concat_tables(
        pq.read_table(io.new_input(data_file.file_path).open()) for task in result.tasks for data_file in task.added_files
    )


def sort_rows(rows):
    return sorted(rows.to_pylist(), key=lambda row: json.dumps(row, sort_keys=True, default=str))


def test_small_files_are_merged_into_one_file(catalog, engine):
    table = create_table(catalog)
    append_files(table, 2)
    rows_before = table.scan().to_arrow()

    result = Execution(engine).execute(Planner(target_file_size_mib=1).plan(table))

    assert [len(task.added_files) for task in result.tasks] == [1]
    assert sort_rows(read_rows(result)) == sort_rows(rows_before)


def test_renamed_and_added_columns_are_read_by_field_id(catalog, engine):
    table = create_table(catalog)
    append_files(table)
    with table.update_schema() as update:
        update.rename_column("payload", "body")
        update.add_column("extra", StringType())
    table.append(pa.Table.from_pylist([{"id": 100, "category": "a", "extra": "x"}], schema=table.schema().as_arrow()))
    rows_before = table.scan().to_arrow()

    result = Execution(engine).execute(Planner(target_file_size_mib=1).plan(table))

    assert sort_rows(read_rows(result)) == sort_rows(rows_before)
