import json
from contextlib import contextmanager

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from helpers import add_position_deletes, append_files, create_table, list_live_data_files, upsert_row
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import StringType

from iceberg_compaction import CompactionEngine, CompactionMode, Execution, Planner, RowCountMismatchError, Strategy
from iceberg_compaction.engines.duckdb_engine import DuckDBEngine
from iceberg_compaction.execution import compute_batch_rows


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


def test_oversized_file_is_split_near_the_target_size(catalog, engine):
    table = create_table(catalog)
    append_files(table, rows=100_000)

    result = Execution(engine).execute(Planner(compaction_mode=CompactionMode.SPLIT, target_file_size_mib=1).plan(table))

    (rewritten_task,) = result.tasks
    sizes = sorted(data_file.file_size_in_bytes for data_file in rewritten_task.added_files)
    assert len(sizes) == rewritten_task.task.estimated_output_files == 3
    assert all(abs(size - 1024**2) < 0.03 * 1024**2 for size in sizes[1:])


def test_position_deletes_are_applied(catalog, engine):
    table = create_table(catalog)
    append_files(table)
    add_position_deletes(table, list_live_data_files(table)[0], [0, 5])

    result = Execution(engine).execute(Planner(strategy=Strategy.FILES_WITH_DELETES).plan(table))

    assert sorted(read_rows(result)["id"].to_pylist()) == [i for i in range(100) if i not in (0, 5)]


def test_equality_delete_removes_older_rows_but_not_the_row_written_with_it(catalog, engine):
    table = create_table(catalog)
    append_files(table)
    upsert_row(table, {"id": 1, "category": "a", "payload": "new"})

    result = Execution(engine).execute(Planner(strategy=Strategy.AUTO, target_file_size_mib=1).plan(table))

    rows = [row for row in read_rows(result).to_pylist() if row["id"] == 1]
    assert (read_rows(result).num_rows, [row["payload"] for row in rows]) == (100, ["new"])


def test_rows_follow_the_table_sort_order(catalog, engine):
    table = create_table(catalog)
    with table.update_sort_order() as update:
        update.desc("id", IdentityTransform())
    append_files(table, 2, rows=50)

    result = Execution(engine).execute(Planner(target_file_size_mib=1).plan(table))

    ids = read_rows(result)["id"].to_pylist()
    assert ids == sorted(ids, reverse=True)
    assert result.tasks[0].added_files[0].sort_order_id == table.sort_order().order_id


class RowDroppingEngine(CompactionEngine):
    def __init__(self, engine):
        self.engine = engine

    @contextmanager
    def read_rows(self, job):
        with self.engine.read_rows(job) as reader:
            rows = reader.read_all().slice(1)
            yield pa.RecordBatchReader.from_batches(rows.schema, rows.to_batches())

    def close(self):
        self.engine.close()


def test_row_count_mismatch_fails_and_deletes_written_files(catalog, engine, tmp_path):
    table = create_table(catalog)
    append_files(table, 2)
    files_before = set((tmp_path / "warehouse").rglob("*.parquet"))

    with pytest.raises(RowCountMismatchError):
        Execution(RowDroppingEngine(engine)).execute(Planner(target_file_size_mib=1).plan(table))

    assert set((tmp_path / "warehouse").rglob("*.parquet")) == files_before


@pytest.mark.parametrize(
    ("rows", "uncompressed_bytes", "batch_rows"),
    [(1_000, 1_000, 100), (1_000, 100_000, 10), (0, 0, 100)],
)
def test_compute_batch_rows_keeps_row_groups_under_both_limits(rows, uncompressed_bytes, batch_rows):
    assert (
        compute_batch_rows(
            row_group_rows_limit=100, row_group_bytes_limit=1_000, rows=rows, uncompressed_bytes=uncompressed_bytes
        )
        == batch_rows
    )
