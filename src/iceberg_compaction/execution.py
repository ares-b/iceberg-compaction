import time
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from typing import IO, Any, Self, cast

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import (
    _get_parquet_writer_kwargs,
    compute_statistics_plan,
    data_file_statistics_from_parquet_metadata,
    parquet_path_to_id_mapping,
    schema_to_pyarrow,
)
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema, sanitize_column_names
from pyiceberg.table import Table, TableProperties
from pyiceberg.table.sorting import NullOrder, SortDirection
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.utils.properties import property_as_int

from iceberg_compaction.errors import RowCountMismatchError
from iceberg_compaction.log import LoggingMixin
from iceberg_compaction.planning import MIB_TO_BYTES, CompactionPlan, CompactionTask, TrackedFile


@dataclass(frozen=True)
class SortColumn:
    name: str
    descending: bool
    nulls_first: bool


type SortColumns = tuple[SortColumn, ...]


@dataclass(frozen=True)
class RewriteJob:
    schema: Schema
    io: FileIO
    data_files: tuple[TrackedFile, ...]
    position_delete_files: tuple[TrackedFile, ...]
    equality_delete_files: tuple[TrackedFile, ...]
    sort_columns: SortColumns
    row_group_rows_limit: int
    row_group_bytes_limit: int


class CompactionEngine(ABC):
    @abstractmethod
    def read_rows(self, job: RewriteJob) -> AbstractContextManager[pa.RecordBatchReader]:
        """Stream the rows of the job's data files without deleted rows, sorted, columns matched by field id.

        Batches stay under the job's row group limits: each batch can become one Parquet row group.
        """

    @abstractmethod
    def close(self) -> None: ...

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


@dataclass(frozen=True)
class RewrittenTask:
    task: CompactionTask
    added_files: tuple[DataFile, ...]


@dataclass(frozen=True)
class ExecutionMetrics:
    duration_seconds: float
    tasks: int
    data_files_read: int
    delete_files_applied: int
    bytes_read: int
    rows_written: int
    files_written: int
    bytes_written: int

    @classmethod
    def compute(cls, rewritten_tasks: tuple[RewrittenTask, ...], duration_seconds: float) -> Self:
        tasks = [rewritten_task.task for rewritten_task in rewritten_tasks]
        added_files = [data_file for rewritten_task in rewritten_tasks for data_file in rewritten_task.added_files]
        return cls(
            duration_seconds=duration_seconds,
            tasks=len(tasks),
            data_files_read=sum(len(task.data_files) for task in tasks),
            delete_files_applied=len({delete_file.file.file_path for task in tasks for delete_file in task.delete_files}),
            bytes_read=sum(
                task.data_bytes + sum(delete_file.file.file_size_in_bytes for delete_file in task.delete_files) for task in tasks
            ),
            rows_written=sum(data_file.record_count for data_file in added_files),
            files_written=len(added_files),
            bytes_written=sum(data_file.file_size_in_bytes for data_file in added_files),
        )


@dataclass(frozen=True)
class ExecutionResult:
    plan: CompactionPlan
    tasks: tuple[RewrittenTask, ...]
    metrics: ExecutionMetrics


@dataclass(frozen=True)
class PartitionPath:
    partition: Record
    spec: PartitionSpec
    schema: Schema

    def to_path(self) -> str:
        return self.spec.partition_to_path(self.partition, self.schema)


class Execution(LoggingMixin, compaction_phase="rewrite"):
    def __init__(self, engine: CompactionEngine, max_concurrent_tasks: int = 4, data_file_prefix: str = "compaction"):
        if max_concurrent_tasks < 1:
            raise ValueError(f"max_concurrent_tasks must be >= 1, got {max_concurrent_tasks}")
        self.engine = engine
        self.max_concurrent_tasks = max_concurrent_tasks
        self.data_file_prefix = data_file_prefix

    def execute(self, plan: CompactionPlan) -> ExecutionResult:
        started = time.perf_counter()
        sort_columns, sort_order_id = self._resolve_sort_columns(plan.table, plan.table_name)
        written_paths: list[str] = []
        try:
            with ThreadPoolExecutor(self.max_concurrent_tasks) as executor:
                futures = [
                    executor.submit(self._rewrite_task, plan, task, sort_columns, sort_order_id, written_paths)
                    for task in plan.tasks
                ]
                try:
                    rewritten_tasks = tuple(future.result() for future in futures)
                except BaseException:
                    for future in futures:
                        future.cancel()
                    raise
        except BaseException:
            self.log.exception(
                f"Execution failed, deleting written files: {len(written_paths)}",
                table=plan.table_name,
                written_files=len(written_paths),
            )
            for path in delete_files(plan.table.io, written_paths):
                self.log.warning(f"Could not delete {path}, remove it by hand", table=plan.table_name, file_path=path)
            raise

        metrics = ExecutionMetrics.compute(rewritten_tasks, time.perf_counter() - started)
        self.log.info(
            f"Execution OK. Tasks: {metrics.tasks}, files read: {metrics.data_files_read}, "
            f"files written: {metrics.files_written}, rows written: {metrics.rows_written}, "
            f"written: {metrics.bytes_written / MIB_TO_BYTES:.1f} MiB, took {metrics.duration_seconds:.2f}s",
            table=plan.table_name,
            **asdict(metrics),
        )
        return ExecutionResult(plan, rewritten_tasks, metrics)

    def _rewrite_task(
        self,
        plan: CompactionPlan,
        task: CompactionTask,
        sort_columns: SortColumns,
        sort_order_id: int | None,
        written_paths: list[str],
    ) -> RewrittenTask:
        started = time.perf_counter()
        job = RewriteJob(
            schema=plan.table.schema(),
            io=plan.table.io,
            data_files=task.data_files,
            position_delete_files=task.position_delete_files,
            equality_delete_files=task.equality_delete_files,
            sort_columns=sort_columns,
            row_group_rows_limit=cast(
                int,
                property_as_int(
                    plan.table.properties,
                    TableProperties.PARQUET_ROW_GROUP_LIMIT,
                    TableProperties.PARQUET_ROW_GROUP_LIMIT_DEFAULT,
                ),
            ),
            row_group_bytes_limit=cast(
                int,
                property_as_int(
                    plan.table.properties,
                    TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
                    TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT,
                ),
            ),
        )
        with self.engine.read_rows(job) as reader:
            added_files = self._write_data_files(reader, plan, task, sort_order_id, written_paths)

        rows_read = sum(data_file.file.record_count for data_file in task.data_files)
        rows_written = sum(data_file.record_count for data_file in added_files)
        if rows_written > rows_read or (not task.delete_files and rows_written != rows_read):
            error = RowCountMismatchError(task.partition_path, rows_read, rows_written)
            self.log.error(str(error), table=plan.table_name, rows_read=rows_read, rows_written=rows_written)
            raise error

        self.log.info(
            f"Task rewritten for partition {task.partition_path or 'unpartitioned'}. Files read: {len(task.data_files)}, "
            f"files written: {len(added_files)}, rows: {rows_written}, took {time.perf_counter() - started:.2f}s",
            table=plan.table_name,
            partition=task.partition_path,
            data_files_read=len(task.data_files),
            delete_files_applied=len(task.delete_files),
            files_written=len(added_files),
            rows_read=rows_read,
            rows_written=rows_written,
        )
        return RewrittenTask(task, tuple(added_files))

    def _resolve_sort_columns(self, table: Table, table_name: str) -> tuple[SortColumns, int | None]:
        sort_order = table.sort_order()
        if sort_order.is_unsorted:
            return (), None
        top_level_names = {schema_field.field_id: schema_field.name for schema_field in table.schema().fields}
        sort_columns = []
        for sort_field in sort_order.fields:
            if not isinstance(sort_field.transform, IdentityTransform) or sort_field.source_id not in top_level_names:
                self.log.warning(
                    f"Sort order {sort_order.order_id} not supported, files are written unsorted. "
                    "Only identity transforms on top-level columns are supported",
                    table=table_name,
                    sort_order_id=sort_order.order_id,
                    sort_field=str(sort_field),
                )
                return (), None
            sort_columns.append(
                SortColumn(
                    name=top_level_names[sort_field.source_id],
                    descending=sort_field.direction == SortDirection.DESC,
                    nulls_first=sort_field.null_order == NullOrder.NULLS_FIRST,
                )
            )
        return tuple(sort_columns), sort_order.order_id

    def _write_data_files(
        self,
        reader: pa.RecordBatchReader,
        plan: CompactionPlan,
        task: CompactionTask,
        sort_order_id: int | None,
        written_paths: list[str],
    ) -> list[DataFile]:
        table = plan.table
        spec = table.spec()
        file_schema = sanitize_column_names(table.schema())
        arrow_schema = schema_to_pyarrow(file_schema, include_field_ids=True)
        writer_kwargs = _get_parquet_writer_kwargs(
            {key: value for key, value in table.properties.items() if key != TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES}
        )
        statistics_plan = compute_statistics_plan(file_schema, table.properties)
        column_mapping = parquet_path_to_id_mapping(file_schema)
        partition_key = None if spec.is_unpartitioned() else PartitionPath(task.partition, spec, table.schema())
        location_provider = table.location_provider()
        file_name_prefix = f"{self.data_file_prefix}-{uuid.uuid4()}"
        target_file_size_bytes = plan.target_file_size_bytes

        def close_file(path: str, stream: Any, writer: pq.ParquetWriter) -> DataFile:
            writer.close()
            size = stream.tell()
            stream.close()
            statistics = data_file_statistics_from_parquet_metadata(writer.writer.metadata, statistics_plan, column_mapping)
            return DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=path,
                file_format=FileFormat.PARQUET,
                partition=task.partition,
                file_size_in_bytes=size,
                sort_order_id=sort_order_id,
                spec_id=spec.spec_id,
                equality_ids=None,
                key_metadata=None,
                **statistics.to_serialized_dict(),
            )

        data_files: list[DataFile] = []
        path, stream, writer, file_rows = "", None, None, 0
        bytes_per_row = task.data_bytes / max(sum(data_file.file.record_count for data_file in task.data_files), 1)
        is_measured = False
        try:
            for batch in reader:
                batch = batch.rename_columns(arrow_schema.names).cast(arrow_schema)
                while batch.num_rows:
                    if writer is None:
                        path = location_provider.new_data_location(
                            f"{file_name_prefix}-{len(data_files):05d}.parquet",
                            partition_key,  # type: ignore[arg-type]
                        )
                        written_paths.append(path)
                        stream = table.io.new_output(path).create(overwrite=False)
                        writer = pq.ParquetWriter(
                            cast(IO[Any], stream), schema=arrow_schema, store_decimal_as_integer=True, **writer_kwargs
                        )
                        file_rows = 0
                    fill_ratio = 0.9 if is_measured else 0.5
                    rows = max(1, int((target_file_size_bytes - stream.tell()) / bytes_per_row * fill_ratio))
                    chunk, batch = batch.slice(0, rows), batch.slice(rows)
                    writer.write_batch(chunk, row_group_size=chunk.num_rows)
                    file_rows += chunk.num_rows
                    bytes_per_row = stream.tell() / file_rows
                    is_measured = True
                    if target_file_size_bytes - stream.tell() < max(bytes_per_row, target_file_size_bytes / 100):
                        data_files.append(close_file(path, stream, writer))
                        writer = None
            if writer is not None:
                data_files.append(close_file(path, stream, writer))
                writer = None
        except BaseException:
            if writer is not None:
                for resource in (writer, stream):
                    try:
                        resource.close()
                    except OSError:
                        self.log.warning(f"Could not close {path}", table=plan.table_name, file_path=path)
            raise
        return data_files


def delete_files(io: FileIO, paths: list[str]) -> list[str]:
    undeleted_paths = []
    for path in paths:
        try:
            io.delete(path)
        except FileNotFoundError:
            pass
        except OSError:
            undeleted_paths.append(path)
    return undeleted_paths


def compute_batch_rows(row_group_rows_limit: int, row_group_bytes_limit: int, rows: int, uncompressed_bytes: int) -> int:
    if not rows:
        return row_group_rows_limit
    return max(1, min(row_group_rows_limit, int(row_group_bytes_limit * rows / max(uncompressed_bytes, 1))))


if __name__ == "__main__":
    import os
    import tempfile

    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.types import BinaryType, LongType, NestedField

    from iceberg_compaction.engines.duckdb_engine import DuckDBEngine
    from iceberg_compaction.execution import Execution as PackageExecution
    from iceberg_compaction.planning import CompactionMode, Planner

    with tempfile.TemporaryDirectory() as warehouse:
        catalog = SqlCatalog("demo", uri=f"sqlite:///{warehouse}/catalog.db", warehouse=f"file://{warehouse}")
        catalog.create_namespace("db")
        table = catalog.create_table(
            "db.events", schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "payload", BinaryType()))
        )
        for row_count in [100] * 20 + [5 * 1024]:
            table.append(
                pa.table(
                    {"id": range(row_count), "payload": [os.urandom(1024) for _ in range(row_count)]},
                    schema=table.schema().as_arrow(),
                )
            )

        plan = Planner(compaction_mode=CompactionMode.BOTH, target_file_size_mib=2).plan(table)
        with DuckDBEngine(memory_limit="1GB") as engine:
            result = PackageExecution(engine).execute(plan)
        print(result.metrics)
        for rewritten_task in result.tasks:
            print([f"{data_file.file_size_in_bytes / MIB_TO_BYTES:.2f} MiB" for data_file in rewritten_task.added_files])
