import time
import uuid
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from dataclasses import dataclass
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

from iceberg_compaction.errors import RowCountMismatchError
from iceberg_compaction.log import LoggingMixin
from iceberg_compaction.planning import CompactionPlan, CompactionTask, TrackedFile


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


class CompactionEngine(ABC):
    @abstractmethod
    def read_rows(self, job: RewriteJob) -> AbstractContextManager[pa.RecordBatchReader]:
        """Stream the rows of the job's data files without deleted rows, sorted, columns matched by field id."""

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
class ExecutionResult:
    plan: CompactionPlan
    tasks: tuple[RewrittenTask, ...]


@dataclass(frozen=True)
class PartitionPath:
    partition: Record
    spec: PartitionSpec
    schema: Schema

    def to_path(self) -> str:
        return self.spec.partition_to_path(self.partition, self.schema)


class Execution(LoggingMixin, compaction_phase="rewrite"):
    def __init__(self, engine: CompactionEngine, data_file_prefix: str = "compaction"):
        self.engine = engine
        self.data_file_prefix = data_file_prefix

    def execute(self, plan: CompactionPlan) -> ExecutionResult:
        sort_columns, sort_order_id = self._resolve_sort_columns(plan.table, plan.table_name)
        written_paths: list[str] = []
        try:
            rewritten_tasks = tuple(
                self._rewrite_task(plan, task, sort_columns, sort_order_id, written_paths) for task in plan.tasks
            )
        except BaseException:
            self.log.exception(
                f"Execution failed, deleting written files: {len(written_paths)}",
                table=plan.table_name,
                written_files=len(written_paths),
            )
            self._delete_files(plan.table.io, written_paths, plan.table_name)
            raise
        return ExecutionResult(plan, rewritten_tasks)

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

    def _delete_files(self, io: FileIO, paths: list[str], table_name: str) -> None:
        for path in paths:
            try:
                io.delete(path)
            except FileNotFoundError:
                pass
            except OSError:
                self.log.warning(f"Could not delete {path}, remove it by hand", table=table_name, file_path=path)
