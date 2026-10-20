import time
import uuid
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import IO, Any, Protocol, cast

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
from pyiceberg.table import TableProperties
from pyiceberg.typedef import Record

from iceberg_compaction.log import LoggingMixin
from iceberg_compaction.planning import CompactionPlan, CompactionTask, TrackedFile


@dataclass(frozen=True)
class RewriteJob:
    schema: Schema
    io: FileIO
    data_files: tuple[TrackedFile, ...]


class CompactionEngine(Protocol):
    def read_rows(self, job: RewriteJob) -> AbstractContextManager[pa.RecordBatchReader]:
        """Stream the rows of the job's data files, columns of the job's schema matched by field id."""
        ...


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
        rewritten_tasks = tuple(self._rewrite_task(plan, task) for task in plan.tasks)
        return ExecutionResult(plan, rewritten_tasks)

    def _rewrite_task(self, plan: CompactionPlan, task: CompactionTask) -> RewrittenTask:
        started = time.perf_counter()
        job = RewriteJob(schema=plan.table.schema(), io=plan.table.io, data_files=task.data_files)
        with self.engine.read_rows(job) as reader:
            added_file = self._write_data_file(reader, plan, task)
        self.log.info(
            f"Task rewritten for partition {task.partition_path or 'unpartitioned'}. Files read: {len(task.data_files)}, "
            f"rows: {added_file.record_count}, took {time.perf_counter() - started:.2f}s",
            table=plan.table_name,
            partition=task.partition_path,
            data_files_read=len(task.data_files),
            rows_written=added_file.record_count,
        )
        return RewrittenTask(task, (added_file,))

    def _write_data_file(self, reader: pa.RecordBatchReader, plan: CompactionPlan, task: CompactionTask) -> DataFile:
        table = plan.table
        spec = table.spec()
        file_schema = sanitize_column_names(table.schema())
        arrow_schema = schema_to_pyarrow(file_schema, include_field_ids=True)
        writer_kwargs = _get_parquet_writer_kwargs(
            {key: value for key, value in table.properties.items() if key != TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES}
        )
        partition_key = None if spec.is_unpartitioned() else PartitionPath(task.partition, spec, table.schema())
        path = table.location_provider().new_data_location(
            f"{self.data_file_prefix}-{uuid.uuid4()}.parquet",
            partition_key,  # type: ignore[arg-type]
        )
        with table.io.new_output(path).create(overwrite=False) as stream:
            writer = pq.ParquetWriter(cast(IO[Any], stream), schema=arrow_schema, store_decimal_as_integer=True, **writer_kwargs)
            with writer:
                for batch in reader:
                    writer.write_batch(batch.rename_columns(arrow_schema.names).cast(arrow_schema))
            size = stream.tell()
        statistics = data_file_statistics_from_parquet_metadata(
            writer.writer.metadata,
            compute_statistics_plan(file_schema, table.properties),
            parquet_path_to_id_mapping(file_schema),
        )
        return DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=path,
            file_format=FileFormat.PARQUET,
            partition=task.partition,
            file_size_in_bytes=size,
            sort_order_id=None,
            spec_id=spec.spec_id,
            equality_ids=None,
            key_metadata=None,
            **statistics.to_serialized_dict(),
        )
