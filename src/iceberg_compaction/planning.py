import copy
import math
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any, Self

from pyiceberg.expressions import AlwaysTrue, And, BooleanExpression, In
from pyiceberg.expressions.literals import literal
from pyiceberg.expressions.visitors import manifest_evaluator
from pyiceberg.manifest import INITIAL_SEQUENCE_NUMBER, DataFile, DataFileContent, FileFormat, ManifestFile
from pyiceberg.table import Table, TableProperties
from pyiceberg.table.delete_file_index import PATH_FIELD_ID
from pyiceberg.table.refs import MAIN_BRANCH
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.typedef import Record

from iceberg_compaction.errors import UnsupportedDataFileTypeError, UnsupportedTableFormatVersionError
from iceberg_compaction.log import LoggingMixin

MIB_TO_BYTES = 1024**2
GIB_TO_BYTES = 1024**3

type HivePartitionFilter = dict[str, list[Any]]
type PartitionFilter = dict[int, set[Any]]
type DataFiles = dict[Record, list[TrackedFile]]
type DeleteFiles = dict[tuple[int, Record], list[TrackedFile]]
type CompactionTaskDecision = tuple[bool, str]


class Strategy(StrEnum):
    SMALL_FILES = "small-files"
    FILES_WITH_DELETES = "files-with-deletes"
    AUTO = "auto"
    FULL = "full"

    @property
    def selects_small_files(self) -> bool:
        return self in (Strategy.SMALL_FILES, Strategy.AUTO, Strategy.FULL)

    @property
    def selects_files_with_deletes(self) -> bool:
        return self in (Strategy.FILES_WITH_DELETES, Strategy.AUTO, Strategy.FULL)


@dataclass(frozen=True)
class TrackedFile:
    file: DataFile
    sequence_number: int


@dataclass(frozen=True)
class CompactionTask:
    partition: Record
    partition_path: str
    data_files: tuple[TrackedFile, ...]
    position_delete_files: tuple[TrackedFile, ...] = ()
    equality_delete_files: tuple[TrackedFile, ...] = ()

    @property
    def data_bytes(self) -> int:
        return sum(data_file.file.file_size_in_bytes for data_file in self.data_files)

    @property
    def delete_files(self) -> tuple[TrackedFile, ...]:
        return self.position_delete_files + self.equality_delete_files


@dataclass(frozen=True)
class PlanMetrics:
    duration_seconds: float
    tasks: int
    data_files_to_rewrite: int
    delete_files_to_apply: int
    data_files_skipped: int
    bytes_to_read: int
    bytes_to_write: int

    @classmethod
    def compute(cls, tasks: list[CompactionTask], data_files_in_scope: int, duration_seconds: float) -> Self:
        data_files_to_rewrite = sum(len(task.data_files) for task in tasks)
        return cls(
            duration_seconds=duration_seconds,
            tasks=len(tasks),
            data_files_to_rewrite=data_files_to_rewrite,
            delete_files_to_apply=len({delete_file.file.file_path for task in tasks for delete_file in task.delete_files}),
            data_files_skipped=data_files_in_scope - data_files_to_rewrite,
            bytes_to_read=sum(
                task.data_bytes + sum(delete_file.file.file_size_in_bytes for delete_file in task.delete_files) for task in tasks
            ),
            bytes_to_write=sum(task.data_bytes for task in tasks),
        )


@dataclass
class IndexedFiles:
    data_files_by_partition: DataFiles = field(default_factory=lambda: defaultdict(list))
    position_deletes_by_partition: DeleteFiles = field(default_factory=lambda: defaultdict(list))
    equality_deletes_by_partition: DeleteFiles = field(default_factory=lambda: defaultdict(list))
    global_equality_deletes: list[TrackedFile] = field(default_factory=list)
    data_files_in_scope: int = 0
    old_spec_file_count: int = 0


@dataclass(frozen=True)
class CompactionPlan:
    table: Table = field(repr=False, compare=False)
    table_name: str
    branch: str
    strategy: Strategy
    snapshot_id: int | None
    target_file_size_bytes: int
    tasks: tuple[CompactionTask, ...]
    metrics: PlanMetrics


class Planner(LoggingMixin, compaction_phase="plan"):
    def __init__(
        self,
        strategy: Strategy = Strategy.SMALL_FILES,
        branch: str = MAIN_BRANCH,
        partition_filter: HivePartitionFilter | None = None,
        target_file_size_mib: int | None = None,
        min_delete_file_count: int = 1,
        max_task_size_gib: int | None = None,
        min_task_size_mib: int | None = None,
        min_task_file_count: int | None = None,
    ):
        for name, value, minimum in (
            ("target_file_size_mib", target_file_size_mib, 1),
            ("min_delete_file_count", min_delete_file_count, 1),
            ("max_task_size_gib", max_task_size_gib, 1),
            ("min_task_size_mib", min_task_size_mib, 0),
            ("min_task_file_count", min_task_file_count, 0),
        ):
            if value is not None and value < minimum:
                raise ValueError(f"{name} must be >= {minimum}, got {value}")
        self.strategy = Strategy(strategy)
        self.branch = branch
        self.partition_filter = partition_filter or {}
        self.target_file_size_mib = target_file_size_mib
        self.min_delete_file_count = min_delete_file_count
        self.max_task_size_bytes = None if max_task_size_gib is None else max_task_size_gib * GIB_TO_BYTES
        self.min_task_size_bytes = None if min_task_size_mib is None else min_task_size_mib * MIB_TO_BYTES
        self.min_task_file_count = min_task_file_count

    def plan(self, table: Table) -> CompactionPlan:
        started = time.perf_counter()
        table_name = ".".join(table.name())
        if table.format_version > 2:
            error = UnsupportedTableFormatVersionError(table.format_version)
            self.log.error(f"{error}. Only versions 1 and 2 are supported", table=table_name)
            raise error
        planned_table = copy.copy(table)
        target_file_size_bytes = self._resolve_target_file_size(planned_table, table_name)
        partition_filter = self._resolve_partition_filter(planned_table)

        snapshot = planned_table.snapshot_by_name(self.branch)
        if snapshot is None:
            if self.branch != MAIN_BRANCH:
                raise ValueError(f"Table {table_name} has no branch {self.branch!r}")
            self.log.info("Nothing to plan, the table has no snapshot", table=table_name)
            metrics = PlanMetrics.compute([], 0, time.perf_counter() - started)
            return CompactionPlan(
                planned_table, table_name, self.branch, self.strategy, None, target_file_size_bytes, (), metrics
            )

        manifests = self._select_manifests(planned_table, snapshot, partition_filter)
        files = self._index_files(planned_table, manifests, partition_filter, table_name)
        spec = planned_table.spec()
        tasks = []
        for partition, data_files in files.data_files_by_partition.items():
            tasks += self._plan_partition(
                partition=partition,
                partition_path=spec.partition_to_path(partition, planned_table.schema()),
                data_files=data_files,
                position_deletes=files.position_deletes_by_partition.get((spec.spec_id, partition), []),
                equality_deletes=files.equality_deletes_by_partition.get((spec.spec_id, partition), [])
                + files.global_equality_deletes,
                target_file_size_bytes=target_file_size_bytes,
                table_name=table_name,
            )
        tasks.sort(key=lambda task: task.partition_path)

        metrics = PlanMetrics.compute(tasks, files.data_files_in_scope, time.perf_counter() - started)
        self.log.info(
            f"Plan OK. Tasks: {metrics.tasks}, files to rewrite: {metrics.data_files_to_rewrite}, "
            f"delete files: {metrics.delete_files_to_apply}, files skipped: {metrics.data_files_skipped}, "
            f"to read: {metrics.bytes_to_read / MIB_TO_BYTES:.1f} MiB, took {metrics.duration_seconds:.2f}s",
            table=table_name,
            branch=self.branch,
            strategy=str(self.strategy),
            snapshot_id=snapshot.snapshot_id,
            target_file_size_bytes=target_file_size_bytes,
            **asdict(metrics),
        )
        return CompactionPlan(
            planned_table,
            table_name,
            self.branch,
            self.strategy,
            snapshot.snapshot_id,
            target_file_size_bytes,
            tuple(tasks),
            metrics,
        )

    def _select_manifests(self, table: Table, snapshot: Snapshot, partition_filter: PartitionFilter) -> list[ManifestFile]:
        spec = table.spec()
        specs = table.specs()
        partition_fields = spec.partition_type(table.schema()).fields
        partition_expression: BooleanExpression = AlwaysTrue()
        for position, values in partition_filter.items():
            partition_expression = And(partition_expression, In(partition_fields[position].name, values))
        matches_partition_filter = manifest_evaluator(spec, table.schema(), partition_expression)
        return [
            manifest
            for manifest in snapshot.manifests(table.io)
            if (manifest.partition_spec_id == spec.spec_id and matches_partition_filter(manifest))
            or specs[manifest.partition_spec_id].is_unpartitioned()
        ]

    def _index_files(
        self, table: Table, manifests: list[ManifestFile], partition_filter: PartitionFilter, table_name: str
    ) -> IndexedFiles:
        spec = table.spec()
        specs = table.specs()
        files = IndexedFiles()
        with ThreadPoolExecutor() as executor:
            manifest_entries = executor.map(
                lambda manifest: manifest.fetch_manifest_entry(table.io, discard_deleted=True), manifests
            )
            for entry in (entry for entries in manifest_entries for entry in entries):
                content_file = entry.data_file
                if content_file.file_format != FileFormat.PARQUET:
                    error = UnsupportedDataFileTypeError(content_file.file_path, content_file.file_format)
                    self.log.error(
                        f"{error}. Only Parquet is supported",
                        table=table_name,
                        file_path=content_file.file_path,
                        file_format=content_file.file_format.value,
                    )
                    raise error
                tracked_file = TrackedFile(content_file, entry.sequence_number or INITIAL_SEQUENCE_NUMBER)
                if content_file.content == DataFileContent.DATA:
                    if content_file.spec_id != spec.spec_id:
                        files.old_spec_file_count += 1
                    elif all(content_file.partition[position] in values for position, values in partition_filter.items()):
                        files.data_files_in_scope += 1
                        files.data_files_by_partition[content_file.partition].append(tracked_file)
                elif content_file.content == DataFileContent.POSITION_DELETES:
                    files.position_deletes_by_partition[(content_file.spec_id, content_file.partition)].append(tracked_file)
                elif specs[content_file.spec_id].is_unpartitioned():
                    files.global_equality_deletes.append(tracked_file)
                else:
                    files.equality_deletes_by_partition[(content_file.spec_id, content_file.partition)].append(tracked_file)
        if files.old_spec_file_count:
            self.log.info(
                f"Files from an older partition spec skipped: {files.old_spec_file_count}",
                table=table_name,
                old_spec_files=files.old_spec_file_count,
            )
        return files

    def _plan_partition(
        self,
        partition: Record,
        partition_path: str,
        data_files: list[TrackedFile],
        position_deletes: list[TrackedFile],
        equality_deletes: list[TrackedFile],
        target_file_size_bytes: int,
        table_name: str,
    ) -> list[CompactionTask]:
        candidates = []
        position_deletes_by_data_path: dict[str, list[TrackedFile]] = {}
        equality_deletes_by_data_path: dict[str, list[TrackedFile]] = {}
        data_paths_to_clean: set[str] = set()
        for data_file in data_files:
            data_path = data_file.file.file_path
            applicable_position_deletes, applicable_equality_deletes = find_applicable_deletes(
                data_file, position_deletes, equality_deletes
            )
            is_small = data_file.file.file_size_in_bytes < target_file_size_bytes
            has_deletes = len(applicable_position_deletes) + len(applicable_equality_deletes) >= self.min_delete_file_count
            if is_candidate(self.strategy, is_small, has_deletes):
                candidates.append(data_file)
                position_deletes_by_data_path[data_path] = applicable_position_deletes
                equality_deletes_by_data_path[data_path] = applicable_equality_deletes
                if has_deletes and self.strategy.selects_files_with_deletes:
                    data_paths_to_clean.add(data_path)

        tasks = []
        for task_data_files in pack_into_tasks(candidates, self.max_task_size_bytes):
            task_paths = [data_file.file.file_path for data_file in task_data_files]
            task = CompactionTask(
                partition=partition,
                partition_path=partition_path,
                data_files=tuple(task_data_files),
                position_delete_files=tuple(
                    {
                        delete.file.file_path: delete for path in task_paths for delete in position_deletes_by_data_path[path]
                    }.values()
                ),
                equality_delete_files=tuple(
                    {
                        delete.file.file_path: delete for path in task_paths for delete in equality_deletes_by_data_path[path]
                    }.values()
                ),
            )
            output_file_count = math.ceil(task.data_bytes / target_file_size_bytes)
            is_kept, reason = self._decide_whether_to_keep(
                task, bool(data_paths_to_clean.intersection(task_paths)), output_file_count
            )
            summary = (
                f"partition {partition_path or 'unpartitioned'}. Files: {len(task.data_files)}, "
                f"delete files: {len(task.delete_files)}, size: {task.data_bytes / MIB_TO_BYTES:.1f} MiB"
            )
            fields = {
                "partition": partition_path,
                "data_files": len(task.data_files),
                "data_bytes": task.data_bytes,
                "delete_files": len(task.delete_files),
                "output_files": output_file_count,
                "reason": reason,
            }
            if is_kept:
                tasks.append(task)
                self.log.info(f"Task planned for {summary}. Reason: {reason}", table=table_name, **fields)
            else:
                self.log.debug(f"Task skipped for {summary}. Reason: {reason}", table=table_name, **fields)
        return tasks

    def _decide_whether_to_keep(
        self, task: CompactionTask, has_files_to_clean: bool, output_file_count: int
    ) -> CompactionTaskDecision:
        if self.strategy == Strategy.FULL:
            return True, "full rewrite"
        if has_files_to_clean:
            return True, "has delete files"
        if output_file_count >= len(task.data_files):
            return False, "merging saves no file"
        if (self.min_task_size_bytes is not None and task.data_bytes < self.min_task_size_bytes) or (
            self.min_task_file_count is not None and len(task.data_files) < self.min_task_file_count
        ):
            return False, "below the minimum task size or file count"
        return True, f"merge {len(task.data_files)} files into {output_file_count}"

    def _resolve_target_file_size(self, table: Table, table_name: str) -> int:
        writer_target_file_size_bytes = int(
            table.properties.get(
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT
            )
        )
        if self.target_file_size_mib is None:
            return writer_target_file_size_bytes
        target_file_size_bytes = self.target_file_size_mib * MIB_TO_BYTES
        if target_file_size_bytes != writer_target_file_size_bytes:
            writer_target_source = (
                f"table property {TableProperties.WRITE_TARGET_FILE_SIZE_BYTES}"
                if TableProperties.WRITE_TARGET_FILE_SIZE_BYTES in table.properties
                else "Iceberg default"
            )
            self.log.warning(
                f"Target file size is {target_file_size_bytes / MIB_TO_BYTES:g} MiB but writers use "
                f"{writer_target_file_size_bytes / MIB_TO_BYTES:g} MiB ({writer_target_source})",
                table=table_name,
                target_file_size_bytes=target_file_size_bytes,
                writer_target_file_size_bytes=writer_target_file_size_bytes,
            )
        return target_file_size_bytes

    def _resolve_partition_filter(self, table: Table) -> PartitionFilter:
        partition_fields = table.spec().partition_type(table.schema()).fields
        positions_by_name = {partition_field.name: position for position, partition_field in enumerate(partition_fields)}
        partition_filter = {}
        for field_name, values in self.partition_filter.items():
            if field_name not in positions_by_name:
                raise ValueError(f"Unknown partition field {field_name!r}, expected one of {sorted(positions_by_name)}")
            if not values:
                raise ValueError(f"No values for partition field {field_name!r}")
            position = positions_by_name[field_name]
            field_type = partition_fields[position].field_type
            partition_filter[position] = {literal(value).to(field_type).value for value in values}
        return partition_filter


def find_applicable_deletes(
    data_file: TrackedFile, position_deletes: list[TrackedFile], equality_deletes: list[TrackedFile]
) -> tuple[list[TrackedFile], list[TrackedFile]]:
    return (
        [
            delete_file
            for delete_file in position_deletes
            if delete_file.sequence_number >= data_file.sequence_number
            and is_in_path_bounds(delete_file.file, data_file.file.file_path)
        ],
        [delete_file for delete_file in equality_deletes if delete_file.sequence_number > data_file.sequence_number],
    )


def is_candidate(strategy: Strategy, is_small: bool, has_deletes: bool) -> bool:
    return (
        strategy == Strategy.FULL
        or (is_small and strategy.selects_small_files)
        or (has_deletes and strategy.selects_files_with_deletes)
    )


def is_in_path_bounds(position_delete_file: DataFile, data_path: str) -> bool:
    lower = (position_delete_file.lower_bounds or {}).get(PATH_FIELD_ID)
    upper = (position_delete_file.upper_bounds or {}).get(PATH_FIELD_ID)
    return lower is None or upper is None or lower <= data_path.encode() <= upper


def pack_into_tasks(data_files: list[TrackedFile], max_task_size_bytes: int | None) -> list[list[TrackedFile]]:
    if not data_files:
        return []
    if max_task_size_bytes is None:
        return [data_files]
    tasks: list[list[TrackedFile]] = []
    task_sizes: list[int] = []
    for data_file in sorted(data_files, key=lambda data_file: data_file.file.file_size_in_bytes, reverse=True):
        size = data_file.file.file_size_in_bytes
        for index, task_size in enumerate(task_sizes):
            if task_size + size <= max_task_size_bytes:
                tasks[index].append(data_file)
                task_sizes[index] += size
                break
        else:
            tasks.append([data_file])
            task_sizes.append(size)
    return tasks


if __name__ == "__main__":
    import tempfile

    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    from iceberg_compaction.planning import Planner as PackagePlanner

    with tempfile.TemporaryDirectory() as warehouse:
        catalog = SqlCatalog("demo", uri=f"sqlite:///{warehouse}/catalog.db", warehouse=f"file://{warehouse}")
        catalog.create_namespace("db")
        table = catalog.create_table(
            "db.events",
            schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "category", StringType())),
            partition_spec=PartitionSpec(
                PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category")
            ),
        )
        for category, appends in (("a", 3), ("b", 2), ("c", 1)):
            for batch in range(appends):
                ids = range(batch * 100, (batch + 1) * 100)
                table.append(pa.table({"id": ids, "category": [category] * 100}, schema=table.schema().as_arrow()))

        plan = PackagePlanner(target_file_size_mib=128, partition_filter={"category": ["a", "c"]}).plan(table)
        print(plan.metrics)
        for task in plan.tasks:
            print(f"  {task.partition_path}: {len(task.data_files)} data file(s), {task.data_bytes} bytes")
