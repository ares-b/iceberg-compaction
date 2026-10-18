import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from pyiceberg.expressions.literals import literal
from pyiceberg.manifest import DataFile
from pyiceberg.table import Table, TableProperties
from pyiceberg.typedef import Record

from iceberg_compaction.log import LoggingMixin

MIB_TO_BYTES = 1024**2

HivePartitionFilter = dict[str, list[Any]]


@dataclass(frozen=True)
class FileGroup:
    """Small data files of one partition, merged together."""

    partition: Record
    partition_path: str  # "" for an unpartitioned table
    data_files: tuple[DataFile, ...]

    @property
    def data_bytes(self) -> int:
        return sum(data_file.file_size_in_bytes for data_file in self.data_files)


@dataclass(frozen=True)
class CompactionPlan:
    snapshot_id: int | None
    target_file_size_bytes: int
    groups: tuple[FileGroup, ...]


class Planner(LoggingMixin, compaction_phase="plan"):
    def __init__(self, target_file_size_mib: int | None = None, partition_filter: HivePartitionFilter | None = None):
        """target_file_size_mib: None follows the table's write.target-file-size-bytes, else Iceberg's 512 MiB default.
        partition_filter: {"partition field": [values]}, values typed or Hive-style strings ("2024-01-02").
        """
        if target_file_size_mib is not None and target_file_size_mib <= 0:
            raise ValueError(f"target_file_size_mib must be > 0, got {target_file_size_mib}")
        self.target_file_size_mib = target_file_size_mib
        self.partition_filter = partition_filter or {}

    def plan(self, table: Table) -> CompactionPlan:
        table_name = ".".join(table.name())
        target_file_size_bytes = self._resolve_target_file_size(table, table_name)
        partition_filter_by_position = self._resolve_partition_filter(table)

        snapshot = table.current_snapshot()
        if snapshot is None:
            self.log.info("Nothing to compact: table has no snapshot", table=table_name)
            return CompactionPlan(snapshot_id=None, target_file_size_bytes=target_file_size_bytes, groups=())

        spec = table.spec()
        small_files_by_partition: dict[Record, list[DataFile]] = defaultdict(list)
        old_spec_file_count = 0
        for task in table.scan().plan_files():
            data_file = task.file
            if data_file.spec_id != spec.spec_id:
                old_spec_file_count += 1
            elif data_file.file_size_in_bytes < target_file_size_bytes and all(
                data_file.partition[position] in values for position, values in partition_filter_by_position.items()
            ):
                small_files_by_partition[data_file.partition].append(data_file)
        if old_spec_file_count:
            self.log.info(
                f"Ignoring {old_spec_file_count} file(s) written with an older partition spec",
                table=table_name,
                old_spec_files=old_spec_file_count,
            )

        groups = []
        for partition, small_files in small_files_by_partition.items():
            group = FileGroup(partition, spec.partition_to_path(partition, table.schema()), tuple(small_files))
            output_file_count = math.ceil(group.data_bytes / target_file_size_bytes)
            summary = (
                f"{len(small_files)} small file(s) of {group.data_bytes} bytes ({group.data_bytes / MIB_TO_BYTES:.1f} MiB) "
                f"in partition '{group.partition_path}'"
            )
            fields = {
                "partition": group.partition_path,
                "small_files": len(small_files),
                "small_files_bytes": group.data_bytes,
                "output_files": output_file_count,
            }
            if output_file_count >= len(small_files):
                self.log.debug(f"Skipped {summary}: a merge would still give {output_file_count} file(s)", table=table_name, **fields)
                continue
            groups.append(group)
            self.log.info(
                f"Planned merge of {summary} into {output_file_count} file(s) of up to "
                f"{target_file_size_bytes / MIB_TO_BYTES:.0f} MiB",
                table=table_name,
                snapshot_id=snapshot.snapshot_id,
                target_file_size_bytes=target_file_size_bytes,
                **fields,
            )
        if not groups:
            self.log.info("Nothing to compact: no partition gains from a merge", table=table_name)
        groups.sort(key=lambda group: group.partition_path)
        return CompactionPlan(snapshot_id=snapshot.snapshot_id, target_file_size_bytes=target_file_size_bytes, groups=tuple(groups))

    def _resolve_target_file_size(self, table: Table, table_name: str) -> int:
        writer_target_file_size_bytes = int(
            table.properties.get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT)
        )
        if self.target_file_size_mib is None:
            return writer_target_file_size_bytes
        target_file_size_bytes = self.target_file_size_mib * MIB_TO_BYTES
        if target_file_size_bytes != writer_target_file_size_bytes:
            writer_target_source = (
                f"table property {TableProperties.WRITE_TARGET_FILE_SIZE_BYTES}"
                if TableProperties.WRITE_TARGET_FILE_SIZE_BYTES in table.properties
                else f"Iceberg default, {TableProperties.WRITE_TARGET_FILE_SIZE_BYTES} not set"
            )
            self.log.warning(
                f"Compacting to {target_file_size_bytes / MIB_TO_BYTES:.1f} MiB files while writers target "
                f"{writer_target_file_size_bytes / MIB_TO_BYTES:.1f} MiB ({writer_target_source})",
                table=table_name,
                target_file_size_bytes=target_file_size_bytes,
                writer_target_file_size_bytes=writer_target_file_size_bytes,
            )
        return target_file_size_bytes

    def _resolve_partition_filter(self, table: Table) -> dict[int, set[Any]]:
        partition_fields = table.spec().partition_type(table.schema()).fields
        positions_by_name = {partition_field.name: position for position, partition_field in enumerate(partition_fields)}
        partition_filter_by_position = {}
        for field_name, values in self.partition_filter.items():
            if field_name not in positions_by_name:
                raise ValueError(f"Unknown partition field {field_name!r}, expected one of {sorted(positions_by_name)}")
            position = positions_by_name[field_name]
            # literal().to() turns Hive-style strings ("2024-01-02") into the field's type (date as days, ...).
            field_type = partition_fields[position].field_type
            partition_filter_by_position[position] = {literal(value).to(field_type).value for value in values}
        return partition_filter_by_position


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
            partition_spec=PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="category")),
        )
        for category, appends in (("a", 3), ("b", 2), ("c", 1)):
            for batch in range(appends):
                ids = range(batch * 100, (batch + 1) * 100)
                table.append(pa.table({"id": ids, "category": [category] * 100}, schema=table.schema().as_arrow()))

        plan = PackagePlanner(target_file_size_mib=128, partition_filter={"category": ["a", "c"]}).plan(table)
        print(f"snapshot {plan.snapshot_id}: {len(plan.groups)} group(s) to merge")
        for group in plan.groups:
            print(f"  {group.partition_path}: {len(group.data_files)} small file(s), {group.data_bytes} bytes")
