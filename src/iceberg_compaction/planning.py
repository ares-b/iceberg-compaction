import math
from dataclasses import dataclass

from pyiceberg.manifest import DataFile
from pyiceberg.table import Table

from iceberg_compaction.log import LoggingMixin

MIB_TO_BYTES = 1024**2


@dataclass(frozen=True)
class CompactionPlan:
    snapshot_id: int | None
    target_file_size_bytes: int
    data_files: tuple[DataFile, ...]


class Planner(LoggingMixin, compaction_phase="plan"):
    def __init__(self, target_file_size_mib: int):
        if target_file_size_mib <= 0:
            raise ValueError(f"target_file_size_mib must be > 0, got {target_file_size_mib}")
        self.target_file_size_bytes = target_file_size_mib * MIB_TO_BYTES

    def plan(self, table: Table) -> CompactionPlan:
        if not table.spec().is_unpartitioned():
            raise NotImplementedError("Partitioned tables are not supported yet")
        table_name = ".".join(table.name())

        snapshot = table.current_snapshot()
        if snapshot is None:
            self.log.info("Nothing to compact: table has no snapshot", table=table_name)
            return CompactionPlan(snapshot_id=None, target_file_size_bytes=self.target_file_size_bytes, data_files=())

        small_files = [
            task.file for task in table.scan().plan_files() if task.file.file_size_in_bytes < self.target_file_size_bytes
        ]
        small_files_bytes = sum(data_file.file_size_in_bytes for data_file in small_files)
        output_file_count = math.ceil(small_files_bytes / self.target_file_size_bytes)
        if output_file_count >= len(small_files):
            self.log.info(
                f"Nothing to compact: merging {len(small_files)} small file(s) of {small_files_bytes} bytes "
                f"({small_files_bytes / MIB_TO_BYTES:.1f} MiB) would still give {output_file_count} file(s)",
                table=table_name,
                small_files=len(small_files),
                small_files_bytes=small_files_bytes,
                output_files=output_file_count,
            )
            small_files = []
        else:
            self.log.info(
                f"Planned merge of {len(small_files)} small files of {small_files_bytes} bytes "
                f"({small_files_bytes / MIB_TO_BYTES:.1f} MiB) into {output_file_count} file(s) "
                f"of up to {self.target_file_size_bytes / MIB_TO_BYTES:.0f} MiB",
                table=table_name,
                snapshot_id=snapshot.snapshot_id,
                small_files=len(small_files),
                small_files_bytes=small_files_bytes,
                output_files=output_file_count,
                target_file_size_bytes=self.target_file_size_bytes,
            )
        return CompactionPlan(
            snapshot_id=snapshot.snapshot_id, target_file_size_bytes=self.target_file_size_bytes, data_files=tuple(small_files)
        )


if __name__ == "__main__":
    import tempfile

    import pyarrow as pa
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    from iceberg_compaction.planning import Planner as PackagePlanner

    with tempfile.TemporaryDirectory() as warehouse:
        catalog = SqlCatalog("demo", uri=f"sqlite:///{warehouse}/catalog.db", warehouse=f"file://{warehouse}")
        catalog.create_namespace("db")
        table = catalog.create_table(
            "db.events", schema=Schema(NestedField(1, "id", LongType()), NestedField(2, "name", StringType()))
        )
        for batch in range(3):
            ids = range(batch * 100, (batch + 1) * 100)
            table.append(pa.table({"id": ids, "name": [f"event-{i}" for i in ids]}, schema=table.schema().as_arrow()))

        plan = PackagePlanner(target_file_size_mib=128).plan(table)
        print(f"snapshot {plan.snapshot_id}: {len(plan.data_files)} small file(s) to merge")
        for data_file in plan.data_files:
            print(f"  {data_file.file_path.removeprefix(f'file://{warehouse}/')} {data_file.file_size_in_bytes} bytes")
