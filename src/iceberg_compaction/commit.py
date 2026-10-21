from pyiceberg.exceptions import CommitStateUnknownException

from iceberg_compaction.execution import ExecutionResult, delete_files
from iceberg_compaction.log import LoggingMixin


class Committer(LoggingMixin, compaction_phase="commit"):
    def commit(self, result: ExecutionResult) -> int | None:
        plan = result.plan
        if not result.tasks:
            self.log.info("Nothing to commit, the plan has no task", table=plan.table_name)
            return None
        removed_files = [data_file.file for rewritten_task in result.tasks for data_file in rewritten_task.task.data_files]
        added_files = [data_file for rewritten_task in result.tasks for data_file in rewritten_task.added_files]
        snapshot_properties = {
            "compaction.strategy": str(plan.strategy),
            "compaction.source-snapshot-id": str(plan.snapshot_id),
            "compaction.files-removed": str(len(removed_files)),
            "compaction.files-added": str(len(added_files)),
            "compaction.target-file-size-bytes": str(plan.target_file_size_bytes),
        }
        try:
            with plan.table.transaction() as transaction:
                update = transaction.update_snapshot(snapshot_properties=snapshot_properties, branch=plan.branch)
                with update.overwrite() as overwrite:
                    for data_file in removed_files:
                        overwrite.delete_data_file(data_file)
                    for data_file in added_files:
                        overwrite.append_data_file(data_file)
        except CommitStateUnknownException:
            self.log.exception(
                f"Commit state unknown, keeping written files: {len(added_files)}",
                table=plan.table_name,
                written_files=len(added_files),
            )
            raise
        except BaseException:
            self.log.exception(
                f"Commit failed, deleting written files: {len(added_files)}",
                table=plan.table_name,
                written_files=len(added_files),
            )
            for path in delete_files(plan.table.io, [data_file.file_path for data_file in added_files]):
                self.log.warning(f"Could not delete {path}, remove it by hand", table=plan.table_name, file_path=path)
            raise

        snapshot = plan.table.snapshot_by_name(plan.branch)
        snapshot_id = snapshot.snapshot_id if snapshot else None
        self.log.info(
            f"Commit OK. Snapshot {snapshot_id} on branch {plan.branch}. "
            f"Files removed: {len(removed_files)}, files added: {len(added_files)}",
            table=plan.table_name,
            snapshot_id=snapshot_id,
            branch=plan.branch,
            files_removed=len(removed_files),
            files_added=len(added_files),
        )
        return snapshot_id
