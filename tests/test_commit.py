import pytest
from helpers import add_position_deletes, append_files, create_table, list_live_data_files
from pyiceberg.exceptions import ValidationException

from iceberg_compaction import Committer, Execution, Planner, compact
from iceberg_compaction.engines import DuckDBEngine


@pytest.fixture
def engine():
    with DuckDBEngine() as engine:
        yield engine


def test_compact_replaces_small_files_in_one_snapshot(catalog, engine):
    table = create_table(catalog)
    append_files(table, 3)
    rows_before = table.scan().to_arrow().sort_by("id")
    snapshot_count = len(table.snapshots())

    result = compact(table, Planner(target_file_size_mib=1), Execution(engine))

    assert (len(table.snapshots()), table.current_snapshot().snapshot_id) == (snapshot_count + 1, result.snapshot_id)
    assert len(list_live_data_files(table)) == 1
    assert table.scan().to_arrow().sort_by("id").equals(rows_before)


def test_compact_plans_and_commits_on_the_given_branch(catalog, engine):
    table = create_table(catalog)
    append_files(table, 2)
    main_snapshot_id = table.current_snapshot().snapshot_id
    table.manage_snapshots().create_branch(main_snapshot_id, "dev").commit()
    append_files(table, branch="dev")

    compact(table, Planner(branch="dev", target_file_size_mib=1), Execution(engine))

    assert table.current_snapshot().snapshot_id == main_snapshot_id
    assert len(table.scan(snapshot_id=table.snapshot_by_name("dev").snapshot_id).plan_files()) == 1


def test_commit_fails_on_a_delete_added_after_the_plan_and_deletes_written_files(catalog, engine, tmp_path):
    table = create_table(catalog)
    append_files(table, 2)
    result = Execution(engine).execute(Planner(target_file_size_mib=1).plan(table))
    add_position_deletes(table, list_live_data_files(table)[0], [0])

    with pytest.raises(ValidationException):
        Committer().commit(result)

    written_paths = {data_file.file_path for task in result.tasks for data_file in task.added_files}
    assert not written_paths & {f"file://{path}" for path in (tmp_path / "warehouse").rglob("*.parquet")}
