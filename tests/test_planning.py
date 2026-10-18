from pathlib import Path

import pytest
from helpers import (
    BY_CATEGORY,
    add_equality_deletes,
    add_position_deletes,
    append_files,
    create_table,
    list_live_data_files,
)
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table.delete_file_index import PATH_FIELD_ID
from pyiceberg.typedef import Record

from iceberg_compaction import Planner, Strategy, UnsupportedDataFileTypeError
from iceberg_compaction.planning import (
    CompactionTask,
    PlanMetrics,
    TrackedFile,
    find_applicable_deletes,
    is_candidate,
    is_in_path_bounds,
    pack_into_tasks,
)


def make_tracked_file(path, sequence_number=1, size=10):
    return TrackedFile(
        DataFile.from_args(file_path=path, file_format=FileFormat.PARQUET, file_size_in_bytes=size), sequence_number
    )


def list_tasks(plan):
    return [(task.partition_path, len(task.data_files)) for task in plan.tasks]


def test_small_files_are_merged_per_partition_when_it_saves_files(catalog):
    table = create_table(catalog, BY_CATEGORY)
    append_files(table, 2, category="a")
    append_files(table, category="b")
    append_files(table, rows=50_000, category="c")
    append_files(table, 2, category="c")

    assert list_tasks(Planner(target_file_size_mib=1).plan(table)) == [("category=a", 2), ("category=c", 2)]


def test_target_defaults_to_the_table_property(catalog):
    table = create_table(catalog)
    with table.transaction() as transaction:
        transaction.set_properties({"write.target-file-size-bytes": str(1024**2)})

    assert Planner().plan(table).target_file_size_bytes == 1024**2


def test_plan_reads_the_given_branch(catalog):
    table = create_table(catalog)
    append_files(table)
    table.manage_snapshots().create_branch(table.current_snapshot().snapshot_id, "dev").commit()
    append_files(table, branch="dev")

    assert list_tasks(Planner(target_file_size_mib=1).plan(table)) == []
    assert list_tasks(Planner(target_file_size_mib=1, branch="dev").plan(table)) == [("", 2)]


def test_plan_keeps_the_table_as_planned(catalog):
    table = create_table(catalog)
    append_files(table, 2)

    plan = Planner(target_file_size_mib=1).plan(table)
    append_files(table)

    assert plan.table.current_snapshot().snapshot_id == plan.snapshot_id != table.current_snapshot().snapshot_id


def test_min_delete_file_count(catalog):
    table = create_table(catalog)
    append_files(table)
    add_position_deletes(table, list_live_data_files(table)[0], [0])

    assert Planner(strategy=Strategy.FILES_WITH_DELETES, min_delete_file_count=2).plan(table).tasks == ()


def test_pack_into_tasks_fills_tasks_largest_file_first():
    tasks = pack_into_tasks([make_tracked_file(f"{size}.parquet", size=size) for size in (3, 6, 2, 5, 4)], max_task_size_bytes=10)

    assert [[data_file.file.file_size_in_bytes for data_file in task] for task in tasks] == [[6, 4], [5, 3, 2]]


@pytest.mark.parametrize(
    ("partition_filter", "message"),
    [({"day": ["2024-01-01"]}, "Unknown partition field 'day'"), ({"category": []}, "No values for partition field 'category'")],
)
def test_partition_filter_rejects_invalid_filters(catalog, partition_filter, message):
    with pytest.raises(ValueError, match=message):
        Planner(partition_filter=partition_filter).plan(create_table(catalog, BY_CATEGORY))


def test_partition_filter_skips_manifests_of_other_partitions(catalog):
    table = create_table(catalog, BY_CATEGORY)
    append_files(table, 2, category="a")
    append_files(table, category="b")
    b_snapshot_id = table.current_snapshot().snapshot_id
    (b_manifest,) = [m for m in table.current_snapshot().manifests(table.io) if m.added_snapshot_id == b_snapshot_id]
    Path(b_manifest.manifest_path.removeprefix("file://")).unlink()

    assert list_tasks(Planner(target_file_size_mib=1, partition_filter={"category": ["a"]}).plan(table)) == [("category=a", 2)]


def test_files_of_an_older_partition_spec_are_ignored(catalog):
    table = create_table(catalog)
    append_files(table, 2)
    with table.update_spec() as update:
        update.add_identity("category")

    assert Planner(target_file_size_mib=1).plan(table).tasks == ()


def test_non_parquet_data_files_fail_the_plan(catalog):
    table = create_table(catalog)
    append_files(table, 2)
    orc_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=f"{table.location()}/data/file.orc",
        file_format=FileFormat.ORC,
        partition=Record(),
        record_count=1,
        file_size_in_bytes=10,
        spec_id=table.spec().spec_id,
    )
    with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
        append.append_data_file(orc_file)

    with pytest.raises(UnsupportedDataFileTypeError, match="Unsupported file format ORC: .*file.orc"):
        Planner(target_file_size_mib=1).plan(table)


def test_equality_deletes_of_an_unpartitioned_spec_apply_to_every_partition(catalog):
    table = create_table(catalog, BY_CATEGORY)
    append_files(table, category="a")
    with table.update_spec() as update:
        update.remove_field("category")
    add_equality_deletes(table, "id", [1])
    with table.update_spec() as update:
        update.add_identity("category")

    (task,) = Planner(strategy=Strategy.FILES_WITH_DELETES).plan(table).tasks

    assert (task.partition_path, len(task.equality_delete_files)) == ("category=a", 1)


def test_is_in_path_bounds_tells_which_data_files_a_position_delete_may_reference():
    def make_position_delete_file(lower, upper):
        return DataFile.from_args(
            file_path="deletes.parquet",
            file_format=FileFormat.PARQUET,
            lower_bounds={PATH_FIELD_ID: lower},
            upper_bounds={PATH_FIELD_ID: upper},
        )

    assert is_in_path_bounds(make_position_delete_file(b"s3://a/1.parquet", b"s3://a/3.parquet"), "s3://a/2.parquet")
    assert not is_in_path_bounds(make_position_delete_file(b"s3://a/1.parquet", b"s3://a/3.parquet"), "s3://a/4.parquet")
    assert is_in_path_bounds(DataFile.from_args(file_path="deletes.parquet", file_format=FileFormat.PARQUET), "s3://a/4.parquet")


def test_position_deletes_apply_from_their_own_commit_equality_deletes_only_to_older_rows():
    data_file = make_tracked_file("data.parquet", sequence_number=2)
    position_deletes = [
        make_tracked_file("pos-1.parquet", 1),
        make_tracked_file("pos-2.parquet", 2),
        make_tracked_file("pos-3.parquet", 3),
    ]
    equality_deletes = [make_tracked_file("eq-2.parquet", 2), make_tracked_file("eq-3.parquet", 3)]

    position, equality = find_applicable_deletes(data_file, position_deletes, equality_deletes)

    assert [delete.file.file_path for delete in position] == ["pos-2.parquet", "pos-3.parquet"]
    assert [delete.file.file_path for delete in equality] == ["eq-3.parquet"]


@pytest.mark.parametrize(
    ("strategy", "small_only", "deletes_only", "neither"),
    [
        (Strategy.SMALL_FILES, True, False, False),
        (Strategy.FILES_WITH_DELETES, False, True, False),
        (Strategy.AUTO, True, True, False),
        (Strategy.FULL, True, True, True),
    ],
)
def test_strategy_selects_candidates(strategy, small_only, deletes_only, neither):
    assert (
        is_candidate(strategy, is_small=True, has_deletes=False),
        is_candidate(strategy, is_small=False, has_deletes=True),
        is_candidate(strategy, is_small=False, has_deletes=False),
    ) == (small_only, deletes_only, neither)


def test_plan_metrics_compute_from_tasks():
    shared_delete = make_tracked_file("delete.parquet", size=5)
    tasks = [
        CompactionTask(
            Record(), "", (make_tracked_file("a.parquet", size=100), make_tracked_file("b.parquet", size=100)), (shared_delete,)
        ),
        CompactionTask(Record(), "", (make_tracked_file("c.parquet", size=50),), (shared_delete,)),
    ]

    metrics = PlanMetrics.compute(tasks, data_files_in_scope=5, duration_seconds=1.0)

    assert metrics == PlanMetrics(
        duration_seconds=1.0,
        tasks=2,
        data_files_to_rewrite=3,
        delete_files_to_apply=1,
        data_files_skipped=2,
        bytes_to_read=260,
        bytes_to_write=250,
    )


@pytest.mark.parametrize(
    ("strategy", "has_files_to_clean", "output_file_count", "min_task_file_count", "decision"),
    [
        (Strategy.FULL, False, 2, None, (True, "full rewrite")),
        (Strategy.AUTO, True, 2, 10, (True, "has delete files")),
        (Strategy.SMALL_FILES, False, 2, None, (False, "merging saves no file")),
        (Strategy.SMALL_FILES, False, 1, 3, (False, "below the minimum task size or file count")),
        (Strategy.SMALL_FILES, False, 1, None, (True, "merge 2 files into 1")),
    ],
)
def test_planner_decides_which_tasks_to_keep(strategy, has_files_to_clean, output_file_count, min_task_file_count, decision):
    task = CompactionTask(Record(), "", (make_tracked_file("a.parquet"), make_tracked_file("b.parquet")))

    planner = Planner(strategy=strategy, min_task_file_count=min_task_file_count)

    assert planner._decide_whether_to_keep(task, has_files_to_clean, output_file_count) == decision
