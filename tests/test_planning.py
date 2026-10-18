from helpers import append_files, create_table

from iceberg_compaction import Planner


def test_small_files_are_merged(catalog):
    table = create_table(catalog)
    append_files(table, 2)

    plan = Planner(target_file_size_mib=1).plan(table)

    assert len(plan.data_files) == 2


def test_lone_small_file_is_not_rewritten(catalog):
    table = create_table(catalog)
    append_files(table)

    assert Planner(target_file_size_mib=1).plan(table).data_files == ()


def test_files_over_the_target_are_not_selected(catalog):
    table = create_table(catalog)
    append_files(table, rows=50_000)
    append_files(table, 2)

    plan = Planner(target_file_size_mib=1).plan(table)

    assert sorted(data_file.record_count for data_file in plan.data_files) == [100, 100]
