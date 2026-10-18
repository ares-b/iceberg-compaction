from helpers import BY_CATEGORY, append_files, create_table

from iceberg_compaction import Planner


def list_groups(plan):
    return [(group.partition_path, len(group.data_files)) for group in plan.groups]


def test_small_files_are_merged_per_partition_when_it_saves_files(catalog):
    table = create_table(catalog, BY_CATEGORY)
    append_files(table, 2, category="a")
    append_files(table, category="b")
    append_files(table, rows=50_000, category="c")
    append_files(table, 2, category="c")

    assert list_groups(Planner(target_file_size_mib=1).plan(table)) == [("category=a", 2), ("category=c", 2)]


def test_target_defaults_to_the_table_property(catalog):
    table = create_table(catalog)
    with table.transaction() as transaction:
        transaction.set_properties({"write.target-file-size-bytes": str(1024**2)})

    assert Planner().plan(table).target_file_size_bytes == 1024**2


def test_partition_filter_keeps_matching_partitions(catalog):
    table = create_table(catalog, BY_CATEGORY)
    append_files(table, 2, category="a")
    append_files(table, 2, category="b")

    assert list_groups(Planner(target_file_size_mib=1, partition_filter={"category": ["b"]}).plan(table)) == [("category=b", 2)]
