# Goal

The goal of this tool is to rewrite data files of an Iceberg Table, into "near" a target size.
- Takes into consideration Hive Partitions
- Merge Small files
- Apply delete files: position + equality, re-written files will not carry deleted rows
- sort rows using the table's sort order

# High Level Architecture (still working on it)

- Plan the compaction by reading Catalog's manifests
- Rewrite all files (We need to be able to easily switch the engine used to re-write files)
- Commit all rewritten files into one atomic snapshot

## Roadmap
| Phase | Feature | Status |
|---|---|---|
| Planning | Plan small-file merges (unpartitioned tables) | Done |
| Planning | Target size from table properties | Planned |
| Planning | Per-partition planning | Planned |
| Planning | Partition filter | Planned |
| Planning | Plan metrics and logs | Planned |