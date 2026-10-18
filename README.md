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

# Planner

The planner reads table metadata, never data files and returns a `CompactionPlan`, which is basically a list of `CompactionTask`
A task is a set of data files of one partition, plus the delete files that apply to them

```python
plan = Planner(strategy=Strategy.AUTO, target_file_size_mib=128).plan(table)
```

## How it plans

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant Catalog as Iceberg Catalog
    participant Planner
    participant Metadata as metadata.json
    participant ManifestList as manifest list
    participant Manifests as manifests

    User->>Catalog: load_table("db.events")
    Catalog->>Metadata: read the current metadata.json it points to
    Metadata-->>Catalog: schema, partition specs, properties, snapshots, branches
    Catalog-->>User: Table

    User->>Planner: plan(table)
    Planner->>Planner: copy the table, so the plan stays on this state
    Planner->>Metadata: target file size, current partition spec, snapshot of the branch
    Planner->>ManifestList: read the snapshot's manifest list
    ManifestList-->>Planner: manifests with their partition spec and partition ranges
    Planner->>Planner: keep manifests of the current partition spec matching the partition filter
    par each kept manifest, in parallel
        Planner->>Manifests: read entries
        Manifests-->>Planner: data and delete files, with sequence numbers and stats
    end
    Planner->>Planner: index data files by partition, delete files by partition spec and partition
    loop each partition
        Planner->>Planner: find the delete files that apply to each data file
        Planner->>Planner: select candidate files for the strategy
        Planner->>Planner: pack candidates into tasks, keep or skip each task
    end
    Planner-->>User: CompactionPlan(tasks, metrics)
```

# Rewriter
TODO