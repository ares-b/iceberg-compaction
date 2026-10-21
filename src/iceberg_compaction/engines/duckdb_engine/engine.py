import tempfile
import threading
from collections.abc import Generator, Iterable, Sequence
from contextlib import closing, contextmanager

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from jinja2 import Environment, PackageLoader, StrictUndefined
from pyiceberg.table.delete_file_index import PATH_FIELD_ID
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

from iceberg_compaction.engines.staging import stage_files
from iceberg_compaction.execution import CompactionEngine, RewriteJob, compute_batch_rows
from iceberg_compaction.planning import TrackedFile

SOURCE_FILE_COLUMN = "__source_file"
ROW_POSITION_COLUMN = "file_row_number"
POSITION_DELETE_COLUMNS = (
    {"field_id": PATH_FIELD_ID, "name": "'file_path'", "type": "'VARCHAR'"},
    {"field_id": PATH_FIELD_ID - 1, "name": "'pos'", "type": "'BIGINT'"},
)
SQL_TEMPLATES = Environment(
    loader=PackageLoader("iceberg_compaction.engines.duckdb_engine", "sql"),
    undefined=StrictUndefined,
    trim_blocks=True,
    lstrip_blocks=True,
    autoescape=False,
)

PRIMITIVE_SQL_TYPES: dict[type[IcebergType], str] = {
    BooleanType: "BOOLEAN",
    IntegerType: "INTEGER",
    LongType: "BIGINT",
    FloatType: "FLOAT",
    DoubleType: "DOUBLE",
    DateType: "DATE",
    TimeType: "TIME",
    TimestampType: "TIMESTAMP",
    TimestamptzType: "TIMESTAMPTZ",
    StringType: "VARCHAR",
    UUIDType: "UUID",
    BinaryType: "BLOB",
    FixedType: "BLOB",
}


class DuckDBEngine(CompactionEngine):
    def __init__(
        self,
        memory_limit: str | None = None,
        threads: int | None = None,
        staging_dir: str | None = None,
        max_concurrent_downloads: int = 8,
    ):
        self.staging_dir = staging_dir
        self.max_concurrent_downloads = max_concurrent_downloads
        self._spill_dir = tempfile.TemporaryDirectory(prefix="duckdb-spill-", dir=staging_dir)
        config: dict[str, str | int | bool] = {
            "TimeZone": "UTC",
            "arrow_lossless_conversion": True,
            "temp_directory": self._spill_dir.name,
        }
        if memory_limit is not None:
            config["memory_limit"] = memory_limit
        if threads is not None:
            config["threads"] = threads
        self._connection = duckdb.connect(config=config)
        self._cursor_lock = threading.Lock()

    def close(self) -> None:
        self._connection.close()
        self._spill_dir.cleanup()

    @contextmanager
    def read_rows(self, job: RewriteJob) -> Generator[pa.RecordBatchReader]:
        with self._cursor_lock:
            cursor = self._connection.cursor()
        with tempfile.TemporaryDirectory(prefix="compaction-", dir=self.staging_dir) as staging_dir, closing(cursor):
            data_paths = stage_files(job.io, job.data_files, staging_dir, "data", self.max_concurrent_downloads)
            position_delete_paths = stage_files(
                job.io, job.position_delete_files, staging_dir, "position-delete", self.max_concurrent_downloads
            )
            equality_delete_paths = stage_files(
                job.io, job.equality_delete_files, staging_dir, "equality-delete", self.max_concurrent_downloads
            )
            batch_rows = compute_batch_rows(job.row_group_rows_limit, job.row_group_bytes_limit, *read_size_of_rows(data_paths))
            query = build_query(job, data_paths, position_delete_paths, equality_delete_paths)
            yield cursor.execute(query).to_arrow_reader(batch_rows)


def read_size_of_rows(paths: list[str]) -> tuple[int, int]:
    rows = uncompressed_bytes = 0
    for path in paths:
        metadata = pq.read_metadata(path)
        rows += metadata.num_rows
        uncompressed_bytes += sum(metadata.row_group(index).total_byte_size for index in range(metadata.num_row_groups))
    return rows, uncompressed_bytes


def build_query(
    job: RewriteJob, data_paths: list[str], position_delete_paths: list[str], equality_delete_paths: list[str]
) -> str:
    if reserved_names := {schema_field.name for schema_field in job.schema.fields} & {SOURCE_FILE_COLUMN, ROW_POSITION_COLUMN}:
        raise ValueError(f"Column names reserved by the DuckDB engine: {sorted(reserved_names)}")
    top_level_fields = {schema_field.field_id: schema_field for schema_field in job.schema.fields}
    equality_deletes = []
    for equality_ids in {tuple(delete_file.file.equality_ids or ()) for delete_file in job.equality_delete_files}:
        if not set(equality_ids) <= top_level_fields.keys():
            raise NotImplementedError(f"Equality deletes on nested or dropped field ids are not supported: {equality_ids}")
        paths_and_files = [
            (path, delete_file)
            for path, delete_file in zip(equality_delete_paths, job.equality_delete_files, strict=True)
            if tuple(delete_file.file.equality_ids or ()) == equality_ids
        ]
        equality_deletes.append(
            {
                "paths": [quote_literal(path) for path, _ in paths_and_files],
                "files": format_files([path for path, _ in paths_and_files], [delete_file for _, delete_file in paths_and_files]),
                "key_columns": format_columns([top_level_fields[field_id] for field_id in equality_ids]),
            }
        )
    return SQL_TEMPLATES.get_template("read_rows.sql.j2").render(
        source_file_column=SOURCE_FILE_COLUMN,
        row_position_column=ROW_POSITION_COLUMN,
        columns=format_columns(job.schema.fields),
        data_paths=[quote_literal(path) for path in data_paths],
        data_files=format_files(data_paths, job.data_files),
        position_delete_paths=[quote_literal(path) for path in position_delete_paths],
        position_delete_columns=POSITION_DELETE_COLUMNS,
        equality_deletes=equality_deletes,
        sort_columns=[
            {
                "identifier": quote_identifier(column.name),
                "direction": "DESC" if column.descending else "ASC",
                "nulls": "NULLS FIRST" if column.nulls_first else "NULLS LAST",
            }
            for column in job.sort_columns
        ],
    )


def format_columns(fields: Iterable[NestedField]) -> list[dict[str, str | int]]:
    return [
        {
            "field_id": schema_field.field_id,
            "name": quote_literal(schema_field.name),
            "identifier": quote_identifier(schema_field.name),
            "type": quote_literal(map_sql_type(schema_field.field_type)),
        }
        for schema_field in fields
    ]


def format_files(local_paths: list[str], files: Sequence[TrackedFile]) -> list[dict[str, str | int]]:
    return [
        {
            "local_path": quote_literal(local_path),
            "original_path": quote_literal(tracked_file.file.file_path),
            "sequence_number": tracked_file.sequence_number,
        }
        for local_path, tracked_file in zip(local_paths, files, strict=True)
    ]


def map_sql_type(field_type: IcebergType) -> str:
    if isinstance(field_type, StructType):
        return "STRUCT(" + ", ".join(f"{quote_identifier(f.name)} {map_sql_type(f.field_type)}" for f in field_type.fields) + ")"
    if isinstance(field_type, ListType):
        return f"{map_sql_type(field_type.element_type)}[]"
    if isinstance(field_type, MapType):
        return f"MAP({map_sql_type(field_type.key_type)}, {map_sql_type(field_type.value_type)})"
    if isinstance(field_type, DecimalType):
        return f"DECIMAL({field_type.precision}, {field_type.scale})"
    if type(field_type) not in PRIMITIVE_SQL_TYPES:
        raise NotImplementedError(f"Type not supported by the DuckDB engine: {field_type}")
    return PRIMITIVE_SQL_TYPES[type(field_type)]


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
