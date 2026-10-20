import tempfile
from collections.abc import Generator
from contextlib import closing, contextmanager

import duckdb
import pyarrow as pa
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
from iceberg_compaction.execution import CompactionEngine, RewriteJob
from iceberg_compaction.planning import TrackedFile

SOURCE_FILE_COLUMN = "__source_file"
ROW_POSITION_COLUMN = "file_row_number"
POSITION_DELETE_READ_SCHEMA = (
    f"MAP {{{PATH_FIELD_ID}: {{name: 'file_path', type: 'VARCHAR', default_value: NULL}}, "
    f"{PATH_FIELD_ID - 1}: {{name: 'pos', type: 'BIGINT', default_value: NULL}}}}"
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
    def __init__(self, staging_dir: str | None = None):
        self.staging_dir = staging_dir
        self._connection = duckdb.connect(config={"TimeZone": "UTC", "arrow_lossless_conversion": True})

    def close(self) -> None:
        self._connection.close()

    @contextmanager
    def read_rows(self, job: RewriteJob) -> Generator[pa.RecordBatchReader]:
        with (
            tempfile.TemporaryDirectory(prefix="compaction-", dir=self.staging_dir) as staging_dir,
            closing(self._connection.cursor()) as cursor,
        ):
            data_paths = stage_files(job.io, job.data_files, staging_dir, "data")
            position_delete_paths = stage_files(job.io, job.position_delete_files, staging_dir, "position-delete")
            equality_delete_paths = stage_files(job.io, job.equality_delete_files, staging_dir, "equality-delete")
            query = build_query(job, data_paths, position_delete_paths, equality_delete_paths)
            yield cursor.execute(query).to_arrow_reader()


def build_query(
    job: RewriteJob, data_paths: list[str], position_delete_paths: list[str], equality_delete_paths: list[str]
) -> str:
    if reserved_names := {schema_field.name for schema_field in job.schema.fields} & {SOURCE_FILE_COLUMN, ROW_POSITION_COLUMN}:
        raise ValueError(f"Column names reserved by the DuckDB engine: {sorted(reserved_names)}")
    columns = ", ".join(f"d.{quote_identifier(schema_field.name)}" for schema_field in job.schema.fields)
    query = (
        f"SELECT {columns}\n"
        f"FROM {build_read_parquet(data_paths, job.schema.fields)} AS d\n"
        f"JOIN {format_files_table(data_paths, job.data_files)} AS f(local_path, original_path, sequence_number) "
        f"ON f.local_path = d.{SOURCE_FILE_COLUMN}"
    )

    conditions = []
    if position_delete_paths:
        conditions.append(
            f"NOT EXISTS (SELECT 1 FROM read_parquet({format_list_literal(position_delete_paths)}, "
            f"schema={POSITION_DELETE_READ_SCHEMA}) AS p WHERE p.file_path = f.original_path AND p.pos = d.{ROW_POSITION_COLUMN})"
        )
    top_level_fields = {schema_field.field_id: schema_field for schema_field in job.schema.fields}
    equality_ids = {tuple(delete_file.file.equality_ids or ()) for delete_file in job.equality_delete_files}
    for ids in equality_ids:
        if not set(ids) <= top_level_fields.keys():
            raise NotImplementedError(f"Equality deletes on nested or dropped field ids are not supported: {ids}")
        key_fields = tuple(top_level_fields[field_id] for field_id in ids)
        layout = [
            (path, delete_file)
            for path, delete_file in zip(equality_delete_paths, job.equality_delete_files, strict=True)
            if tuple(delete_file.file.equality_ids or ()) == ids
        ]
        paths = [path for path, _ in layout]
        key_matches = " AND ".join(
            f"e.{quote_identifier(key.name)} IS NOT DISTINCT FROM d.{quote_identifier(key.name)}" for key in key_fields
        )
        conditions.append(
            f"NOT EXISTS (SELECT 1 FROM {build_read_parquet(paths, key_fields)} AS e "
            f"JOIN {format_files_table(paths, [delete_file for _, delete_file in layout])} "
            f"AS ef(local_path, original_path, sequence_number) ON ef.local_path = e.{SOURCE_FILE_COLUMN} "
            f"WHERE {key_matches} AND ef.sequence_number > f.sequence_number)"
        )
    if conditions:
        query += "\nWHERE " + "\n  AND ".join(conditions)

    if job.sort_columns:
        query += "\nORDER BY " + ", ".join(
            f"d.{quote_identifier(column.name)} {'DESC' if column.descending else 'ASC'} "
            f"{'NULLS FIRST' if column.nulls_first else 'NULLS LAST'}"
            for column in job.sort_columns
        )
    return query


def build_read_parquet(paths: list[str], fields: tuple[NestedField, ...]) -> str:
    read_schema = ", ".join(
        f"{schema_field.field_id}: {{name: {quote_literal(schema_field.name)}, "
        f"type: {quote_literal(map_sql_type(schema_field.field_type))}, default_value: NULL}}"
        for schema_field in fields
    )
    return (
        f"read_parquet({format_list_literal(paths)}, schema=MAP {{{read_schema}}}, "
        f"filename={quote_literal(SOURCE_FILE_COLUMN)}, file_row_number=true)"
    )


def format_files_table(local_paths: list[str], files: list[TrackedFile] | tuple[TrackedFile, ...]) -> str:
    rows = ", ".join(
        f"({quote_literal(local_path)}, {quote_literal(tracked_file.file.file_path)}, {tracked_file.sequence_number})"
        for local_path, tracked_file in zip(local_paths, files, strict=True)
    )
    return f"(VALUES {rows})"


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


def format_list_literal(values: list[str]) -> str:
    return "[" + ", ".join(quote_literal(value) for value in values) + "]"


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
