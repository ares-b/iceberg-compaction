import tempfile
from collections.abc import Generator
from contextlib import closing, contextmanager

import duckdb
import pyarrow as pa
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
            yield cursor.execute(build_query(job, data_paths)).to_arrow_reader()


def build_query(job: RewriteJob, data_paths: list[str]) -> str:
    columns = ", ".join(quote_identifier(schema_field.name) for schema_field in job.schema.fields)
    return f"SELECT {columns} FROM {build_read_parquet(data_paths, job.schema.fields)}"


def build_read_parquet(paths: list[str], fields: tuple[NestedField, ...]) -> str:
    read_schema = ", ".join(
        f"{schema_field.field_id}: {{name: {quote_literal(schema_field.name)}, "
        f"type: {quote_literal(map_sql_type(schema_field.field_type))}, default_value: NULL}}"
        for schema_field in fields
    )
    return f"read_parquet({format_list_literal(paths)}, schema=MAP {{{read_schema}}})"


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
