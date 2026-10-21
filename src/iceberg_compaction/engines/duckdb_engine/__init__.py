try:
    import duckdb  # noqa: F401
    import jinja2  # noqa: F401
except ImportError as error:
    raise ImportError('The DuckDB engine needs the duckdb extra: pip install "iceberg-compaction[duckdb]"') from error

from iceberg_compaction.engines.duckdb_engine.engine import DuckDBEngine

__all__ = ["DuckDBEngine"]
