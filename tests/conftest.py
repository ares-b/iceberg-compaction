from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog


@pytest.fixture
def catalog(tmp_path: Path) -> SqlCatalog:
    catalog = SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}/warehouse")
    catalog.create_namespace("db")
    return catalog

