"""Database engines and warehouse bootstrap."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ingestion.config import settings

DDL_FILE = Path(__file__).parent / "sql" / "warehouse_ddl.sql"


@lru_cache(maxsize=None)
def warehouse_engine() -> Engine:
    return create_engine(settings.warehouse_url, pool_pre_ping=True, future=True)


@lru_cache(maxsize=None)
def source_engine() -> Engine:
    return create_engine(settings.source_db_url, pool_pre_ping=True, future=True)


def bootstrap_warehouse(engine: Engine | None = None) -> None:
    """Create schemas/tables if missing. Safe to run every time."""
    engine = engine or warehouse_engine()
    with engine.begin() as conn:
        # Serialise concurrent bootstraps (e.g. two runs starting at once).
        conn.exec_driver_sql("SELECT pg_advisory_xact_lock(hashtext('dataflow_bootstrap'))")
        conn.exec_driver_sql(DDL_FILE.read_text())
