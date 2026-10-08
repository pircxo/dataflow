"""Incremental extraction from the source OLTP database."""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.retry import with_retry

log = logging.getLogger(__name__)


@with_retry()
def extract_incremental(
    engine: Engine, table: str, cursor_column: str, since: datetime | None
) -> pd.DataFrame:
    """All rows changed since `since` (inclusive), oldest first. None = full extract."""
    sql = f"SELECT * FROM public.{table}"
    params = {}
    if since is not None:
        sql += f" WHERE {cursor_column} >= :since"
        params["since"] = since
    sql += f" ORDER BY {cursor_column}"
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
    log.info("Extracted %d rows from source %s (since=%s)", len(df), table, since)
    return df
