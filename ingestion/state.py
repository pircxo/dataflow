"""Pipeline state: high-water marks and processed-file bookkeeping.

State is written in the SAME transaction as the data it describes, so a crash can
never leave "watermark moved forward but data not loaded" (or the reverse).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Connection


def get_watermark(conn: Connection, source_name: str) -> datetime | None:
    return conn.execute(
        text("SELECT watermark FROM observability.ingestion_watermarks WHERE source_name = :s"),
        {"s": source_name},
    ).scalar_one_or_none()


def set_watermark(conn: Connection, source_name: str, watermark: datetime, run_id: str) -> None:
    # GREATEST: a backfill (--since) that re-reads old data never moves the mark backwards.
    conn.execute(
        text(
            """
            INSERT INTO observability.ingestion_watermarks (source_name, watermark, updated_at, run_id)
            VALUES (:s, :w, now(), :r)
            ON CONFLICT (source_name) DO UPDATE
               SET watermark  = GREATEST(observability.ingestion_watermarks.watermark, EXCLUDED.watermark),
                   updated_at = now(),
                   run_id     = EXCLUDED.run_id
            """
        ),
        {"s": source_name, "w": watermark, "r": run_id},
    )


def processed_files(conn: Connection, source_name: str) -> dict[str, str]:
    """file_name -> checksum of files already loaded."""
    rows = conn.execute(
        text("SELECT file_name, file_checksum FROM observability.processed_files WHERE source_name = :s"),
        {"s": source_name},
    ).all()
    return {name: checksum for name, checksum in rows}


def mark_file_processed(
    conn: Connection, source_name: str, file_name: str, checksum: str, rows_read: int, run_id: str
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO observability.processed_files
                   (source_name, file_name, file_checksum, rows_read, processed_at, run_id)
            VALUES (:s, :f, :c, :n, now(), :r)
            ON CONFLICT (source_name, file_name) DO UPDATE
               SET file_checksum = EXCLUDED.file_checksum,
                   rows_read     = EXCLUDED.rows_read,
                   processed_at  = now(),
                   run_id        = EXCLUDED.run_id
            """
        ),
        {"s": source_name, "f": file_name, "c": checksum, "n": rows_read, "r": run_id},
    )
