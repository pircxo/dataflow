"""Idempotent loading into `raw`, and quarantining of rejected rows.

Load pattern (staging-merge):
  1. COPY the batch into a temp table           (fast bulk load)
  2. INSERT ... SELECT ... ON CONFLICT DO UPDATE (merge into raw by primary key)
       - skip the update if nothing changed     (_record_hash is the same)
       - skip the update if the incoming version is OLDER than what we have
  3. Everything happens in the caller's transaction, together with the watermark.

Running the same batch twice gives the same result -> safe to retry and backfill.
"""

from __future__ import annotations

import hashlib
import io
import json
from dataclasses import dataclass

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from ingestion.entities import EntitySpec

META_COLUMNS = ["_record_hash", "_source", "_run_id"]


@dataclass
class LoadResult:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0


def _jsonable(value):
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _md5(obj) -> str:
    return hashlib.md5(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def record_hashes(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    """Stable fingerprint of the business columns of each row."""
    records = df[columns].astype(object).to_dict(orient="records")
    return pd.Series([_md5({k: _jsonable(v) for k, v in r.items()}) for r in records], index=df.index)


def dedupe_latest(df: pd.DataFrame, spec: EntitySpec) -> tuple[pd.DataFrame, int]:
    """Keep one row per primary key: the newest version (last one wins on ties)."""
    if df.empty:
        return df, 0
    ordered = df
    if spec.version_column:
        ordered = df.sort_values(spec.version_column, kind="stable")
    deduped = ordered.drop_duplicates(subset=[spec.primary_key], keep="last")
    return deduped, len(df) - len(deduped)


def _to_copy_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object and out[col].map(lambda v: isinstance(v, (dict, list))).any():
            out[col] = out[col].map(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else None)
    return out


def upsert_raw(conn: Connection, spec: EntitySpec, df: pd.DataFrame, source: str, run_id: str) -> LoadResult:
    if df.empty:
        return LoadResult()

    batch = df[spec.columns].copy()
    batch["_record_hash"] = record_hashes(batch, spec.columns)
    batch["_source"] = source
    batch["_run_id"] = run_id
    columns = spec.columns + META_COLUMNS
    col_list = ", ".join(columns)
    tmp = f"tmp_load_{spec.name}"

    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {tmp}")
    conn.exec_driver_sql(f"CREATE TEMP TABLE {tmp} (LIKE {spec.raw_table} INCLUDING DEFAULTS) ON COMMIT DROP")

    buffer = io.StringIO()
    _to_copy_frame(batch[columns]).to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)
    cursor = conn.connection.driver_connection.cursor()
    cursor.copy_expert(f"COPY {tmp} ({col_list}) FROM STDIN WITH (FORMAT csv)", buffer)

    updates = [f"{c} = EXCLUDED.{c}" for c in columns if c != spec.primary_key]
    updates.append("_ingested_at = now()")
    condition = "t._record_hash IS DISTINCT FROM EXCLUDED._record_hash"
    if spec.version_column:
        condition += f" AND EXCLUDED.{spec.version_column} >= t.{spec.version_column}"

    result = conn.exec_driver_sql(
        f"""
        INSERT INTO {spec.raw_table} AS t ({col_list})
        SELECT {col_list} FROM {tmp}
        ON CONFLICT ({spec.primary_key}) DO UPDATE
           SET {', '.join(updates)}
         WHERE {condition}
        RETURNING (xmax = 0) AS inserted
        """
    )
    flags = result.scalars().all()
    inserted = sum(1 for f in flags if f)
    updated = len(flags) - inserted
    return LoadResult(inserted=inserted, updated=updated, unchanged=len(batch) - len(flags))


def quarantine(
    conn: Connection,
    entity: str,
    payloads: list[dict],
    reasons: list[list[str]],
    record_keys: list[str | None],
    source: str,
    run_id: str,
) -> int:
    """Store rejected records exactly as received. Returns number of records."""
    if not payloads:
        return 0
    grouped: dict[str, dict] = {}
    for payload, why, key in zip(payloads, reasons, record_keys):
        h = _md5(payload)
        if h in grouped:  # same bad record twice in one batch
            grouped[h]["times_seen"] += 1
            continue
        grouped[h] = {
            "entity": entity,
            "record_key": key,
            "reasons": sorted(why),
            "payload": json.dumps(payload, default=str),
            "payload_hash": h,
            "source": source,
            "run_id": run_id,
            "times_seen": 1,
        }
    conn.execute(
        text(
            """
            INSERT INTO quarantine.rejected_records
                   (entity, record_key, reasons, payload, payload_hash, source, run_id, times_seen)
            VALUES (:entity, :record_key, :reasons, CAST(:payload AS jsonb), :payload_hash,
                    :source, :run_id, :times_seen)
            ON CONFLICT (entity, payload_hash) DO UPDATE
               SET last_seen_at = now(),
                   times_seen   = quarantine.rejected_records.times_seen + EXCLUDED.times_seen,
                   run_id       = EXCLUDED.run_id,
                   reasons      = EXCLUDED.reasons
            """
        ),
        list(grouped.values()),
    )
    return len(payloads)


def frame_to_payloads(df: pd.DataFrame) -> list[dict]:
    """Original source rows -> JSON-safe dicts (timestamps as ISO strings)."""
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso", default_handler=str))
