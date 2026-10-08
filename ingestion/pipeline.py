"""Per-source ingestion jobs: extract -> validate -> load raw / quarantine -> save state.

Each source runs on its own. If one fails (say the API is down even after retries),
the others still load, the failed source keeps its old watermark, and the next run
picks up where it stopped.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from ingestion import state
from ingestion.config import settings
from ingestion.entities import (
    CLICKSTREAM_EVENTS,
    CUSTOMERS,
    ORDER_ITEMS,
    ORDERS,
    PAYMENTS,
    PRODUCTS,
    EntitySpec,
)
from ingestion.extractors import api, files, postgres
from ingestion.loaders.warehouse import dedupe_latest, frame_to_payloads, quarantine, upsert_raw
from ingestion.retry import with_retry
from ingestion.validators import validate

log = logging.getLogger(__name__)


@dataclass
class RunOptions:
    run_id: str
    full_refresh: bool = False  # ignore watermarks / processed files
    since: date | None = None  # backfill from this date (keeps the watermark moving forward)


@dataclass
class EntityMetrics:
    entity: str
    status: str = "running"
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: datetime | None = None
    rows_extracted: int = 0
    rows_valid: int = 0
    rows_quarantined: int = 0
    rows_deduplicated: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_unchanged: int = 0
    files_processed: int = 0
    watermark_before: datetime | None = None
    watermark_after: datetime | None = None
    error_message: str | None = None

    @property
    def duration_seconds(self) -> float:
        end = self.finished_at or datetime.now(timezone.utc)
        return round((end - self.started_at).total_seconds(), 2)

    def as_dict(self) -> dict:
        d = asdict(self)
        d["duration_seconds"] = self.duration_seconds
        return d


# ---------------------------------------------------------------------------
# Core: one batch
# ---------------------------------------------------------------------------
def process_batch(
    conn: Connection,
    spec: EntitySpec,
    source_df: pd.DataFrame,
    source_ref: str,
    run_id: str,
    metrics: EntityMetrics,
) -> None:
    """Validate a batch, upsert the good rows, quarantine the bad ones."""
    metrics.rows_extracted += len(source_df)
    if source_df.empty:
        return

    source_df = source_df.reset_index(drop=True)
    typed = spec.prepare(source_df)
    result = validate(typed, spec.all_rules)

    deduped, n_dupes = dedupe_latest(result.valid, spec)
    load = upsert_raw(conn, spec, deduped, source_ref, run_id)

    bad_rows = source_df.loc[result.invalid_index]
    keys = [
        None if pd.isna(k) else str(k)
        for k in (bad_rows[spec.primary_key] if spec.primary_key in bad_rows else [None] * len(bad_rows))
    ]
    quarantine(
        conn,
        spec.name,
        frame_to_payloads(bad_rows),
        list(result.reasons.loc[result.invalid_index]),
        keys,
        source_ref,
        run_id,
    )

    metrics.rows_valid += len(result.valid)
    metrics.rows_quarantined += len(result.invalid_index)
    metrics.rows_deduplicated += n_dupes
    metrics.rows_inserted += load.inserted
    metrics.rows_updated += load.updated
    metrics.rows_unchanged += load.unchanged


def _start_point(
    conn: Connection, source_name: str, opts: RunOptions
) -> tuple[datetime | None, datetime | None]:
    """Returns (stored watermark, where to start reading)."""
    stored = state.get_watermark(conn, source_name)
    if opts.full_refresh:
        return stored, None
    if opts.since:
        return stored, datetime.combine(opts.since, datetime.min.time(), tzinfo=timezone.utc)
    if stored is None:
        return None, None
    return stored, stored - timedelta(minutes=settings.lookback_minutes)


# ---------------------------------------------------------------------------
# Source runners
# ---------------------------------------------------------------------------
def run_postgres_entity(
    spec: EntitySpec, wh: Engine, src: Engine, opts: RunOptions, m: EntityMetrics
) -> None:
    source_name = f"postgres.{spec.name}"

    @with_retry()
    def attempt() -> None:
        # Retrying the whole extract+load is safe because loading is idempotent.
        m.rows_extracted = m.rows_valid = m.rows_quarantined = m.rows_deduplicated = 0
        m.rows_inserted = m.rows_updated = m.rows_unchanged = 0
        with wh.begin() as conn:
            stored, since = _start_point(conn, source_name, opts)
            m.watermark_before = stored
            df = postgres.extract_incremental(src, spec.name, "updated_at", since)
            process_batch(conn, spec, df, f"postgres:public.{spec.name}", opts.run_id, m)
            if not df.empty:
                new_mark = pd.to_datetime(df["updated_at"], utc=True).max().to_pydatetime()
                state.set_watermark(conn, source_name, new_mark, opts.run_id)
            m.watermark_after = state.get_watermark(conn, source_name)

    attempt()


def run_payments_api(wh: Engine, opts: RunOptions, m: EntityMetrics) -> None:
    spec, source_name = PAYMENTS, "api.payments"
    with wh.connect() as conn:
        stored, since = _start_point(conn, source_name, opts)
    m.watermark_before = stored

    records = api.fetch_payments(since)  # retries per page inside
    df = pd.DataFrame.from_records(records)
    with wh.begin() as conn:
        process_batch(conn, spec, df, "api:/payments", opts.run_id, m)
        if not df.empty:
            marks = pd.to_datetime(df["updated_at"], utc=True, errors="coerce", format="ISO8601")
            if marks.notna().any():
                state.set_watermark(conn, source_name, marks.max().to_pydatetime(), opts.run_id)
        m.watermark_after = state.get_watermark(conn, source_name)


def run_file_entity(
    spec: EntitySpec,
    folder_name: str,
    pattern: str,
    reader: Callable,
    wh: Engine,
    opts: RunOptions,
    m: EntityMetrics,
) -> None:
    source_name = f"files.{spec.name}"
    folder = settings.data_dir / folder_name
    with wh.connect() as conn:
        done = state.processed_files(conn, source_name)
    todo = files.files_to_process(folder, pattern, done, since=opts.since, reprocess_all=opts.full_refresh)
    log.info("%s: %d file(s) to process in %s", spec.name, len(todo), folder)

    # One transaction per file: a crash keeps the files already finished.
    for path, checksum in todo:
        result = reader(path)
        df, malformed = result if isinstance(result, tuple) else (result, [])
        with wh.begin() as conn:
            process_batch(conn, spec, df, f"file:{path.name}", opts.run_id, m)
            if malformed:
                quarantine(
                    conn,
                    spec.name,
                    malformed,
                    [["malformed_json"]] * len(malformed),
                    [None] * len(malformed),
                    f"file:{path.name}",
                    opts.run_id,
                )
                m.rows_extracted += len(malformed)
                m.rows_quarantined += len(malformed)
            state.mark_file_processed(
                conn, source_name, path.name, checksum, len(df) + len(malformed), opts.run_id
            )
        m.files_processed += 1


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
def build_jobs(wh: Engine, src_factory: Callable[[], Engine], opts: RunOptions) -> dict[str, Callable]:
    """Entity name -> function(metrics). Order matters only for readability."""
    return {
        "customers": lambda m: run_postgres_entity(CUSTOMERS, wh, src_factory(), opts, m),
        "orders": lambda m: run_postgres_entity(ORDERS, wh, src_factory(), opts, m),
        "order_items": lambda m: run_postgres_entity(ORDER_ITEMS, wh, src_factory(), opts, m),
        "products": lambda m: run_file_entity(
            PRODUCTS, "products", "products_*.csv", files.read_csv_as_text, wh, opts, m
        ),
        "payments": lambda m: run_payments_api(wh, opts, m),
        "clickstream_events": lambda m: run_file_entity(
            CLICKSTREAM_EVENTS, "clickstream", "events_*.jsonl", files.read_jsonl, wh, opts, m
        ),
    }


def record_metrics(wh: Engine, run_id: str, m: EntityMetrics) -> None:
    with wh.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO observability.ingestion_runs
                    (run_id, entity, status, started_at, finished_at, duration_seconds,
                     rows_extracted, rows_valid, rows_quarantined, rows_deduplicated,
                     rows_inserted, rows_updated, rows_unchanged, watermark_before,
                     watermark_after, files_processed, error_message)
                VALUES
                    (:run_id, :entity, :status, :started_at, :finished_at, :duration_seconds,
                     :rows_extracted, :rows_valid, :rows_quarantined, :rows_deduplicated,
                     :rows_inserted, :rows_updated, :rows_unchanged, :watermark_before,
                     :watermark_after, :files_processed, :error_message)
                """
            ),
            {"run_id": run_id, **m.as_dict()},
        )


def run_entity(name: str, job: Callable[[EntityMetrics], None], wh: Engine, run_id: str) -> EntityMetrics:
    m = EntityMetrics(entity=name)
    t0 = time.monotonic()
    try:
        job(m)
        m.status = "success"
    except Exception as exc:  # noqa: BLE001 - one source failing must not stop the others
        log.exception("Ingestion of %s failed", name)
        m.status = "failed"
        m.error_message = f"{type(exc).__name__}: {exc}"[:2000]
    m.finished_at = datetime.now(timezone.utc)
    log.info(
        "%-20s %-7s extracted=%d valid=%d quarantined=%d inserted=%d updated=%d (%.1fs)",
        name,
        m.status,
        m.rows_extracted,
        m.rows_valid,
        m.rows_quarantined,
        m.rows_inserted,
        m.rows_updated,
        time.monotonic() - t0,
    )
    try:
        record_metrics(wh, run_id, m)
    except Exception:  # noqa: BLE001 - metrics must never hide the real outcome
        log.exception("Could not record metrics for %s", name)
    return m
