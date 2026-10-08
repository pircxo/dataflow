"""Check that every source (and the warehouse) is reachable before running.

  python -m ingestion.healthcheck        # exit 0 = all good, 1 = something is down

Transient problems are retried with backoff first, so a short blip does not
fail the pipeline; a real outage does, before any work is done.
"""

from __future__ import annotations

import json
import logging
import sys

import requests
from sqlalchemy import text

from ingestion.config import settings
from ingestion.db import source_engine, warehouse_engine
from ingestion.retry import with_retry

log = logging.getLogger("healthcheck")


@with_retry(attempts=4)
def _check_db(engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


@with_retry(attempts=4)
def _check_api() -> None:
    r = requests.get(f"{settings.payments_api_url.rstrip('/')}/health", timeout=5)
    r.raise_for_status()


def _check_folder(name: str, pattern: str) -> None:
    folder = settings.data_dir / name
    if not any(folder.glob(pattern)):
        raise FileNotFoundError(f"No {pattern} files in {folder}")


CHECKS = {
    "warehouse_db": lambda: _check_db(warehouse_engine()),
    "source_db": lambda: _check_db(source_engine()),
    "payments_api": _check_api,
    "product_files": lambda: _check_folder("products", "products_*.csv"),
    "clickstream_files": lambda: _check_folder("clickstream", "events_*.jsonl"),
}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    results = {}
    for name, check in CHECKS.items():
        try:
            check()
            results[name] = "ok"
        except Exception as exc:  # noqa: BLE001
            results[name] = f"FAILED: {type(exc).__name__}: {exc}"
        log.info("%-18s %s", name, results[name])
    print(json.dumps(results))
    return 0 if all(v == "ok" for v in results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
