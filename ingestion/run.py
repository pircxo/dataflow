"""Run the ingestion pipeline.

Examples:
  python -m ingestion.run                                  # all sources, incremental
  python -m ingestion.run --sources orders,payments        # only some sources
  python -m ingestion.run --sources orders --since 2026-09-01   # backfill from a date
  python -m ingestion.run --full-refresh                   # re-read everything

Exit code 0 = every source succeeded, 1 = at least one failed.
The last line printed is a JSON summary (Airflow reads it via XCom).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import date, datetime, timezone

from ingestion.db import bootstrap_warehouse, source_engine, warehouse_engine
from ingestion.pipeline import RunOptions, build_jobs, run_entity

log = logging.getLogger("ingestion")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sources", default="all", help="comma-separated entity names, or 'all'")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--since", type=date.fromisoformat, default=None, help="backfill from YYYY-MM-DD")
    parser.add_argument("--full-refresh", action="store_true")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    args = parse_args(argv)
    run_id = args.run_id or f"manual__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
    opts = RunOptions(run_id=run_id, full_refresh=args.full_refresh, since=args.since)

    wh = warehouse_engine()
    bootstrap_warehouse(wh)
    jobs = build_jobs(wh, source_engine, opts)

    wanted = list(jobs) if args.sources == "all" else [s.strip() for s in args.sources.split(",")]
    unknown = set(wanted) - set(jobs)
    if unknown:
        log.error("Unknown sources: %s. Choose from: %s", sorted(unknown), ", ".join(jobs))
        return 2

    log.info("Run %s starting: %s", run_id, ", ".join(wanted))
    results = [run_entity(name, jobs[name], wh, run_id) for name in wanted]

    failed = [m.entity for m in results if m.status != "success"]
    summary = {
        "run_id": run_id,
        "status": "failed" if failed else "success",
        "failed_sources": failed,
        "records_extracted": sum(m.rows_extracted for m in results),
        "records_loaded": sum(m.rows_valid for m in results),
        "records_rejected": sum(m.rows_quarantined for m in results),
        "records_inserted": sum(m.rows_inserted for m in results),
        "records_updated": sum(m.rows_updated for m in results),
        "duration_seconds": round(sum(m.duration_seconds for m in results), 2),
    }
    print(json.dumps(summary))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
