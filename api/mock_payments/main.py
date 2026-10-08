"""Mock payments REST API (FastAPI).

Serves the payment records written by the data generator, like a payment
provider's "list transactions" endpoint:

    GET /health
    GET /payments?updated_since=2026-10-01T00:00:00Z&page=1&page_size=500

Results are sorted by (updated_at, payment_id) and paginated.

To test the ingestion engine's retry logic, a share of requests fail on purpose
(API_FAILURE_RATE, default 0.1): random 500 / 503 errors and 429 rate limits
with a Retry-After header.
"""

from __future__ import annotations

import json
import os
import random
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

PAYMENTS_FILE = Path(os.getenv("PAYMENTS_FILE", "data/generated/payments.json"))
FAILURE_RATE = float(os.getenv("API_FAILURE_RATE", "0.1"))

app = FastAPI(title="Mock Payments API", version="1.0")

_cache: dict = {"mtime": None, "rows": []}


def load_payments() -> list[dict]:
    """Re-read the file only when the generator has changed it."""
    if not PAYMENTS_FILE.exists():
        return []
    mtime = PAYMENTS_FILE.stat().st_mtime
    if _cache["mtime"] != mtime:
        rows = json.loads(PAYMENTS_FILE.read_text())
        rows.sort(key=lambda r: (r["updated_at"], r["payment_id"]))
        _cache.update(mtime=mtime, rows=rows)
    return _cache["rows"]


def parse_ts(value: str) -> str:
    """Validate an ISO-8601 timestamp and normalise it to the stored format."""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid updated_since: {value!r}")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def maybe_fail() -> JSONResponse | None:
    if random.random() >= FAILURE_RATE:
        return None
    kind = random.choice(["500", "503", "429"])
    if kind == "429":
        return JSONResponse({"error": "rate limit exceeded"}, status_code=429, headers={"Retry-After": "1"})
    return JSONResponse({"error": "upstream unavailable"}, status_code=int(kind))


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "records": len(load_payments())}


@app.get("/payments")
def list_payments(
    updated_since: str | None = Query(None, description="ISO-8601; returns records with updated_at >= this"),
    page: int = Query(1, ge=1),
    page_size: int = Query(500, ge=1, le=1000),
):
    failure = maybe_fail()
    if failure is not None:
        return failure

    rows = load_payments()
    if updated_since:
        since = parse_ts(updated_since)
        rows = [r for r in rows if r["updated_at"] >= since]

    total = len(rows)
    start = (page - 1) * page_size
    data = rows[start:start + page_size]
    has_more = start + page_size < total
    return {
        "data": data,
        "page": page,
        "page_size": page_size,
        "total": total,
        "next_page": page + 1 if has_more else None,
    }
