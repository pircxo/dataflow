"""Paginated extraction from the payments REST API, with retries per page."""

from __future__ import annotations

import logging
from datetime import datetime

import requests

from ingestion.config import settings
from ingestion.retry import with_retry

log = logging.getLogger(__name__)


@with_retry()
def _get_page(session: requests.Session, url: str, params: dict) -> dict:
    response = session.get(url, params=params, timeout=settings.api_timeout_seconds)
    response.raise_for_status()  # 429/5xx -> HTTPError -> retried; other 4xx -> fail fast
    return response.json()


def fetch_payments(updated_since: datetime | None, session: requests.Session | None = None) -> list[dict]:
    """Return every payment with updated_at >= updated_since, following pagination.

    `updated_since` is fixed for the whole extraction, so pages are consistent even
    though each page is a separate request.
    """
    session = session or requests.Session()
    url = f"{settings.payments_api_url.rstrip('/')}/payments"
    params: dict = {"page": 1, "page_size": settings.api_page_size}
    if updated_since is not None:
        params["updated_since"] = updated_since.strftime("%Y-%m-%dT%H:%M:%SZ")

    records: list[dict] = []
    while True:
        body = _get_page(session, url, params)
        records.extend(body["data"])
        if not body.get("next_page"):
            break
        params["page"] = body["next_page"]
    log.info("Fetched %d payment records from API (%d pages)", len(records), params["page"])
    return records
