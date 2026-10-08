"""Retry with exponential backoff + jitter for transient failures.

Only *transient* errors are retried (network blips, timeouts, HTTP 429/5xx,
dropped DB connections). Bugs and bad requests (HTTP 4xx, SQL errors) fail fast,
because retrying them only wastes time.

Every load is idempotent, so re-running a step after a partial failure is safe.
"""

from __future__ import annotations

import logging

import requests
from sqlalchemy import exc as sa_exc
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from ingestion.config import settings

log = logging.getLogger(__name__)

RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}


def is_transient(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError):
        return exc.response is not None and exc.response.status_code in RETRYABLE_HTTP_STATUS
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, (sa_exc.OperationalError, sa_exc.InterfaceError)):
        return True
    if isinstance(exc, sa_exc.DBAPIError) and exc.connection_invalidated:
        return True
    return False


class wait_retry_after_or_backoff:
    """Honour the server's Retry-After header (HTTP 429) if present, else back off."""

    def __init__(self, initial: float, max_wait: float):
        self.backoff = wait_exponential_jitter(initial=initial, max=max_wait, jitter=1)
        self.max_wait = max_wait

    def __call__(self, state: RetryCallState) -> float:
        exc = state.outcome.exception() if state.outcome else None
        if isinstance(exc, requests.HTTPError) and exc.response is not None:
            header = exc.response.headers.get("Retry-After")
            if header and header.isdigit():
                return min(float(header), self.max_wait)
        return self.backoff(state)


def _log_before_sleep(state: RetryCallState) -> None:
    exc = state.outcome.exception() if state.outcome else None
    log.warning(
        "Transient error in %s (attempt %d): %s. Retrying in %.1fs",
        getattr(state.fn, "__qualname__", state.fn),
        state.attempt_number,
        exc,
        state.next_action.sleep if state.next_action else 0,
    )


def with_retry(attempts: int | None = None, initial_wait: float = 1.0, max_wait: float | None = None):
    """Decorator: retry transient errors with exponential backoff and jitter."""
    return retry(
        retry=retry_if_exception(is_transient),
        stop=stop_after_attempt(attempts or settings.retry_attempts),
        wait=wait_retry_after_or_backoff(initial_wait, max_wait or settings.retry_max_wait_seconds),
        before_sleep=_log_before_sleep,
        reraise=True,
    )
