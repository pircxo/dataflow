"""Data-quality guardrails.

A Rule is a name plus a function that returns a boolean Series:
True means "this row FAILS the rule". Each row collects the names of every rule
it fails, so the quarantine table explains *all* problems at once, not just the first.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

import pandas as pd

RuleCheck = Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class Rule:
    name: str
    check: RuleCheck


@dataclass
class ValidationResult:
    valid: pd.DataFrame  # typed rows that passed every rule
    invalid_index: pd.Index  # index labels of rows that failed
    reasons: pd.Series  # index -> list[str] of failed rule names (invalid rows only)


def validate(df: pd.DataFrame, rules: list[Rule]) -> ValidationResult:
    failures = {}
    for rule in rules:
        failed = rule.check(df)
        failed = failed.reindex(df.index).fillna(False).astype(bool)
        failures[rule.name] = failed
    if failures:
        fail_frame = pd.DataFrame(failures, index=df.index)
        bad = fail_frame.any(axis=1)
        reasons = fail_frame[bad].apply(lambda row: [n for n, v in row.items() if v], axis=1)
    else:
        bad = pd.Series(False, index=df.index)
        reasons = pd.Series(dtype=object)
    return ValidationResult(valid=df[~bad], invalid_index=df.index[bad], reasons=reasons)


# ---------------------------------------------------------------------------
# Parsing helpers: turn messy source values into typed columns.
# Anything that cannot be parsed becomes NaN/NaT, and a rule then rejects it.
# ---------------------------------------------------------------------------
_MONEY_JUNK = re.compile(r"[\s$,]")
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
MIN_VALID_TS = pd.Timestamp("2015-01-01", tz="UTC")


def parse_timestamp(series: pd.Series) -> pd.Series:
    """ISO-8601 strings (or datetimes) -> UTC timestamps. Garbage -> NaT."""
    if pd.api.types.is_datetime64_any_dtype(series):
        out = series
        return out.dt.tz_localize("UTC") if out.dt.tz is None else out.dt.tz_convert("UTC")
    as_text = series.where(series.isna(), series.astype(str))
    # Only accept strings that look like a date; bare numbers like "1234" are rejected.
    looks_like_date = as_text.str.match(r"^\d{4}-\d{2}-\d{2}", na=False)
    parsed = pd.to_datetime(as_text.where(looks_like_date), utc=True, errors="coerce", format="ISO8601")
    return parsed


def parse_money(series: pd.Series) -> pd.Series:
    """'$1,234.50' / '12.50' / 12.5 -> 12.5. 'N/A' -> NaN."""
    as_text = series.where(series.isna(), series.astype(str).str.replace(_MONEY_JUNK, "", regex=True))
    return pd.to_numeric(as_text, errors="coerce")


def parse_int(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    whole = numeric.where(numeric.isna() | (numeric == numeric.round()))
    return whole.astype("Int64")


def parse_bool(series: pd.Series) -> pd.Series:
    mapping = {"true": True, "1": True, "yes": True, "false": False, "0": False, "no": False}
    return series.astype(str).str.strip().str.lower().map(mapping).astype("boolean")


def blank_to_na(series: pd.Series) -> pd.Series:
    """Empty / whitespace-only strings -> NA."""
    if series.dtype == object:
        return series.where(~series.astype(str).str.strip().eq(""), pd.NA).where(series.notna(), pd.NA)
    return series


# ---------------------------------------------------------------------------
# Reusable rule builders
# ---------------------------------------------------------------------------
def not_null(column: str, name: str | None = None) -> Rule:
    """Value is missing in the source (unparseable values are caught by parse_failed)."""

    def check(df: pd.DataFrame) -> pd.Series:
        src = f"src__{column}"
        return df[src].isna() if src in df.columns else df[column].isna()

    return Rule(name or f"missing_{column}", check)


def parse_failed(column: str, original: str, name: str | None = None) -> Rule:
    """Value was present in the source but could not be parsed."""
    return Rule(
        name or f"invalid_{column}",
        lambda df: df[column].isna() & df[original].notna(),
    )


def non_negative(column: str, name: str | None = None) -> Rule:
    return Rule(name or f"negative_{column}", lambda df: df[column] < 0)


def positive(column: str, name: str | None = None) -> Rule:
    return Rule(name or f"non_positive_{column}", lambda df: df[column] <= 0)


def in_set(column: str, allowed: set[str], name: str | None = None, normalize: bool = True) -> Rule:
    def check(df: pd.DataFrame) -> pd.Series:
        values = df[column]
        if normalize:
            values = values.astype("string").str.strip().str.lower()
        return values.notna() & ~values.isin(allowed)

    return Rule(name or f"unknown_{column}", check)


def matches(column: str, pattern: str, name: str) -> Rule:
    return Rule(name, lambda df: df[column].notna() & ~df[column].astype(str).str.match(pattern))


def timestamp_in_range(column: str, name: str | None = None, future_slack_days: int = 1) -> Rule:
    def check(df: pd.DataFrame) -> pd.Series:
        upper = pd.Timestamp(datetime.now(timezone.utc) + timedelta(days=future_slack_days))
        ts = df[column]
        return ts.notna() & ((ts < MIN_VALID_TS) | (ts > upper))

    return Rule(name or f"{column}_out_of_range", check)
