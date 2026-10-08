"""File-based sources: CSV drops and JSON-lines event files."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import date
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)
_DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})")


def file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_date(path: Path) -> date | None:
    match = _DATE_IN_NAME.search(path.name)
    return date.fromisoformat(match.group(1)) if match else None


def files_to_process(
    folder: Path,
    pattern: str,
    already_processed: dict[str, str],
    since: date | None = None,
    reprocess_all: bool = False,
) -> list[tuple[Path, str]]:
    """New or changed files, oldest first. Returns (path, checksum) pairs."""
    if not folder.exists():
        return []
    todo = []
    for path in sorted(folder.glob(pattern)):
        if since and (file_date(path) or date.min) < since:
            continue
        checksum = file_checksum(path)
        if reprocess_all or since or already_processed.get(path.name) != checksum:
            todo.append((path, checksum))
    return todo


def read_csv_as_text(path: Path) -> pd.DataFrame:
    """Read every column as text. Typing happens in validation, where bad values
    can be quarantined instead of crashing the read."""
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def read_jsonl(path: Path) -> tuple[pd.DataFrame, list[dict]]:
    """Parse JSON lines. Returns (good records as a frame, malformed lines)."""
    records, malformed = [], []
    with open(path, encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise ValueError("not a JSON object")
                records.append(obj)
            except ValueError:
                malformed.append({"file": path.name, "line_number": line_no, "raw_line": line})
    return pd.DataFrame.from_records(records), malformed
