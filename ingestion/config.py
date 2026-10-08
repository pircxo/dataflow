"""All settings come from environment variables (or .env when run on a laptop)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def _pg_url(prefix: str, default_db: str, default_user: str, default_port: str) -> str:
    user = os.getenv(f"{prefix}_USER", default_user)
    password = os.getenv(f"{prefix}_PASSWORD", default_user)
    host = os.getenv(f"{prefix}_HOST", "localhost")
    port = os.getenv(f"{prefix}_PORT", default_port)
    db = os.getenv(f"{prefix}_DB") or os.getenv(f"{prefix}_NAME") or default_db
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def _data_dir() -> Path:
    path = Path(os.getenv("DATA_DIR", "data/generated"))
    return path if path.is_absolute() else PROJECT_ROOT / path


@dataclass(frozen=True)
class Settings:
    warehouse_url: str = field(default_factory=lambda: _pg_url("WAREHOUSE", "warehouse", "dataflow", "5433"))
    source_db_url: str = field(default_factory=lambda: _pg_url("SOURCE_DB", "shop", "shop", "5434"))
    payments_api_url: str = field(
        default_factory=lambda: os.getenv("PAYMENTS_API_URL", "http://localhost:8000")
    )
    data_dir: Path = field(default_factory=_data_dir)

    # Re-read this many minutes before the stored watermark, to catch rows from
    # transactions that committed late. Safe because loads are idempotent.
    lookback_minutes: int = int(os.getenv("INGEST_LOOKBACK_MINUTES", "60"))
    api_page_size: int = int(os.getenv("PAYMENTS_API_PAGE_SIZE", "500"))
    api_timeout_seconds: float = float(os.getenv("PAYMENTS_API_TIMEOUT", "10"))
    retry_attempts: int = int(os.getenv("INGEST_RETRY_ATTEMPTS", "6"))
    retry_max_wait_seconds: float = float(os.getenv("INGEST_RETRY_MAX_WAIT", "30"))


settings = Settings()
