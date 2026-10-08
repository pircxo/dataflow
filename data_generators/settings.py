"""Settings for the data generators, read from environment / .env."""

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def source_db_url() -> str:
    return (
        "postgresql+psycopg2://"
        f"{os.getenv('SOURCE_DB_USER', 'shop')}:{os.getenv('SOURCE_DB_PASSWORD', 'shop')}"
        f"@{os.getenv('SOURCE_DB_HOST', 'localhost')}:{os.getenv('SOURCE_DB_PORT', '5434')}"
        f"/{os.getenv('SOURCE_DB_NAME', 'shop')}"
    )


def data_dir() -> Path:
    path = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data" / "generated"))
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


SOURCE_SCHEMA_SQL = PROJECT_ROOT / "docker" / "postgres" / "source" / "01_schema.sql"
