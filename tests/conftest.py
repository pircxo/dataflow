import os

import pytest
from sqlalchemy import create_engine, text


@pytest.fixture(scope="session")
def warehouse():
    """Real Postgres for integration tests. Skipped when none is reachable."""
    from ingestion.config import settings
    from ingestion.db import bootstrap_warehouse

    url = os.getenv("TEST_WAREHOUSE_URL", settings.warehouse_url)
    engine = create_engine(url, future=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:  # noqa: BLE001
        pytest.skip("No Postgres available for integration tests")
    bootstrap_warehouse(engine)
    return engine
