import json
import uuid

import pandas as pd
import requests
from sqlalchemy import text

from ingestion.entities import PRODUCTS
from ingestion.extractors.files import read_jsonl
from ingestion.loaders.warehouse import dedupe_latest, record_hashes, upsert_raw
from ingestion.retry import is_transient


def _product(pid, price, ts):
    return {
        "product_id": pid,
        "product_name": "Bag",
        "category": "Bags",
        "brand": "X",
        "unit_cost": 1.0,
        "list_price": price,
        "currency": "USD",
        "is_active": True,
        "updated_at": pd.Timestamp(ts, tz="UTC"),
    }


def test_dedupe_keeps_latest_version():
    df = pd.DataFrame(
        [_product("P1", 10, "2026-09-01"), _product("P1", 12, "2026-09-02"), _product("P2", 5, "2026-09-01")]
    )
    out, removed = dedupe_latest(df, PRODUCTS)
    assert removed == 1
    assert out.set_index("product_id").loc["P1", "list_price"] == 12


def test_record_hash_is_stable_and_sensitive():
    a = pd.DataFrame([_product("P1", 10, "2026-09-01")])
    b = pd.DataFrame([_product("P1", 11, "2026-09-01")])
    assert record_hashes(a, PRODUCTS.columns).iloc[0] == record_hashes(a.copy(), PRODUCTS.columns).iloc[0]
    assert record_hashes(a, PRODUCTS.columns).iloc[0] != record_hashes(b, PRODUCTS.columns).iloc[0]


def test_read_jsonl_separates_malformed_lines(tmp_path):
    f = tmp_path / "events_2026-09-01.jsonl"
    f.write_text(json.dumps({"event_id": "a"}) + "\n" + '{"event_id": "b", "ev' + "\n" + "[1,2]\n")
    df, bad = read_jsonl(f)
    assert list(df["event_id"]) == ["a"]
    assert [b["line_number"] for b in bad] == [2, 3]


def test_transient_error_classification():
    resp = requests.Response()
    resp.status_code = 503
    assert is_transient(requests.HTTPError(response=resp))
    resp.status_code = 400
    assert not is_transient(requests.HTTPError(response=resp))
    assert is_transient(requests.ConnectionError())
    assert not is_transient(ValueError())


def test_upsert_is_idempotent_and_ignores_older_versions(warehouse):
    pid = f"TEST-{uuid.uuid4().hex[:8]}"
    newer = pd.DataFrame([_product(pid, 20, "2026-09-02")])
    older = pd.DataFrame([_product(pid, 10, "2026-09-01")])
    try:
        with warehouse.begin() as conn:
            first = upsert_raw(conn, PRODUCTS, newer, "test", "t1")
        with warehouse.begin() as conn:
            again = upsert_raw(conn, PRODUCTS, newer, "test", "t2")
        with warehouse.begin() as conn:
            stale = upsert_raw(conn, PRODUCTS, older, "test", "t3")
        assert (first.inserted, first.updated) == (1, 0)
        assert (again.inserted, again.updated, again.unchanged) == (0, 0, 1)
        assert (stale.inserted, stale.updated, stale.unchanged) == (0, 0, 1)
        with warehouse.connect() as conn:
            price = conn.execute(
                text("SELECT list_price FROM raw.products WHERE product_id = :p"), {"p": pid}
            ).scalar_one()
        assert float(price) == 20.0
    finally:
        with warehouse.begin() as conn:
            conn.execute(text("DELETE FROM raw.products WHERE product_id = :p"), {"p": pid})
