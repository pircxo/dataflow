import pandas as pd

from ingestion.entities import CLICKSTREAM_EVENTS, ORDERS, PAYMENTS, PRODUCTS
from ingestion.validators import validate
from ingestion.validators.core import parse_money, parse_timestamp


def run(spec, rows):
    df = pd.DataFrame(rows)
    typed = spec.prepare(df)
    return typed, validate(typed, spec.all_rules)


def test_parse_money_handles_symbols_and_garbage():
    out = parse_money(pd.Series(["$1,234.50", "12.5", 7, "N/A", None]))
    assert out.iloc[0] == 1234.5
    assert out.iloc[1] == 12.5
    assert out.iloc[2] == 7
    assert pd.isna(out.iloc[3]) and pd.isna(out.iloc[4])


def test_parse_timestamp_rejects_garbage_and_bare_numbers():
    out = parse_timestamp(
        pd.Series(["2026-09-01T10:00:00Z", "yesterday", "2026-02-30T10:00:00Z", 1234, None])
    )
    assert out.iloc[0] == pd.Timestamp("2026-09-01T10:00:00Z")
    assert out.iloc[1:].isna().all()


def test_products_good_row_passes_and_cleanable_price_is_parsed():
    typed, result = run(
        PRODUCTS,
        [
            {
                "product_id": "P00001",
                "product_name": "Bag",
                "category": "Bags",
                "brand": "X",
                "unit_cost": "10.00",
                "list_price": "$1,050.00",
                "currency": "USD",
                "is_active": "true",
                "updated_at": "2026-09-01T10:00:00Z",
            }
        ],
    )
    assert len(result.valid) == 1
    assert result.valid["list_price"].iloc[0] == 1050.0


def test_products_collects_every_reason():
    _, result = run(
        PRODUCTS,
        [
            {
                "product_id": None,
                "product_name": "Bag",
                "category": "Bags",
                "brand": "X",
                "unit_cost": "10",
                "list_price": "N/A",
                "currency": "USD",
                "is_active": "true",
                "updated_at": "2026-13-45 25:61:00",
            }
        ],
    )
    assert result.valid.empty
    reasons = set(result.reasons.iloc[0])
    assert {"missing_product_id", "invalid_list_price", "invalid_updated_at"} <= reasons
    # "N/A" is present-but-invalid, not missing
    assert "missing_list_price" not in reasons


def test_orders_rules():
    base = {
        "order_id": 1,
        "customer_id": 5,
        "order_status": " Placed",
        "order_ts": "2026-09-01T10:00:00Z",
        "shipping_city": "X",
        "total_amount": 10,
        "currency": "USD",
        "updated_at": "2026-09-01T10:00:00Z",
    }
    rows = [
        base,  # ok (messy case is fine)
        {**base, "order_id": 2, "total_amount": -5},  # negative
        {**base, "order_id": 3, "order_ts": "1970-01-01T00:00:00Z"},  # out of range
        {**base, "order_id": 4, "order_status": "???"},  # unknown status
        {**base, "order_id": 5, "order_ts": None},  # missing
    ]
    _, result = run(ORDERS, rows)
    assert list(result.valid["order_id"]) == [1]
    by_id = dict(zip([2, 3, 4, 5], result.reasons))
    assert by_id[2] == ["negative_total_amount"]
    assert by_id[3] == ["order_ts_out_of_range"]
    assert by_id[4] == ["unknown_order_status"]
    assert by_id[5] == ["missing_order_ts"]


def test_payments_amount_as_string_is_ok_but_negative_is_not():
    base = {
        "payment_id": "p1",
        "order_id": 1,
        "amount": "12.50",
        "currency": "usd",
        "status": "succeeded",
        "method": "card",
        "failure_reason": None,
        "created_at": "2026-09-01T10:00:00Z",
        "updated_at": "2026-09-01T10:00:00Z",
    }
    _, result = run(PAYMENTS, [base, {**base, "payment_id": "p2", "amount": -1}])
    assert list(result.valid["payment_id"]) == ["p1"]
    assert result.reasons.iloc[0] == ["negative_amount"]


def test_schema_drift_missing_column_is_rejected():
    _, result = run(CLICKSTREAM_EVENTS, [{"event_id": "e1", "event_type": "page_view"}])  # no event_ts
    assert result.valid.empty
    assert "missing_event_ts" in result.reasons.iloc[0]
