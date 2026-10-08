"""What each entity looks like in `raw`, and which data-quality rules guard it.

Split of responsibilities (a common interview question):
  * Ingestion (here)  -> record-level checks: types, required keys, ranges, allowed values.
                         A row that fails is quarantined, never silently dropped.
  * dbt (Milestone 4) -> cross-table checks: orphans, duplicate business events,
                         totals that don't add up. Those need the whole dataset.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import pandas as pd

from ingestion.validators.core import (
    EMAIL_RE,
    Rule,
    blank_to_na,
    in_set,
    matches,
    non_negative,
    not_null,
    parse_bool,
    parse_failed,
    parse_int,
    parse_money,
    parse_timestamp,
    positive,
    timestamp_in_range,
)

ORDER_STATUSES = {"placed", "paid", "shipped", "delivered", "cancelled", "returned"}
PAYMENT_STATUSES = {"succeeded", "failed", "pending", "refunded"}
CURRENCIES = {"usd", "eur", "gbp", "gel"}
EVENT_TYPES = {"page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout_start", "purchase"}

Parser = Callable[[pd.Series], pd.Series]


def as_text(series: pd.Series) -> pd.Series:
    return blank_to_na(series).astype("string")


def as_json(series: pd.Series) -> pd.Series:
    return series.where(series.notna(), None)


def make_prepare(conversions: dict[str, Parser]) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build a function that turns source columns into typed raw columns.

    Original values are kept as `src__<col>` so rules can tell
    "missing in source" apart from "present but unparseable".
    A column missing from the source entirely (schema drift) becomes all-NA,
    so the matching not-null rule rejects it loudly.
    """

    def prepare(df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame(index=df.index)
        for col, parser in conversions.items():
            src = df[col] if col in df.columns else pd.Series(pd.NA, index=df.index, dtype=object)
            out[f"src__{col}"] = blank_to_na(src)
            out[col] = parser(src)
        return out

    return prepare


@dataclass(frozen=True)
class EntitySpec:
    name: str
    primary_key: str
    conversions: dict[str, Parser]
    rules: list[Rule]
    # Column that orders versions of the same key. On conflict, an OLDER version
    # never overwrites a NEWER one (protects against out-of-order delivery).
    version_column: str | None = "updated_at"
    columns: list[str] = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "columns", list(self.conversions))

    @property
    def raw_table(self) -> str:
        return f"raw.{self.name}"

    @property
    def all_rules(self) -> list[Rule]:
        """Explicit rules + an automatic "invalid_<col>" rule for every parsed column:
        the source had a value, but it could not be turned into the right type."""
        auto = [
            parse_failed(col, f"src__{col}")
            for col, parser in self.conversions.items()
            if parser not in (as_text, as_json)
        ]
        return auto + self.rules

    def prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        return make_prepare(self.conversions)(df)


CUSTOMERS = EntitySpec(
    name="customers",
    primary_key="customer_id",
    conversions={
        "customer_id": parse_int,
        "email": as_text,
        "first_name": as_text,
        "last_name": as_text,
        "phone": as_text,
        "city": as_text,
        "country": as_text,
        "created_at": parse_timestamp,
        "updated_at": parse_timestamp,
    },
    rules=[
        not_null("customer_id"),
        not_null("email"),
        matches("email", EMAIL_RE, "invalid_email"),
        timestamp_in_range("created_at", "created_at_in_future"),
        not_null("updated_at"),
    ],
)

ORDERS = EntitySpec(
    name="orders",
    primary_key="order_id",
    conversions={
        "order_id": parse_int,
        "customer_id": parse_int,
        "order_status": as_text,
        "order_ts": parse_timestamp,
        "shipping_city": as_text,
        "total_amount": parse_money,
        "currency": as_text,
        "updated_at": parse_timestamp,
    },
    rules=[
        not_null("order_id"),
        not_null("customer_id"),
        not_null("order_ts"),
        timestamp_in_range("order_ts"),
        not_null("total_amount"),
        non_negative("total_amount", "negative_total_amount"),
        in_set("order_status", ORDER_STATUSES, "unknown_order_status"),
        not_null("updated_at"),
    ],
)

ORDER_ITEMS = EntitySpec(
    name="order_items",
    primary_key="order_item_id",
    conversions={
        "order_item_id": parse_int,
        "order_id": parse_int,
        "product_id": as_text,
        "quantity": parse_int,
        "unit_price": parse_money,
        "updated_at": parse_timestamp,
    },
    rules=[
        not_null("order_item_id"),
        not_null("order_id"),
        not_null("product_id"),
        not_null("quantity"),
        positive("quantity"),
        not_null("unit_price"),
        non_negative("unit_price"),
        not_null("updated_at"),
    ],
)

PRODUCTS = EntitySpec(
    name="products",
    primary_key="product_id",
    conversions={
        "product_id": as_text,
        "product_name": as_text,
        "category": as_text,
        "brand": as_text,
        "unit_cost": parse_money,
        "list_price": parse_money,
        "currency": as_text,
        "is_active": parse_bool,
        "updated_at": parse_timestamp,
    },
    rules=[
        not_null("product_id"),
        not_null("list_price"),
        non_negative("list_price"),
        non_negative("unit_cost"),
        not_null("updated_at"),
    ],
)

PAYMENTS = EntitySpec(
    name="payments",
    primary_key="payment_id",
    conversions={
        "payment_id": as_text,
        "order_id": parse_int,
        "amount": parse_money,
        "currency": as_text,
        "status": as_text,
        "method": as_text,
        "failure_reason": as_text,
        "created_at": parse_timestamp,
        "updated_at": parse_timestamp,
    },
    rules=[
        not_null("payment_id"),
        not_null("order_id"),
        not_null("amount"),
        non_negative("amount"),
        in_set("status", PAYMENT_STATUSES, "unknown_payment_status"),
        in_set("currency", CURRENCIES),
        not_null("created_at"),
        not_null("updated_at"),
    ],
)

CLICKSTREAM_EVENTS = EntitySpec(
    name="clickstream_events",
    primary_key="event_id",
    conversions={
        "event_id": as_text,
        "session_id": as_text,
        "customer_id": parse_int,
        "event_type": as_text,
        "product_id": as_text,
        "page_url": as_text,
        "event_ts": parse_timestamp,
        "context": as_json,
    },
    rules=[
        not_null("event_id"),
        not_null("event_ts"),
        timestamp_in_range("event_ts"),
        in_set("event_type", EVENT_TYPES, "unknown_event_type"),
    ],
    version_column=None,  # events are immutable
)

ALL = {spec.name: spec for spec in (CUSTOMERS, ORDERS, ORDER_ITEMS, PRODUCTS, PAYMENTS, CLICKSTREAM_EVENTS)}
