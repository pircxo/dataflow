"""Generate realistic, messy multi-source e-commerce data.

Four sources, like a real company:
  1. PostgreSQL source DB -> customers, orders, order_items          (OLTP system)
  2. CSV file drops       -> data/generated/products/products_<date>.csv
  3. REST API             -> data/generated/payments.json (served by api/mock_payments)
  4. JSONL clickstream    -> data/generated/clickstream/events_<date>.jsonl

The generator simulates the shop day by day. Every simulated day it:
  * adds new customers, orders, order items, payments and web events
  * moves older orders forward (placed -> paid -> shipped -> delivered / returned)
  * updates some existing rows (customer moves city, pending payment succeeds, refunds)
  * writes a product CSV "drop" with new + changed products
  * injects bad records on purpose (see `Mess` below) so the pipeline has work to do

Usage:
  python -m data_generators.generate init --days 60 --seed 42   # wipe + build history
  python -m data_generators.generate append                     # simulate the next day
  python -m data_generators.generate append --date 2026-10-09
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import shutil
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from data_generators import settings

ORDER_STATUSES = ["placed", "paid", "shipped", "delivered", "cancelled", "returned"]
PAYMENT_METHODS = ["card", "card", "card", "paypal", "bank_transfer", "apple_pay"]
FAILURE_REASONS = ["card_declined", "insufficient_funds", "fraud_suspected", "expired_card"]
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "checkout_start"]
DEVICES = ["mobile", "mobile", "desktop", "tablet"]
BROWSERS = ["chrome", "safari", "firefox", "edge"]
BRANDS = ["Atelier North", "Kartli Craft", "Old Tannery", "Saddle & Co", "Urban Hide"]
CATEGORIES = {  # category -> (min price, max price)
    "Bags": (45, 260),
    "Wallets": (15, 95),
    "Belts": (20, 85),
    "Accessories": (5, 60),
    "Shoes": (60, 320),
    "Jackets": (140, 650),
}
# Orders are more likely in the afternoon/evening.
HOUR_WEIGHTS = [1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 8, 8, 8, 9, 10, 11, 12, 11, 8, 5, 2]
CSV_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "brand",
    "unit_cost",
    "list_price",
    "currency",
    "is_active",
    "updated_at",
]


# ---------------------------------------------------------------------------
# Messiness injection
# ---------------------------------------------------------------------------
@dataclass
class Mess:
    """Decides when to corrupt a record and counts what was injected."""

    rng: random.Random
    rate: float
    injected: Counter = field(default_factory=Counter)

    def hit(self, kind: str, scale: float = 1.0) -> bool:
        if self.rng.random() < self.rate * scale:
            self.injected[kind] += 1
            return True
        return False


@dataclass
class Ctx:
    rng: random.Random
    fake: Faker
    engine: Engine
    data_dir: Path
    mess: Mess
    catalog: dict[str, dict] = field(default_factory=dict)  # clean product catalog
    next_customer_id: int = 1
    next_order_id: int = 1
    next_item_id: int = 1
    payments: list[dict] = field(default_factory=list)
    counts: Counter = field(default_factory=Counter)

    @property
    def state_dir(self) -> Path:
        return self.data_dir / "_state"


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def ts_in_day(rng: random.Random, day: date) -> datetime:
    hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
    return datetime.combine(day, time(hour), tzinfo=timezone.utc) + timedelta(seconds=rng.randint(0, 3599))


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def write_json_atomic(path: Path, payload) -> None:
    """Write to a temp file and rename, so the API never reads half a file."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, default=str))
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Source database
# ---------------------------------------------------------------------------
def ensure_source_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.exec_driver_sql(settings.SOURCE_SCHEMA_SQL.read_text())


def insert_rows(engine: Engine, table: str, rows: list[dict]) -> None:
    if not rows:
        return
    cols = list(rows[0])
    sql = text(
        f"INSERT INTO public.{table} ({', '.join(cols)}) " f"VALUES ({', '.join(':' + c for c in cols)})"
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)


def load_counters(ctx: Ctx) -> None:
    with ctx.engine.connect() as conn:
        ctx.next_customer_id = conn.execute(
            text("SELECT coalesce(max(customer_id), 0) + 1 FROM customers")
        ).scalar_one()
        ctx.next_order_id = conn.execute(
            text("SELECT coalesce(max(order_id), 0) + 1 FROM orders")
        ).scalar_one()
        ctx.next_item_id = conn.execute(
            text("SELECT coalesce(max(order_item_id), 0) + 1 FROM order_items")
        ).scalar_one()


# ---------------------------------------------------------------------------
# Products (CSV drops)
# ---------------------------------------------------------------------------
def make_product(ctx: Ctx, num: int, day: date) -> dict:
    rng, fake = ctx.rng, ctx.fake
    category = rng.choice(list(CATEGORIES))
    lo, hi = CATEGORIES[category]
    price = round(rng.uniform(lo, hi), 2)
    noun = category[:-1] if category.endswith("s") else category
    return {
        "product_id": f"P{num:05d}",
        "product_name": f"{fake.color_name()} {fake.word().title()} {noun}",
        "category": category,
        "brand": rng.choice(BRANDS),
        "unit_cost": round(price * rng.uniform(0.35, 0.65), 2),
        "list_price": price,
        "currency": "USD",
        "is_active": True,
        "updated_at": iso(ts_in_day(rng, day)),
    }


def messy_product_rows(ctx: Ctx, clean_rows: list[dict]) -> list[dict]:
    """Turn clean catalog rows into what a supplier's export really looks like."""
    m, out = ctx.mess, []
    for clean in clean_rows:
        row = {k: ("true" if v is True else "false" if v is False else v) for k, v in clean.items()}
        if m.hit("product_missing_id"):
            row["product_id"] = ""
        elif m.hit("product_negative_price"):
            row["list_price"] = -abs(float(row["list_price"]))
        elif m.hit("product_garbage_price"):
            row["list_price"] = m.rng.choice(["N/A", "call us", ""])
        elif m.hit("product_price_with_symbol", scale=2):  # cleanable, NOT an error
            row["list_price"] = f"${float(row['list_price']):,.2f}"
        elif m.hit("product_invalid_timestamp"):
            row["updated_at"] = m.rng.choice(["2026-13-45 25:61:00", "yesterday", "0000-00-00"])
        out.append(row)
        if m.hit("product_exact_duplicate"):
            out.append(dict(row))
        elif row["product_id"] and m.hit("product_conflicting_duplicate"):
            # Same product twice in one file; the later updated_at must win.
            dup = dict(clean)
            dup["list_price"] = round(float(clean["list_price"]) * 1.1, 2)
            dup["updated_at"] = iso(
                datetime.fromisoformat(clean["updated_at"].replace("Z", "+00:00")) + timedelta(seconds=1)
            )
            ctx.catalog[clean["product_id"]]["list_price"] = dup["list_price"]
            out.append({k: ("true" if v is True else v) for k, v in dup.items()})
    return out


def write_products_drop(ctx: Ctx, day: date, clean_rows: list[dict]) -> None:
    if not clean_rows:
        return
    folder = ctx.data_dir / "products"
    folder.mkdir(parents=True, exist_ok=True)
    rows = messy_product_rows(ctx, clean_rows)
    with open(folder / f"products_{day.isoformat()}.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    ctx.counts["product_rows"] += len(rows)


def simulate_product_changes(ctx: Ctx, day: date) -> list[dict]:
    rng, changed = ctx.rng, []
    for p in ctx.catalog.values():
        if rng.random() < 0.02:  # price change
            p["list_price"] = round(p["list_price"] * rng.uniform(0.85, 1.15), 2)
            p["updated_at"] = iso(ts_in_day(rng, day))
            changed.append(dict(p))
        elif p["is_active"] and rng.random() < 0.002:  # discontinued
            p["is_active"] = False
            p["updated_at"] = iso(ts_in_day(rng, day))
            changed.append(dict(p))
    for _ in range(rng.randint(0, 3)):  # new products
        num = len(ctx.catalog) + 1
        p = make_product(ctx, num, day)
        ctx.catalog[p["product_id"]] = p
        changed.append(dict(p))
    return changed


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------
def make_customers(ctx: Ctx, n: int, created_day_range: tuple[date, date]) -> list[dict]:
    rng, fake, m = ctx.rng, ctx.fake, ctx.mess
    start, end = created_day_range
    span = max((end - start).days, 0)
    rows = []
    for _ in range(n):
        first, last = fake.first_name(), fake.last_name()
        created = ts_in_day(rng, start + timedelta(days=rng.randint(0, span)))
        row = {
            "customer_id": ctx.next_customer_id,
            "email": f"{first}.{last}{rng.randint(1, 999)}@{fake.free_email_domain()}".lower(),
            "first_name": first,
            "last_name": last,
            "phone": fake.phone_number(),
            "city": fake.city() if rng.random() > 0.05 else None,
            "country": rng.choice(["US", "US", "US", "GB", "DE", "GE", "FR"]),
            "created_at": created,
            "updated_at": created,
        }
        ctx.next_customer_id += 1
        if m.hit("customer_missing_email"):
            row["email"] = None
        elif m.hit("customer_invalid_email"):
            row["email"] = rng.choice([f"{first}.{last}@", f"{first}{last}.com", "n/a"])
        elif m.hit("customer_future_created_at", scale=0.5):
            row["created_at"] = created + timedelta(days=400)
        rows.append(row)
        if rows and m.hit("customer_duplicate_person", scale=0.5):
            # Same person signs up twice with a different-cased email.
            dup = dict(row)
            dup["customer_id"] = ctx.next_customer_id
            ctx.next_customer_id += 1
            dup["email"] = (row["email"] or "").upper() or None
            dup["created_at"] = dup["updated_at"] = created + timedelta(minutes=rng.randint(5, 600))
            rows.append(dup)
    return rows


def simulate_customer_updates(ctx: Ctx, day: date) -> int:
    rng = ctx.rng
    with ctx.engine.begin() as conn:
        all_ids = conn.execute(text("SELECT customer_id FROM customers")).scalars().all()
        ids = rng.sample(sorted(all_ids), k=min(rng.randint(2, 10), len(all_ids)))
        for cid in ids:
            conn.execute(
                text(
                    "UPDATE customers SET city = :city, phone = :phone, updated_at = :ts "
                    "WHERE customer_id = :id"
                ),
                {
                    "city": ctx.fake.city(),
                    "phone": ctx.fake.phone_number(),
                    "ts": ts_in_day(rng, day),
                    "id": cid,
                },
            )
    return len(ids)


# ---------------------------------------------------------------------------
# Orders, order items, payments
# ---------------------------------------------------------------------------
def make_payment(ctx: Ctx, order_id, amount, status, ts: datetime) -> dict:
    return {
        "payment_id": "pay_" + new_uuid(ctx.rng).replace("-", "")[:20],
        "order_id": order_id,
        "amount": amount,
        "currency": "USD",
        "status": status,
        "method": ctx.rng.choice(PAYMENT_METHODS),
        "failure_reason": ctx.rng.choice(FAILURE_REASONS) if status == "failed" else None,
        "created_at": iso(ts),
        "updated_at": iso(ts),
    }


def messy_payment(ctx: Ctx, p: dict) -> list[dict]:
    m = ctx.mess
    if m.hit("payment_missing_order_id"):
        p["order_id"] = None
    elif m.hit("payment_negative_amount"):
        p["amount"] = -abs(p["amount"])
    elif m.hit("payment_invalid_created_at"):
        p["created_at"] = m.rng.choice(["31/02/2026", "not-a-date", ""])
    elif m.hit("payment_unknown_status"):
        p["status"] = "UNKNOWN"
    elif m.hit("payment_amount_as_string", scale=2):  # cleanable, NOT an error
        p["amount"] = f"{p['amount']:.2f}"
    elif m.hit("payment_lowercase_currency", scale=2):  # cleanable, NOT an error
        p["currency"] = "usd"
    elif m.hit("payment_orphan_order", scale=0.5):  # caught later by dbt
        p["order_id"] = 9_000_000 + m.rng.randint(1, 99_999)
    out = [p]
    if m.hit("payment_exact_duplicate"):
        out.append(dict(p))
    return out


def make_orders_for_day(ctx: Ctx, day: date, customer_ids: list[int]) -> None:
    rng, m = ctx.rng, ctx.mess
    active_products = [p for p in ctx.catalog.values() if p["is_active"]]
    weekday_factor = 1.25 if day.weekday() >= 5 else 1.0
    n_orders = int(rng.gauss(110, 15) * weekday_factor)

    orders, items = [], []
    for _ in range(max(n_orders, 10)):
        order_id = ctx.next_order_id
        ctx.next_order_id += 1
        ts = ts_in_day(rng, day)
        order_items = []
        for _ in range(rng.choices([1, 2, 3, 4], weights=[55, 25, 13, 7])[0]):
            product = rng.choice(active_products)
            discount = rng.choice([1, 1, 1, 0.9, 0.8])
            item = {
                "order_item_id": ctx.next_item_id,
                "order_id": order_id,
                "product_id": product["product_id"],
                "quantity": rng.choices([1, 2, 3], weights=[80, 15, 5])[0],
                "unit_price": round(product["list_price"] * discount, 2),
                "updated_at": ts,
            }
            ctx.next_item_id += 1
            if m.hit("item_bad_quantity"):
                item["quantity"] = rng.choice([0, -1, -3])
            elif m.hit("item_negative_price"):
                item["unit_price"] = -abs(item["unit_price"])
            elif m.hit("item_unknown_product"):
                item["product_id"] = "P99999"
            elif m.hit("item_missing_product"):
                item["product_id"] = None
            order_items.append(item)

        total = round(sum(max(i["quantity"], 0) * abs(i["unit_price"]) for i in order_items), 2)
        order = {
            "order_id": order_id,
            "customer_id": rng.choice(customer_ids),
            "order_status": "placed",
            "order_ts": ts,
            "shipping_city": ctx.fake.city(),
            "total_amount": total,
            "currency": "USD",
            "updated_at": ts,
        }
        if m.hit("order_orphan_customer"):  # caught by dbt relationship test
            order["customer_id"] = 900_000 + rng.randint(1, 99_999)
        elif m.hit("order_negative_total"):
            order["total_amount"] = -total
        elif m.hit("order_null_timestamp"):
            order["order_ts"] = None
        elif m.hit("order_impossible_timestamp"):
            order["order_ts"] = rng.choice(
                [datetime(1970, 1, 1, tzinfo=timezone.utc), ts + timedelta(days=3 * 365)]
            )
        elif m.hit("order_unknown_status"):
            order["order_status"] = rng.choice(["unknwn", "???", "TEST"])
        elif m.hit("order_messy_status_case", scale=2):  # cleanable, NOT an error
            order["order_status"] = rng.choice([" Placed", "PLACED", "placed "])
        elif m.hit("order_total_mismatch"):  # caught by custom dbt test
            order["total_amount"] = round(total + rng.uniform(5, 50), 2)

        orders.append(order)
        items.extend(order_items)

        # Payment attempt(s) for this order.
        pay_ts = ts + timedelta(minutes=rng.randint(0, 20))
        r = rng.random()
        if r < 0.08:  # failed attempt, sometimes retried successfully
            ctx.payments.extend(messy_payment(ctx, make_payment(ctx, order_id, total, "failed", pay_ts)))
            if rng.random() < 0.7:
                retry = make_payment(ctx, order_id, total, "succeeded", pay_ts + timedelta(minutes=7))
                ctx.payments.extend(messy_payment(ctx, retry))
        elif r < 0.18:  # pending, resolved on a later day
            ctx.payments.extend(messy_payment(ctx, make_payment(ctx, order_id, total, "pending", pay_ts)))
        elif r < 0.97:
            ctx.payments.extend(messy_payment(ctx, make_payment(ctx, order_id, total, "succeeded", pay_ts)))
        # else: no payment at all (abandoned)

        # Double-submit: same order inserted twice with a new id (caught by dbt test).
        if m.hit("order_double_submit", scale=0.5):
            dup = dict(order)
            dup["order_id"] = ctx.next_order_id
            ctx.next_order_id += 1
            orders.append(dup)
            for i in order_items:
                items.append({**i, "order_item_id": ctx.next_item_id, "order_id": dup["order_id"]})
                ctx.next_item_id += 1

    insert_rows(ctx.engine, "orders", orders)
    insert_rows(ctx.engine, "order_items", items)
    ctx.counts["orders"] += len(orders)
    ctx.counts["order_items"] += len(items)


NEXT_STATUS = {
    "placed": ["paid", "cancelled"],
    "paid": ["shipped"],
    "shipped": ["delivered"],
    "delivered": ["returned"],
}
STATUS_PROB = {"placed": 0.85, "paid": 0.7, "shipped": 0.6, "delivered": 0.02}


def progress_orders(ctx: Ctx, day: date) -> None:
    """Move open orders forward one step. These UPDATEs exercise incremental loading."""
    rng = ctx.rng
    with ctx.engine.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT order_id, lower(trim(order_status)) FROM orders "
                "WHERE lower(trim(order_status)) IN ('placed','paid','shipped','delivered') "
                "AND updated_at < :day_start AND updated_at > :cutoff"
            ),
            {
                "day_start": datetime.combine(day, time(), tzinfo=timezone.utc),
                "cutoff": datetime.combine(day - timedelta(days=30), time(), tzinfo=timezone.utc),
            },
        ).all()
        updates = []
        for order_id, status in rows:
            if rng.random() < STATUS_PROB[status]:
                new = rng.choices(NEXT_STATUS[status], weights=[92, 8][: len(NEXT_STATUS[status])])[0]
                updates.append({"id": order_id, "s": new, "ts": ts_in_day(rng, day)})
        if updates:
            conn.execute(
                text("UPDATE orders SET order_status = :s, updated_at = :ts WHERE order_id = :id"),
                updates,
            )
    ctx.counts["order_updates"] += len(updates)

    # Mirror refunds and pending resolutions into the payments feed.
    returned = {u["id"] for u in updates if u["s"] == "returned"}
    for p in ctx.payments:
        p_ts = p["updated_at"]
        if p["status"] == "pending" and p_ts < iso(datetime.combine(day, time(), tzinfo=timezone.utc)):
            p["status"] = "succeeded" if rng.random() < 0.9 else "failed"
            p["failure_reason"] = rng.choice(FAILURE_REASONS) if p["status"] == "failed" else None
            p["updated_at"] = iso(ts_in_day(rng, day))
            ctx.counts["payment_updates"] += 1
        elif p["status"] == "succeeded" and p["order_id"] in returned:
            p["status"] = "refunded"
            p["updated_at"] = iso(ts_in_day(rng, day))
            ctx.counts["payment_updates"] += 1


# ---------------------------------------------------------------------------
# Clickstream (JSON lines)
# ---------------------------------------------------------------------------
def write_clickstream(ctx: Ctx, day: date, customer_ids: list[int]) -> None:
    rng, m = ctx.rng, ctx.mess
    product_ids = list(ctx.catalog)
    lines: list[str] = []
    for _ in range(max(int(rng.gauss(450, 60)), 50)):
        session_id = new_uuid(rng)
        customer_id = rng.choice(customer_ids) if rng.random() < 0.6 else None
        ts = ts_in_day(rng, day)
        context = {
            "device": rng.choice(DEVICES),
            "browser": rng.choice(BROWSERS),
            "country": rng.choice(["US", "GB", "DE", "GE", "FR"]),
        }
        for _ in range(rng.randint(1, 8)):
            event_type = rng.choice(EVENT_TYPES)
            product_id = rng.choice(product_ids) if event_type != "page_view" else None
            event = {
                "event_id": new_uuid(rng),
                "session_id": session_id,
                "customer_id": customer_id,
                "event_type": event_type,
                "product_id": product_id,
                "page_url": f"/products/{product_id}" if product_id else rng.choice(["/", "/sale", "/about"]),
                "event_ts": iso(ts),
                "context": context,
            }
            ts += timedelta(seconds=rng.randint(5, 240))
            if m.hit("event_missing_id"):
                event["event_id"] = None
            elif m.hit("event_invalid_timestamp"):
                event["event_ts"] = rng.choice(["yesterday", "2026-02-30T10:00:00Z", 1234])
            elif m.hit("event_unknown_type"):
                event["event_type"] = "UNKNOWN_EVT"
            line = json.dumps(event)
            if m.hit("event_malformed_json", scale=0.5):
                line = line[: len(line) // 2]  # truncated line
            lines.append(line)
            if m.hit("event_duplicate"):
                lines.append(line)
    folder = ctx.data_dir / "clickstream"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"events_{day.isoformat()}.jsonl").write_text("\n".join(lines) + "\n")
    ctx.counts["events"] += len(lines)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def simulate_day(ctx: Ctx, day: date) -> None:
    progress_orders(ctx, day)
    ctx.counts["customer_updates"] += simulate_customer_updates(ctx, day)

    new_customers = make_customers(ctx, ctx.rng.randint(8, 25), (day, day))
    insert_rows(ctx.engine, "customers", new_customers)
    ctx.counts["customers"] += len(new_customers)

    with ctx.engine.connect() as conn:
        customer_ids = conn.execute(text("SELECT customer_id FROM customers")).scalars().all()

    write_products_drop(ctx, day, simulate_product_changes(ctx, day))
    make_orders_for_day(ctx, day, customer_ids)
    write_clickstream(ctx, day, customer_ids)


def load_state(ctx: Ctx) -> date:
    state = json.loads((ctx.state_dir / "state.json").read_text())
    ctx.catalog = json.loads((ctx.state_dir / "catalog.json").read_text())
    payments_file = ctx.data_dir / "payments.json"
    ctx.payments = json.loads(payments_file.read_text()) if payments_file.exists() else []
    ctx.rng.seed(state["seed"] + state["days_generated"])
    Faker.seed(state["seed"] + state["days_generated"])
    return date.fromisoformat(state["last_day"])


def save_state(ctx: Ctx, last_day: date, seed: int, days_generated: int) -> None:
    ctx.state_dir.mkdir(parents=True, exist_ok=True)
    write_json_atomic(ctx.data_dir / "payments.json", ctx.payments)
    write_json_atomic(ctx.state_dir / "catalog.json", ctx.catalog)
    write_json_atomic(
        ctx.state_dir / "state.json",
        {"last_day": last_day.isoformat(), "seed": seed, "days_generated": days_generated},
    )


def cmd_init(ctx: Ctx, days: int, end: date, seed: int) -> None:
    if end > date.today():
        raise SystemExit("--end cannot be in the future")
    start = end - timedelta(days=days - 1)
    print(f"Resetting sources and generating {days} days: {start} -> {end}")

    with ctx.engine.begin() as conn:
        conn.exec_driver_sql("DROP TABLE IF EXISTS order_items, orders, customers")
    ensure_source_schema(ctx.engine)
    for sub in ("products", "clickstream", "_state"):
        shutil.rmtree(ctx.data_dir / sub, ignore_errors=True)
    (ctx.data_dir / "payments.json").unlink(missing_ok=True)
    ctx.data_dir.mkdir(parents=True, exist_ok=True)

    # Initial catalog + existing customer base (signed up before the window).
    first_drop = []
    for num in range(1, 201):
        p = make_product(ctx, num, start - timedelta(days=1))
        ctx.catalog[p["product_id"]] = p
        first_drop.append(dict(p))
    write_products_drop(ctx, start - timedelta(days=1), first_drop)
    base = make_customers(ctx, 1500, (start - timedelta(days=730), start - timedelta(days=1)))
    insert_rows(ctx.engine, "customers", base)
    ctx.counts["customers"] += len(base)

    for i in range(days):
        simulate_day(ctx, start + timedelta(days=i))
    save_state(ctx, end, seed, days)


def cmd_append(ctx: Ctx, day: date | None, force: bool, init_if_missing: bool = False) -> None:
    """Simulate every missing day up to `day` (default: the next day).

    Re-running for a day that already exists is a no-op (exit 0), so a scheduler can
    safely retry or re-run it.
    """
    if not (ctx.state_dir / "state.json").exists():
        if not init_if_missing:
            raise SystemExit("No generator state found. Run `init` first.")
        end = min(day or date.today() - timedelta(days=1), date.today())
        cmd_init(ctx, 60, end, seed=42)
        return

    last_day = load_state(ctx)
    state = json.loads((ctx.state_dir / "state.json").read_text())
    day = day or last_day + timedelta(days=1)
    if day > date.today():
        # Future-dated rows would (correctly) be quarantined by the ingestion
        # timestamp guard, so never simulate days that haven't happened yet.
        print(f"{day} is in the future; nothing to simulate yet.")
        return
    if day <= last_day and not force:
        print(f"{day} already generated (last day {last_day}); nothing to do.")
        return

    first = day if force else last_day + timedelta(days=1)
    load_counters(ctx)
    current = first
    while current <= day:
        print(f"Simulating {current}")
        simulate_day(ctx, current)
        current += timedelta(days=1)
    generated = state["days_generated"] + (day - first).days + 1
    save_state(ctx, max(day, last_day), state["seed"], generated)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p_init = sub.add_parser("init", help="wipe sources and generate history")
    p_init.add_argument("--days", type=int, default=60)
    p_init.add_argument("--end", type=date.fromisoformat, default=date.today() - timedelta(days=1))
    p_init.add_argument("--seed", type=int, default=42)
    p_app = sub.add_parser("append", help="simulate one more day")
    p_app.add_argument("--date", type=date.fromisoformat, default=None)
    p_app.add_argument("--force", action="store_true", help="re-simulate a day that already exists")
    p_app.add_argument("--init-if-missing", action="store_true", help="run `init` (60 days) if no state")
    for p in (p_init, p_app):
        p.add_argument(
            "--mess-rate",
            type=float,
            default=0.03,
            help="probability of each kind of corruption per record (default 0.03)",
        )
    args = parser.parse_args()

    seed = getattr(args, "seed", 42)
    rng = random.Random(seed)
    fake = Faker()
    Faker.seed(seed)
    ctx = Ctx(
        rng=rng,
        fake=fake,
        engine=create_engine(settings.source_db_url(), future=True),
        data_dir=settings.data_dir(),
        mess=Mess(rng=rng, rate=args.mess_rate),
    )
    if args.command == "init":
        cmd_init(ctx, args.days, args.end, seed)
    else:
        cmd_append(ctx, args.date, args.force, args.init_if_missing)

    print("\nGenerated:")
    for k, v in sorted(ctx.counts.items()):
        print(f"  {k:<20} {v:>8}")
    print("\nInjected problems (on purpose):")
    for k, v in sorted(ctx.mess.injected.items()):
        print(f"  {k:<32} {v:>6}")


if __name__ == "__main__":
    main()
