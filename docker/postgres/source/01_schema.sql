-- Source OLTP system ("the shop's production database").
-- Runs automatically on the first start of the source-db container.
-- The data generator also runs it (idempotent), so it works without Docker too.
--
-- NOTE: there are deliberately NO foreign keys. This mimics a legacy system where
-- orphan rows happen. Catching them is the pipeline's job (dbt relationship tests).

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id   integer PRIMARY KEY,
    email         text,
    first_name    text,
    last_name     text,
    phone         text,
    city          text,
    country       text,
    created_at    timestamptz,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id      integer PRIMARY KEY,
    customer_id   integer,
    order_status  text,
    order_ts      timestamptz,
    shipping_city text,
    total_amount  numeric(12, 2),
    currency      text,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.order_items (
    order_item_id integer PRIMARY KEY,
    order_id      integer,
    product_id    text,
    quantity      integer,
    unit_price    numeric(12, 2),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Incremental extraction filters on updated_at, so index it.
CREATE INDEX IF NOT EXISTS ix_customers_updated_at   ON public.customers (updated_at);
CREATE INDEX IF NOT EXISTS ix_orders_updated_at      ON public.orders (updated_at);
CREATE INDEX IF NOT EXISTS ix_order_items_updated_at ON public.order_items (updated_at);
