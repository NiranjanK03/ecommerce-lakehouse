-- Olist e-commerce schema
-- Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
--
-- Schema mirrors the original Olist CSV structure with:
--   - proper types (UUID PKs, TIMESTAMP, NUMERIC for money)
--   - created_at / updated_at for CDC tracking
--   - REPLICA IDENTITY FULL on all tables so Debezium captures before/after
--     images for UPDATE and DELETE events (not just PKs)

-- Debezium requires the CDC user to have the REPLICATION attribute.
ALTER USER olist_user REPLICATION;

SET search_path = public;

-- ─── customers ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers (
    customer_id          UUID PRIMARY KEY,
    customer_unique_id   UUID        NOT NULL,
    customer_zip_code    VARCHAR(10),
    customer_city        VARCHAR(100),
    customer_state       CHAR(2),
    created_at           TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP   NOT NULL DEFAULT NOW()
);

ALTER TABLE customers REPLICA IDENTITY FULL;

-- ─── sellers ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sellers (
    seller_id            UUID PRIMARY KEY,
    seller_zip_code      VARCHAR(10),
    seller_city          VARCHAR(100),
    seller_state         CHAR(2),
    created_at           TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP   NOT NULL DEFAULT NOW()
);

ALTER TABLE sellers REPLICA IDENTITY FULL;

-- ─── products ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    product_id           UUID PRIMARY KEY,
    product_category     VARCHAR(100),
    product_name_length  INTEGER,
    product_desc_length  INTEGER,
    product_photos_qty   INTEGER,
    product_weight_g     INTEGER,
    product_length_cm    INTEGER,
    product_height_cm    INTEGER,
    product_width_cm     INTEGER,
    created_at           TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP   NOT NULL DEFAULT NOW()
);

ALTER TABLE products REPLICA IDENTITY FULL;

-- ─── orders ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    order_id                 UUID PRIMARY KEY,
    customer_id              UUID        REFERENCES customers(customer_id),
    order_status             VARCHAR(20) NOT NULL,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at        TIMESTAMP,
    order_delivered_carrier  TIMESTAMP,
    order_delivered_customer TIMESTAMP,
    order_estimated_delivery DATE,
    created_at               TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMP   NOT NULL DEFAULT NOW()
);

ALTER TABLE orders REPLICA IDENTITY FULL;

-- ─── order_items ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_items (
    order_id            UUID        NOT NULL REFERENCES orders(order_id),
    order_item_id       INTEGER     NOT NULL,
    product_id          UUID        REFERENCES products(product_id),
    seller_id           UUID        REFERENCES sellers(seller_id),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10, 2),
    freight_value       NUMERIC(10, 2),
    created_at          TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, order_item_id)
);

ALTER TABLE order_items REPLICA IDENTITY FULL;

-- ─── order_payments ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_payments (
    order_id             UUID        NOT NULL REFERENCES orders(order_id),
    payment_sequential   INTEGER     NOT NULL,
    payment_type         VARCHAR(20),
    payment_installments INTEGER,
    payment_value        NUMERIC(10, 2),
    created_at           TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_id, payment_sequential)
);

ALTER TABLE order_payments REPLICA IDENTITY FULL;

-- ─── order_reviews ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_reviews (
    review_id            UUID        NOT NULL,
    order_id             UUID        NOT NULL REFERENCES orders(order_id),
    review_score         SMALLINT    CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title TEXT,
    review_comment_msg   TEXT,
    review_creation_date TIMESTAMP,
    review_answer_date   TIMESTAMP,
    created_at           TIMESTAMP   NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (review_id, order_id)
);

ALTER TABLE order_reviews REPLICA IDENTITY FULL;

-- ─── geolocation ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS geolocation (
    geolocation_zip_code VARCHAR(10),
    geolocation_lat      DOUBLE PRECISION,
    geolocation_lng      DOUBLE PRECISION,
    geolocation_city     VARCHAR(100),
    geolocation_state    CHAR(2),
    created_at           TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- ─── category_translations ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS category_translations (
    product_category_name         VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100),
    created_at                    TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ─── Debezium heartbeat ────────────────────────────────────────────────────────
-- Debezium updates this table every 5s (heartbeat.action.query in connector config).
-- This advances the WAL LSN position even when source tables are quiet,
-- preventing WAL segments from accumulating indefinitely.
CREATE TABLE IF NOT EXISTS debezium_heartbeat (
    id                  INTEGER PRIMARY KEY DEFAULT 1,
    last_heartbeat_ts   TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO debezium_heartbeat (id, last_heartbeat_ts)
VALUES (1, NOW())
ON CONFLICT DO NOTHING;
