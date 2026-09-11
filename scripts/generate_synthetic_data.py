#!/usr/bin/env python3
"""
Generates synthetic e-commerce data matching the Olist schema and bulk-loads
it into PostgreSQL via COPY FROM STDIN.

Debezium reads the WAL after load, so every row flows through the full
Bronze → Silver → Gold pipeline exactly like real CDC events.

Prerequisites:
    pip install psycopg2-binary
    make port-forward   # PostgreSQL must be reachable on localhost:5432

Usage:
    python scripts/generate_synthetic_data.py --orders 900000
    python scripts/generate_synthetic_data.py --orders 900000 --truncate
    python scripts/generate_synthetic_data.py --orders 100000 --host localhost --port 5432

Target row counts at --orders 900000 (~5M total rows):
    customers:       900,000
    sellers:          28,125
    products:        300,000
    orders:          900,000
    order_items:  ~1,017,000  (avg 1.13 items/order, matching Olist ratio)
    order_payments: ~945,000  (avg 1.05 payments/order)
    order_reviews:  ~882,000  (98% of orders have a review)
    ─────────────────────────
    total:        ~4,972,000
"""

import argparse
import io
import random
import sys
import time
import uuid
from datetime import datetime, timedelta

import psycopg2

# ── Realistic distributions sampled from the Olist dataset ───────────────────

STATES = [
    "SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "ES", "DF",
    "PE", "CE", "MT", "MS", "PB", "PA", "RN", "AL", "MA", "PI",
    "AM", "TO", "RO", "SE", "AC", "AP", "RR",
]
# Weighted toward SP/RJ/MG (mirrors real Olist distribution)
STATE_W = [
    42, 13, 12, 6, 5, 4, 3, 2, 2, 2,
    2, 1, 1, 1, 1, 1, 1, 1, 0.5, 0.5,
    0.4, 0.3, 0.2, 0.2, 0.1, 0.1, 0.1,
]

PRODUCT_CATEGORIES = [
    "bed_bath_table", "health_beauty", "sports_leisure", "furniture_decor",
    "computers_accessories", "housewares", "watches_gifts", "telephony",
    "auto", "toys", "cool_stuff", "garden_tools", "baby", "electronics",
    "stationery", "fashion_bags_accessories", "fashion_shoes", "office_furniture",
    "food", "perfumery", "books_general_interest", "construction_tools_safety",
    "pet_shop", "luggage_accessories", "small_appliances",
]

ORDER_STATUSES = [
    "delivered", "shipped", "canceled", "processing",
    "invoiced", "approved", "unavailable",
]
ORDER_STATUS_W = [97, 1, 0.6, 0.6, 0.4, 0.3, 0.1]

PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
PAYMENT_W = [74, 19, 5, 2]

INSTALLMENT_W = {1: 40, 2: 10, 3: 10, 4: 10, 6: 10, 8: 7, 10: 7, 12: 6}
INSTALLMENT_CHOICES = list(INSTALLMENT_W.keys())
INSTALLMENT_WEIGHTS = list(INSTALLMENT_W.values())

REVIEW_SCORES = [1, 2, 3, 4, 5]
REVIEW_SCORE_W = [4, 3, 8, 19, 66]

REVIEW_TITLES = [
    "Produto excelente", "Muito bom", "Recomendo", "Chegou rapido",
    "Bom custo beneficio", "Qualidade otima", "Produto ok", "Atendeu expectativas",
    "Nao gostei", "Produto com defeito", "Entrega rapida", "Vale a pena",
    None, None, None,  # ~20% have no title
]

CITIES = [
    "sao paulo", "rio de janeiro", "belo horizonte", "brasilia", "curitiba",
    "porto alegre", "salvador", "fortaleza", "manaus", "recife",
    "goiania", "belem", "guarulhos", "campinas", "sao luis",
    "maceio", "natal", "teresina", "campo grande", "joao pessoa",
    "florianopolis", "sorocaba", "ribeirao preto", "uberlandia", "contagem",
]

# Timestamps in the 2016–2018 range (matches Olist historical data)
_START = datetime(2016, 1, 1)
_END   = datetime(2018, 12, 31)
_RANGE_S = int((_END - _START).total_seconds())


# ── Helpers ──────────────────────────────────────────────────────────────────

def _uid():
    return str(uuid.uuid4())

def _rand_ts(base=None, max_days=None):
    if base is None:
        return _START + timedelta(seconds=random.randint(0, _RANGE_S))
    return base + timedelta(seconds=random.randint(0, int(max_days * 86400)))

def _fmt(v):
    if v is None:
        return "\\N"
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v)

def _fmt_date(v):
    if v is None:
        return "\\N"
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    return str(v)

def _row(*vals):
    return "\t".join(_fmt(v) for v in vals) + "\n"


# ── Bulk-copy helper ──────────────────────────────────────────────────────────

def _copy(cur, table, columns, gen, total, label=None):
    label = label or table
    buf = io.StringIO()
    n = 0
    flush_every = 50_000
    t0 = time.time()

    for row_str in gen:
        buf.write(row_str)
        n += 1
        if n % flush_every == 0:
            buf.seek(0)
            cur.copy_from(buf, table, columns=columns, null="\\N")
            buf = io.StringIO()
            elapsed = time.time() - t0
            rate = n / elapsed if elapsed else 0
            print(f"  {label}: {n:>10,} / {total:,}   ({rate:,.0f} rows/s)", end="\r", flush=True)

    if buf.tell():
        buf.seek(0)
        cur.copy_from(buf, table, columns=columns, null="\\N")

    elapsed = time.time() - t0
    rate = n / elapsed if elapsed else 0
    print(f"  {label}: {n:>10,} rows   ({rate:,.0f} rows/s)          ")
    return n


# ── Generators ────────────────────────────────────────────────────────────────

def _gen_customers(ids, now):
    for cid in ids:
        yield _row(
            cid, _uid(),
            str(random.randint(10000, 99999)),
            random.choice(CITIES),
            random.choices(STATES, weights=STATE_W)[0],
            now, now,
        )

def _gen_sellers(ids, now):
    for sid in ids:
        yield _row(
            sid,
            str(random.randint(10000, 99999)),
            random.choice(CITIES),
            random.choices(STATES, weights=STATE_W)[0],
            now, now,
        )

def _gen_products(ids, now):
    for pid in ids:
        yield _row(
            pid,
            random.choice(PRODUCT_CATEGORIES),
            random.randint(10, 80),
            random.randint(50, 3000),
            random.randint(1, 6),
            random.randint(100, 30000),
            random.randint(10, 100),
            random.randint(5, 80),
            random.randint(10, 100),
            now, now,
        )


# ── Main generate function ────────────────────────────────────────────────────

def generate(conn, n_orders, truncate):
    n_customers = n_orders
    n_sellers   = max(3095,  n_orders // 32)
    n_products  = max(32951, n_orders // 3)
    now = datetime.utcnow()

    print(f"\nRow targets:")
    print(f"  customers:  {n_customers:>10,}")
    print(f"  sellers:    {n_sellers:>10,}")
    print(f"  products:   {n_products:>10,}")
    print(f"  orders:     {n_orders:>10,}")
    print(f"  order_items:    ~{int(n_orders * 1.13):>7,}")
    print(f"  order_payments: ~{int(n_orders * 1.05):>7,}")
    print(f"  order_reviews:  ~{int(n_orders * 0.98):>7,}")
    print()

    with conn.cursor() as cur:
        if truncate:
            print("Truncating existing data...")
            cur.execute("""
                TRUNCATE order_reviews, order_payments, order_items, orders,
                         products, sellers, customers RESTART IDENTITY CASCADE
            """)
            conn.commit()
            print("  done\n")

        # ── 1. Customers ──────────────────────────────────────────────────────
        print("Loading customers...")
        customer_ids = [_uid() for _ in range(n_customers)]
        _copy(cur, "customers",
              ["customer_id", "customer_unique_id", "customer_zip_code",
               "customer_city", "customer_state", "created_at", "updated_at"],
              _gen_customers(customer_ids, now), n_customers)
        conn.commit()

        # ── 2. Sellers ────────────────────────────────────────────────────────
        print("Loading sellers...")
        seller_ids = [_uid() for _ in range(n_sellers)]
        _copy(cur, "sellers",
              ["seller_id", "seller_zip_code", "seller_city", "seller_state",
               "created_at", "updated_at"],
              _gen_sellers(seller_ids, now), n_sellers)
        conn.commit()

        # ── 3. Products ───────────────────────────────────────────────────────
        print("Loading products...")
        product_ids = [_uid() for _ in range(n_products)]
        _copy(cur, "products",
              ["product_id", "product_category", "product_name_length",
               "product_desc_length", "product_photos_qty", "product_weight_g",
               "product_length_cm", "product_height_cm", "product_width_cm",
               "created_at", "updated_at"],
              _gen_products(product_ids, now), n_products)
        conn.commit()

        # ── 4–7. Orders + child tables (batched together) ─────────────────────
        # Generated in 50k-order batches. Each batch commits all four tables
        # atomically so FK constraints are never violated mid-batch.
        print("Loading orders + order_items + order_payments + order_reviews...")

        BATCH = 50_000
        total_orders = total_items = total_payments = total_reviews = 0
        t0 = time.time()

        for batch_start in range(0, n_orders, BATCH):
            batch_n = min(BATCH, n_orders - batch_start)

            order_buf = io.StringIO()
            item_buf  = io.StringIO()
            pay_buf   = io.StringIO()
            rev_buf   = io.StringIO()

            for _ in range(batch_n):
                order_id    = _uid()
                customer_id = customer_ids[random.randint(0, n_customers - 1)]
                status      = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_W)[0]
                purchase_ts = _rand_ts()

                approved_ts = _rand_ts(purchase_ts, 2)  if status != "canceled" else None
                carrier_ts  = _rand_ts(purchase_ts, 7)  if status in ("delivered", "shipped") else None
                delivered_ts= _rand_ts(purchase_ts, 20) if status == "delivered" else None
                estimated_dt= _rand_ts(purchase_ts, 30)

                order_buf.write(_row(
                    order_id, customer_id, status,
                    purchase_ts, approved_ts, carrier_ts, delivered_ts,
                    _fmt_date(estimated_dt),
                    now, now,
                ))

                # order_items: 1-3 per order
                n_items = random.choices([1, 2, 3], weights=[80, 16, 4])[0]
                for seq in range(1, n_items + 1):
                    ship_ts = _rand_ts(purchase_ts, 5) if status != "canceled" else None
                    item_buf.write(_row(
                        order_id, seq,
                        product_ids[random.randint(0, n_products - 1)],
                        seller_ids[random.randint(0, n_sellers - 1)],
                        ship_ts,
                        round(random.uniform(10.0, 800.0), 2),
                        round(random.uniform(5.0, 80.0), 2),
                        now, now,
                    ))
                total_items += n_items

                # order_payments: 1-2 per order (5% have 2)
                n_pays = 2 if random.random() < 0.05 else 1
                for seq in range(1, n_pays + 1):
                    pay_buf.write(_row(
                        order_id, seq,
                        random.choices(PAYMENT_TYPES, weights=PAYMENT_W)[0],
                        random.choices(INSTALLMENT_CHOICES, weights=INSTALLMENT_WEIGHTS)[0],
                        round(random.uniform(20.0, 1500.0), 2),
                        now, now,
                    ))
                total_payments += n_pays

                # order_reviews: 98% of orders
                if random.random() < 0.98:
                    creation_ts = _rand_ts(purchase_ts, 30)
                    answer_ts   = _rand_ts(creation_ts, 5)
                    title = random.choice(REVIEW_TITLES)
                    rev_buf.write(_row(
                        _uid(), order_id,
                        random.choices(REVIEW_SCORES, weights=REVIEW_SCORE_W)[0],
                        title, None,  # comment msg left null for speed
                        creation_ts, answer_ts,
                        now, now,
                    ))
                    total_reviews += 1

            # Flush all four tables atomically
            order_buf.seek(0)
            cur.copy_from(order_buf, "orders",
                          columns=["order_id", "customer_id", "order_status",
                                   "order_purchase_timestamp", "order_approved_at",
                                   "order_delivered_carrier", "order_delivered_customer",
                                   "order_estimated_delivery", "created_at", "updated_at"],
                          null="\\N")

            item_buf.seek(0)
            cur.copy_from(item_buf, "order_items",
                          columns=["order_id", "order_item_id", "product_id", "seller_id",
                                   "shipping_limit_date", "price", "freight_value",
                                   "created_at", "updated_at"],
                          null="\\N")

            pay_buf.seek(0)
            cur.copy_from(pay_buf, "order_payments",
                          columns=["order_id", "payment_sequential", "payment_type",
                                   "payment_installments", "payment_value",
                                   "created_at", "updated_at"],
                          null="\\N")

            rev_buf.seek(0)
            cur.copy_from(rev_buf, "order_reviews",
                          columns=["review_id", "order_id", "review_score",
                                   "review_comment_title", "review_comment_msg",
                                   "review_creation_date", "review_answer_date",
                                   "created_at", "updated_at"],
                          null="\\N")

            conn.commit()
            total_orders += batch_n
            elapsed = time.time() - t0
            rate = total_orders / elapsed if elapsed else 0
            print(
                f"  orders: {total_orders:>8,}/{n_orders:,} | "
                f"items: {total_items:>8,} | payments: {total_payments:>8,} | "
                f"reviews: {total_reviews:>8,}  ({rate:,.0f} orders/s)",
                end="\r", flush=True,
            )

        elapsed = time.time() - t0
        print(
            f"\n  orders: {total_orders:,} | items: {total_items:,} | "
            f"payments: {total_payments:,} | reviews: {total_reviews:,}  "
            f"({elapsed:.0f}s)"
        )

    total = n_customers + n_sellers + n_products + total_orders + total_items + total_payments + total_reviews
    return total


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic Olist-schema data")
    parser.add_argument("--orders",   type=int, default=900_000,
                        help="Number of orders to generate (default: 900000 → ~5M total rows)")
    parser.add_argument("--host",     default="localhost")
    parser.add_argument("--port",     type=int, default=5432)
    parser.add_argument("--dbname",   default="olist")
    parser.add_argument("--user",     default="olist_user")
    parser.add_argument("--password", default="olist_pass")
    parser.add_argument("--truncate", action="store_true",
                        help="Truncate all tables before generating (removes existing Olist data)")
    parser.add_argument("--seed",     type=int, default=42,
                        help="Random seed for reproducibility (default: 42)")
    args = parser.parse_args()

    random.seed(args.seed)

    print(f"Connecting to {args.host}:{args.port}/{args.dbname}...")
    try:
        conn = psycopg2.connect(
            host=args.host, port=args.port, dbname=args.dbname,
            user=args.user, password=args.password,
        )
    except psycopg2.OperationalError as e:
        print(f"Connection failed: {e}")
        print("Is 'make port-forward' running?")
        sys.exit(1)

    conn.autocommit = False

    t_start = time.time()
    total = generate(conn, args.orders, args.truncate)
    conn.close()

    elapsed = time.time() - t_start
    print(f"\nDone. {total:,} total rows loaded in {elapsed:.0f}s ({total/elapsed:,.0f} rows/s)")
    print("\nNext steps:")
    print("  1. Bronze streaming will pick up WAL changes automatically")
    print("  2. Monitor: kubectl logs bronze-cdc-ingestion-<pod>-driver -n processing -f")
    print("  3. After Bronze settles, trigger Silver: make run-silver")
    print("  4. Re-run benchmarks: python -m benchmarks.engine_compare")


if __name__ == "__main__":
    main()
