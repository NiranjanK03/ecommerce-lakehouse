#!/usr/bin/env bash
# Load the full Olist Brazilian e-commerce dataset into PostgreSQL.
#
# Prerequisites:
#   1. Download CSVs from https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
#      and extract into ./data/olist-raw/
#   2. `make port-forward` must be running (PostgreSQL on localhost:5432)
#   3. psql must be installed locally

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../data/olist-raw"
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-olist_user}"
PGPASSWORD="${PGPASSWORD:-olist_pass}"
PGDATABASE="${PGDATABASE:-olist}"
export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE

if [ ! -d "$DATA_DIR" ]; then
    echo "ERROR: $DATA_DIR not found."
    echo ""
    echo "Download the Olist dataset from Kaggle:"
    echo "  https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
    echo ""
    echo "Then extract the CSVs to: $DATA_DIR"
    echo "Expected files:"
    echo "  olist_customers_dataset.csv"
    echo "  olist_sellers_dataset.csv"
    echo "  olist_products_dataset.csv"
    echo "  olist_orders_dataset.csv"
    echo "  olist_order_items_dataset.csv"
    echo "  olist_order_payments_dataset.csv"
    echo "  olist_order_reviews_dataset.csv"
    echo "  olist_geolocation_dataset.csv"
    echo "  product_category_name_translation.csv"
    exit 1
fi

check_psql() {
    if ! command -v psql &>/dev/null; then
        echo "psql not found. Install with: brew install libpq && brew link --force libpq"
        exit 1
    fi
}

load_csv() {
    local table=$1 file=$2 columns=$3
    if [ ! -f "$DATA_DIR/$file" ]; then
        echo "  [skip] $file not found"
        return
    fi
    echo "  Loading $table from $file..."
    # Use \copy (client-side) instead of COPY (server-side) so no superuser needed.
    # The header row is skipped via HEADER option.
    psql -c "\copy $table ($columns) FROM '$DATA_DIR/$file' WITH (FORMAT csv, HEADER true, NULL '')"
    local count
    count=$(psql -tA -c "SELECT count(*) FROM $table")
    echo "    → $count rows"
}

check_psql

echo "==> Loading Olist dataset into PostgreSQL ($PGHOST:$PGPORT/$PGDATABASE)"
echo ""

# Truncate in FK-safe order before reload
psql -c "TRUNCATE order_reviews, order_payments, order_items, orders, products, sellers, customers, geolocation, category_translations RESTART IDENTITY CASCADE;"

load_csv customers           olist_customers_dataset.csv \
    "customer_id,customer_unique_id,customer_zip_code,customer_city,customer_state"

load_csv sellers             olist_sellers_dataset.csv \
    "seller_id,seller_zip_code,seller_city,seller_state"

load_csv products            olist_products_dataset.csv \
    "product_id,product_category,product_name_length,product_desc_length,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm"

load_csv orders              olist_orders_dataset.csv \
    "order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier,order_delivered_customer,order_estimated_delivery"

load_csv order_items         olist_order_items_dataset.csv \
    "order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value"

load_csv order_payments      olist_order_payments_dataset.csv \
    "order_id,payment_sequential,payment_type,payment_installments,payment_value"

load_csv order_reviews       olist_order_reviews_dataset.csv \
    "review_id,order_id,review_score,review_comment_title,review_comment_msg,review_creation_date,review_answer_date"

load_csv geolocation         olist_geolocation_dataset.csv \
    "geolocation_zip_code,geolocation_lat,geolocation_lng,geolocation_city,geolocation_state"

load_csv category_translations product_category_name_translation.csv \
    "product_category_name,product_category_name_english"

echo ""
echo "==> Dataset loaded successfully."
echo "    Verify with: psql -c 'SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;'"
