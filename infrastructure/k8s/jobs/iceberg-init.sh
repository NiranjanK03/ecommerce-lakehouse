#!/bin/sh
# Create Bronze Iceberg tables via REST API (no Spark needed for DDL).
set -e

CATALOG="http://iceberg-rest-catalog.infrastructure.svc.cluster.local:8181"

# ---------- helpers ----------
post() {
  url="$1"; body="$2"
  resp=$(wget -qO- --header="Content-Type: application/json" --post-data="$body" "$url" 2>&1)
  echo "$resp"
}

create_table() {
  ns="$1"; tbl="$2"
  echo "Creating lakehouse.${ns}.${tbl} ..."
  post "${CATALOG}/v1/namespaces/${ns}/tables" "{
    \"name\": \"${tbl}\",
    \"schema\": {
      \"type\": \"struct\",
      \"schema-id\": 0,
      \"identifier-field-ids\": [],
      \"fields\": [
        {\"id\": 1, \"name\": \"_cdc_op\",           \"required\": true,  \"type\": \"string\"},
        {\"id\": 2, \"name\": \"_cdc_source_ts_ms\",  \"required\": true,  \"type\": \"long\"},
        {\"id\": 3, \"name\": \"_cdc_event_ts_ms\",   \"required\": true,  \"type\": \"long\"},
        {\"id\": 4, \"name\": \"_cdc_ingested_at\",   \"required\": true,  \"type\": \"timestamptz\"},
        {\"id\": 5, \"name\": \"_cdc_topic\",         \"required\": true,  \"type\": \"string\"},
        {\"id\": 6, \"name\": \"_cdc_partition\",     \"required\": true,  \"type\": \"int\"},
        {\"id\": 7, \"name\": \"_cdc_offset\",        \"required\": true,  \"type\": \"long\"},
        {\"id\": 8, \"name\": \"before_json\",        \"required\": false, \"type\": \"string\"},
        {\"id\": 9, \"name\": \"after_json\",         \"required\": false, \"type\": \"string\"}
      ]
    },
    \"partition-spec\": {
      \"spec-id\": 0,
      \"fields\": [
        {\"field-id\": 1000, \"source-id\": 4, \"name\": \"_cdc_ingested_at_day\", \"transform\": \"day\"}
      ]
    },
    \"write-order\": {\"order-id\": 0, \"fields\": []},
    \"properties\": {
      \"write.format.default\": \"parquet\",
      \"write.parquet.compression-codec\": \"snappy\",
      \"write.metadata.delete-after-commit.enabled\": \"true\",
      \"write.metadata.previous-versions-max\": \"10\"
    }
  }"
  echo "  OK"
}

# ---------- create namespace ----------
echo "Creating namespace lakehouse.bronze ..."
post "${CATALOG}/v1/namespaces" '{"namespace": ["bronze"], "properties": {}}' | head -1
echo "  OK"

# ---------- create tables ----------
for tbl in orders order_items customers products sellers order_payments order_reviews; do
  create_table "bronze" "$tbl"
done

echo ""
echo "Bronze tables created. Listing:"
wget -qO- "${CATALOG}/v1/namespaces/bronze/tables" 2>&1
