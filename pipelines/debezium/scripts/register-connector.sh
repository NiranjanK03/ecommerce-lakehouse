#!/usr/bin/env bash
# Register the Olist Debezium connector via the Kafka Connect REST API.
#
# Run this AFTER debezium-connect is Ready and port-forward is active:
#   kubectl port-forward svc/debezium-connect 8083:8083 -n streaming &
#   bash pipelines/debezium/scripts/register-connector.sh
#
# The connector will immediately begin the initial snapshot — reading all
# existing rows from PostgreSQL and emitting them as op="r" events to Kafka.
# After the snapshot, it switches to streaming mode (WAL changes only).

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/connectors/olist-connector.json"

echo "==> Checking Kafka Connect is ready..."
until curl -sf "${CONNECT_URL}/connectors" >/dev/null; do
  echo "  waiting for Connect REST API..."
  sleep 3
done

echo "==> Registering olist-postgres-connector..."
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -X GET "${CONNECT_URL}/connectors/olist-postgres-connector")

if [ "$STATUS" = "200" ]; then
  echo "  Connector already registered — checking status..."
  curl -s "${CONNECT_URL}/connectors/olist-postgres-connector/status" | python3 -m json.tool
  exit 0
fi

curl -sf \
  -X POST \
  -H "Content-Type: application/json" \
  --data @"$CONNECTOR_FILE" \
  "${CONNECT_URL}/connectors" | python3 -m json.tool

echo ""
echo "==> Connector registered. Monitoring status..."
sleep 5
curl -s "${CONNECT_URL}/connectors/olist-postgres-connector/status" | python3 -m json.tool

echo ""
echo "  Watch Kafka topics being populated:"
echo "  kubectl exec -n streaming kafka-controller-0 -- \\"
echo "    kafka-console-consumer.sh --bootstrap-server localhost:9092 \\"
echo "    --topic olist.public.orders --from-beginning --max-messages 3"
