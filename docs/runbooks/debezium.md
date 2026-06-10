# Runbook: Debezium CDC

## What it does
Debezium reads PostgreSQL's Write-Ahead Log via a replication slot and publishes every INSERT, UPDATE, and DELETE as a JSON event to Kafka. It runs as a Kafka Connect worker in the `streaming` namespace.

## Deploy

```bash
make deploy-debezium

# Verify the pod is ready
kubectl get pods -n streaming -l app=debezium-connect
# Expected: debezium-connect-XXX  1/1  Running
```

## Register the connector

```bash
# Port-forward first
kubectl port-forward svc/debezium-connect 8083:8083 -n streaming &

# Register
make register-connector
# or: bash pipelines/debezium/scripts/register-connector.sh
```

## Verify

```bash
# Check connector status
make connector-status
# Expected: {"connector":{"state":"RUNNING",...},"tasks":[{"state":"RUNNING",...}]}

# Watch events arriving in Kafka for orders topic
kubectl exec -n streaming kafka-controller-0 -- \
  kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic olist.public.orders \
  --from-beginning \
  --max-messages 3
```

## Connector lifecycle

**Initial snapshot:** When first registered, Debezium reads every existing row in all 7 tables and emits each as an event with `op: "r"` (read). This gets all historical Olist data into Kafka. After the snapshot completes, Debezium switches to streaming WAL changes only (`op: "c"/"u"/"d"`).

**Replication slot:** Debezium creates a PostgreSQL replication slot named `debezium_olist`. Check it is active:
```bash
kubectl port-forward svc/postgres-postgresql 5432:5432 -n infrastructure &
psql -h localhost -U olist_user -d olist \
  -c "SELECT slot_name, active, restart_lsn FROM pg_replication_slots;"
# Expected: debezium_olist | t | ...
```

## Common issues

**Connector state is FAILED:**
```bash
curl -s http://localhost:8083/connectors/olist-postgres-connector/status | jq '.tasks[].trace'
# Common causes:
# - PostgreSQL not reachable: check postgres-postgresql service in infrastructure ns
# - wal_level not set to logical: check postgres.yaml extendedConfiguration
# - Replication slot already exists from a crashed connector: DROP the slot manually
```

**Drop and recreate the replication slot:**
```bash
psql -h localhost -U olist_user -d olist \
  -c "SELECT pg_drop_replication_slot('debezium_olist');"
# Then delete and re-register the connector
curl -X DELETE http://localhost:8083/connectors/olist-postgres-connector
make register-connector
```

**Kafka consumer lag growing (Bronze not keeping up):**
Check Bronze SparkApplication is running:
```bash
kubectl get sparkapplication -n processing
kubectl logs bronze-cdc-ingestion-*-driver -n processing --tail=50
```

## Heartbeat table

The connector config includes a heartbeat action that updates a heartbeat table in PostgreSQL every 5 seconds. This keeps the WAL position advancing even when source tables are quiet, preventing WAL bloat. Create the table:

```sql
CREATE TABLE IF NOT EXISTS public.debezium_heartbeat (
  id INTEGER PRIMARY KEY DEFAULT 1,
  last_heartbeat_ts TIMESTAMP
);
INSERT INTO public.debezium_heartbeat VALUES (1, NOW())
  ON CONFLICT DO NOTHING;
```
