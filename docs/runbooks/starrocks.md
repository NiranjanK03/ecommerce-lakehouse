# Runbook: StarRocks (shared-data mode)

## What it does
StarRocks is the Gold query layer. It runs in shared-data mode: CN pods are stateless and execute queries by reading Parquet files directly from MinIO. Gold materialised views store pre-aggregated results in `lakehouse-starrocks/`. The FE pod handles query planning and exposes a MySQL-compatible endpoint on port 9030.

## Deploy

```bash
make deploy-week4     # deploys StarRocks operator + cluster + Trino

# or StarRocks only
make deploy-starrocks

# Verify pods are ready
kubectl get pods -n serving -l "app.kubernetes.io/instance=kube-starrocks"
# Expected: kube-starrocks-fe-0   1/1  Running
#           kube-starrocks-cn-0   1/1  Running
```

## Initialise (storage volume + Iceberg catalog + Gold DB)

```bash
make port-forward    # start all port-forwards including StarRocks on 9030
make init-starrocks  # runs scripts/init-starrocks.sql via mysql client

# Verify manually
mysql -h 127.0.0.1 -P 9030 -u root --password=''
> SHOW STORAGE VOLUMES;
# Expected: minio_volume  ENABLED  DEFAULT=true
> SHOW CATALOGS;
# Expected: default_catalog, iceberg_catalog
> SHOW DATABASES;
# Expected: information_schema, gold
```

## Create Gold materialised views

```bash
# Requires StarRocks port-forward on 9030 and dbt-starrocks installed
pip install -r transform/dbt/requirements.txt

make init-gold-mvs   # runs: dbt run --profiles-dir transform/dbt

# Verify
mysql -h 127.0.0.1 -P 9030 -u root --password='' \
  -e "SHOW MATERIALIZED VIEWS IN gold;"
# Expected: daily_revenue, top_sellers, review_sentiment_by_state, order_funnel
```

## Trigger MV refresh manually

```bash
mysql -h 127.0.0.1 -P 9030 -u root --password='' -e "
  REFRESH MATERIALIZED VIEW gold.daily_revenue WITH SYNC MODE;
  REFRESH MATERIALIZED VIEW gold.top_sellers WITH SYNC MODE;
  REFRESH MATERIALIZED VIEW gold.review_sentiment_by_state WITH SYNC MODE;
  REFRESH MATERIALIZED VIEW gold.order_funnel WITH SYNC MODE;
"
```

## Check MV refresh status

```bash
mysql -h 127.0.0.1 -P 9030 -u root --password='' \
  -e "SELECT TABLE_NAME, LAST_REFRESH_TIME, LAST_REFRESH_STATE
      FROM information_schema.materialized_views
      WHERE TABLE_SCHEMA = 'gold';"
# LAST_REFRESH_STATE should be SUCCESS
```

## Query Silver via external Iceberg catalog

```bash
mysql -h 127.0.0.1 -P 9030 -u root --password=''
> SET CATALOG iceberg_catalog;
> USE silver;
> SELECT COUNT(*) FROM orders;
# If this returns a count, the external catalog is working
```

## Common errors

**`aws.s3.enable_path_style_access` not set → 403 from MinIO**
StarRocks constructs virtual-hosted-style URLs by default (`bucket.host/key`). MinIO requires path-style (`host/bucket/key`). Verify `init-starrocks.sql` was run and the storage volume shows `enable_path_style_access=true`.

**`CREATE STORAGE VOLUME` fails: "no default storage volume"**
This happens if you try to create a Gold database before setting the default storage volume. Run `init-starrocks.sql` in full, not just part of it.

**CN pod OOMKilled**
Increase `starRocksCnSpec.limits.memory` in `starrocks-operator.yaml` and run `make deploy-starrocks` again (Helm upgrade is safe to re-run).

**`REFRESH MATERIALIZED VIEW` returns immediately but data is stale**
You omitted `WITH SYNC MODE`. Without it the statement is asynchronous — it returns before the refresh completes. Always use `WITH SYNC MODE` in Airflow and manual scripts.

## FE Web UI

```bash
# After make port-forward (StarRocks UI forwarded to 8086 — avoids Airflow conflict on 8080)
open http://localhost:8086
```

## Logs

```bash
kubectl logs -n serving -l "app=kube-starrocks-fe" --follow
kubectl logs -n serving -l "app=kube-starrocks-cn" --follow
```
