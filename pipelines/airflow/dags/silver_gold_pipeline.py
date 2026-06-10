"""
Silver → Gold pipeline DAG.

Trigger model: self-retriggering loop.
  1. IcebergSnapshotSensor waits for new Bronze data
  2. Silver SparkApplication runs (MERGE INTO Silver Iceberg)
  3. Gold MV refresh runs (StarRocks async MVs, task is a stub)
  4. TriggerDagRunOperator re-triggers this DAG to start the loop again

This pattern gives near-continuous processing without a fixed schedule —
Silver runs within minutes of Bronze writing a new snapshot, not hourly.

Schedule: None (externally triggered on first run, self-triggering after)
First trigger: `make trigger-silver-dag` or Airflow UI → Trigger DAG

Dependencies:
  - apache-airflow-providers-cncf-kubernetes (SparkKubernetesOperator)
  - plugins/iceberg_snapshot_sensor.py (IcebergSnapshotSensor)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

from iceberg_snapshot_sensor import IcebergSnapshotSensor

CATALOG_URI = "http://iceberg-rest-catalog.infrastructure.svc.cluster.local:8181"

default_args = {
    "owner": "lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="silver_gold_pipeline",
    description="Bronze snapshot → Silver MERGE INTO → Gold MV refresh",
    schedule=None,        # triggered externally on first run, self-triggering after
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["silver", "gold", "iceberg", "spark"],
) as dag:

    # ── Step 1: wait for new Bronze data ──────────────────────────────────────
    # Polls the Iceberg REST catalog every 30 seconds.
    # Fires only when Bronze has a snapshot newer than the last run.
    # timeout=3600: if Bronze produces no data for 1 hour, fail and alert.
    wait_for_bronze = IcebergSnapshotSensor(
        task_id="wait_for_new_bronze_snapshot",
        catalog_uri=CATALOG_URI,
        table_namespace="bronze",
        table_name="orders",      # orders is the primary Bronze table — a new
                                  # snapshot here signals the full CDC batch landed
        poke_interval=30,
        timeout=3600,
        mode="poke",
    )

    # ── Step 2: run Silver MERGE INTO ─────────────────────────────────────────
    # Submits the silver-batch SparkApplication CRD to the k8s API.
    # The operator waits for the SparkApplication to reach COMPLETED state.
    # On failure: retries per default_args (2 times, 2 min apart).
    run_silver = SparkKubernetesOperator(
        task_id="silver_merge",
        namespace="processing",
        application_file="pipelines/spark/applications/silver-batch.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        delete_on_termination=True,   # clean up driver/executor pods after completion
    )

    # ── Step 3: refresh Gold MVs ──────────────────────────────────────────────
    # Connects to StarRocks FE via MySQL protocol and refreshes all four Gold MVs.
    # WITH SYNC MODE blocks until each refresh completes so Airflow can report
    # success/failure accurately. Without SYNC MODE the statement returns immediately
    # and the DAG would report success before the MV is actually populated.
    from airflow.operators.bash import BashOperator

    refresh_gold = BashOperator(
        task_id="refresh_gold_mvs",
        bash_command=(
            "mysql -h starrocks-fe.serving.svc.cluster.local "
            "-P 9030 -u root --password='' "
            "-e \""
            "REFRESH MATERIALIZED VIEW gold.daily_revenue WITH SYNC MODE; "
            "REFRESH MATERIALIZED VIEW gold.top_sellers WITH SYNC MODE; "
            "REFRESH MATERIALIZED VIEW gold.review_sentiment_by_state WITH SYNC MODE; "
            "REFRESH MATERIALIZED VIEW gold.order_funnel WITH SYNC MODE;"
            "\""
        ),
    )

    # ── Step 4: re-trigger this DAG to loop ───────────────────────────────────
    # After Silver+Gold complete, the pipeline immediately waits for the
    # next Bronze snapshot. No fixed schedule needed.
    retrigger = TriggerDagRunOperator(
        task_id="retrigger_pipeline",
        trigger_dag_id="silver_gold_pipeline",
        wait_for_completion=False,  # fire and forget — don't block this run
        reset_dag_run=False,
    )

    # ── DAG topology ──────────────────────────────────────────────────────────
    wait_for_bronze >> run_silver >> refresh_gold >> retrigger
