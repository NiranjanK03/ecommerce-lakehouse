"""
IcebergSnapshotSensor — custom Airflow sensor.

Fires when an Iceberg table has a snapshot newer than the last time
this sensor successfully completed. Uses the Iceberg REST Catalog API
to read snapshot metadata — no data files are read.

Why this instead of a time-based schedule:
  A time-based schedule (every hour) runs Silver even when Bronze has no
  new data — wasted compute. This sensor fires only when Bronze has written
  at least one new snapshot since the last Silver run, reducing Silver
  invocations to exactly the number of Bronze commits.

Why snapshot ID comparison instead of timestamp:
  Timestamps can collide if two snapshots are committed within the same
  millisecond (unlikely but possible). Snapshot IDs are monotonically
  increasing integers — comparing them is unambiguous.

Deployment:
  Copy to Airflow's plugins directory:
    make sync-plugins
  Airflow auto-discovers plugins in AIRFLOW__CORE__PLUGINS_FOLDER.
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from airflow.models import Variable
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

log = logging.getLogger(__name__)

# Airflow Variable key used to persist the last processed snapshot ID
# across DAG runs. Stored in the Airflow metadata database.
_VARIABLE_KEY_TEMPLATE = "iceberg_last_snapshot_{namespace}_{table}"


class IcebergSnapshotSensor(BaseSensorOperator):
    """
    Polls the Iceberg REST Catalog until a new snapshot appears on the
    target table since the last time this sensor fired.

    Args:
        catalog_uri: Base URI of the Iceberg REST Catalog
                     (e.g. http://iceberg-rest-catalog.infrastructure:8181)
        table_namespace: Iceberg namespace (e.g. "bronze")
        table_name: Table name (e.g. "orders")
        poke_interval: Seconds between REST catalog polls (default 30)
        timeout: Max seconds to wait before marking the sensor as failed
    """

    template_fields = ("catalog_uri", "table_namespace", "table_name")

    def __init__(
        self,
        catalog_uri: str,
        table_namespace: str,
        table_name: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.catalog_uri = catalog_uri.rstrip("/")
        self.table_namespace = table_namespace
        self.table_name = table_name
        self._variable_key = _VARIABLE_KEY_TEMPLATE.format(
            namespace=table_namespace, table=table_name
        )

    def _get_current_snapshot_id(self) -> int | None:
        """Query the REST catalog for the table's current snapshot ID."""
        url = (
            f"{self.catalog_uri}/v1/namespaces/{self.table_namespace}"
            f"/tables/{self.table_name}"
        )
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.warning("Catalog request failed: %s — will retry", exc)
            return None

        metadata = resp.json().get("metadata", {})
        snapshot_id = metadata.get("current-snapshot-id")
        return int(snapshot_id) if snapshot_id is not None else None

    def poke(self, context: Context) -> bool:
        current_snapshot_id = self._get_current_snapshot_id()

        if current_snapshot_id is None:
            log.info(
                "No snapshot found on %s.%s — table may be empty",
                self.table_namespace,
                self.table_name,
            )
            return False

        # Retrieve the last snapshot ID this sensor processed.
        # Variable.get returns None (not found) or the stored string.
        last_snapshot_str = Variable.get(self._variable_key, default_var=None)
        last_snapshot_id = int(last_snapshot_str) if last_snapshot_str else None

        log.info(
            "Snapshot check — table: %s.%s  current: %s  last_processed: %s",
            self.table_namespace,
            self.table_name,
            current_snapshot_id,
            last_snapshot_id,
        )

        if last_snapshot_id is None or current_snapshot_id != last_snapshot_id:
            # New snapshot available — store it and let the DAG proceed.
            # Push to XCom so downstream tasks know which snapshot triggered them.
            context["ti"].xcom_push(
                key="triggered_snapshot_id", value=current_snapshot_id
            )
            Variable.set(self._variable_key, str(current_snapshot_id))
            log.info(
                "New snapshot detected (%s) — firing sensor", current_snapshot_id
            )
            return True

        log.info("No new snapshot since last run — waiting")
        return False
