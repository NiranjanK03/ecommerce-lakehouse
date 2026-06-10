"""Load connection details for benchmark engines from config/local.yaml."""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import yaml


def load_config(config_path: Optional[str] = None) -> dict:
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config" / "local.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_starrocks_conn(config: dict) -> dict:
    sr = config["starrocks"]
    return {
        "host": sr["host_local"],
        "port": sr["query_port"],
        "user": sr["user"],
        "password": sr.get("password", ""),
        "database": sr["database"],
    }


def get_trino_conn(config: dict) -> dict:
    t = config["trino"]
    return {
        "host": t["host_local"],
        "port": t["port"],
        "catalog": t["catalog"],
        "schema": t["schema"],
    }
