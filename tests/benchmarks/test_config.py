import pytest
import yaml
from benchmarks.config import load_config, get_starrocks_conn, get_trino_conn


@pytest.fixture
def config_file(tmp_path):
    data = {
        "starrocks": {
            "host": "starrocks-fe.svc",
            "host_local": "localhost",
            "query_port": 9030,
            "user": "root",
            "password": "",
            "database": "gold",
        },
        "trino": {
            "host": "trino.svc",
            "host_local": "localhost",
            "port": 8085,
            "catalog": "lakehouse",
            "schema": "silver",
        },
    }
    p = tmp_path / "local.yaml"
    p.write_text(yaml.dump(data))
    return str(p)


def test_load_config_reads_starrocks_port(config_file):
    cfg = load_config(config_file)
    assert cfg["starrocks"]["query_port"] == 9030


def test_get_starrocks_conn_uses_local_host(config_file):
    cfg = load_config(config_file)
    conn = get_starrocks_conn(cfg)
    assert conn["host"] == "localhost"
    assert conn["port"] == 9030
    assert conn["user"] == "root"
    assert conn["database"] == "gold"


def test_get_trino_conn_returns_catalog(config_file):
    cfg = load_config(config_file)
    conn = get_trino_conn(cfg)
    assert conn["host"] == "localhost"
    assert conn["port"] == 8085
    assert conn["catalog"] == "lakehouse"
    assert conn["schema"] == "silver"
