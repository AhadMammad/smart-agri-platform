"""Integration tests against the running stack.

Requires `make up-core`. Run with `make test-integration`.

These are the tests that would have caught every Phase 1 integration mistake:
a WebHDFS redirect pointing at an unroutable container IP, a ClickHouse user
that was never created, a Metastore that started before the NameNode.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest
import requests

from smart_agri.config import LakeZone, Settings
from smart_agri.health import run_health_checks

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def live_settings() -> Settings:
    """Settings pointing at the running stack.

    Service hostnames only resolve inside the platform network, so run these
    tests from a container on `agri-net` (which is what `make test-integration`
    does) or with the host-published ports exported into the environment.
    """
    return Settings()


class TestAllServices:
    def test_every_service_is_reachable(self, live_settings: Settings) -> None:
        results = run_health_checks(live_settings, timeout_s=30)
        unhealthy = [r for r in results if not r.healthy]
        assert not unhealthy, "unreachable services:\n" + "\n".join(str(r) for r in unhealthy)


class TestHdfsLake:
    def test_all_four_zones_exist(self, live_settings: Settings) -> None:
        cfg = live_settings.hdfs
        response = requests.get(
            f"{cfg.webhdfs_url}/webhdfs/v1{cfg.lake_root}",
            params={"op": "LISTSTATUS", "user.name": cfg.user},
            timeout=30,
        )
        response.raise_for_status()
        found = {e["pathSuffix"] for e in response.json()["FileStatuses"]["FileStatus"]}
        assert {z.value for z in LakeZone} <= found

    @pytest.fixture
    def scratch_path(self, live_settings: Settings) -> Iterator[str]:
        path = live_settings.hdfs.zone_path(
            LakeZone.BRONZE, "_integration_check", f"{uuid.uuid4().hex}.txt"
        )
        yield path
        requests.delete(
            f"{live_settings.hdfs.webhdfs_url}/webhdfs/v1{path}",
            params={"op": "DELETE", "user.name": live_settings.hdfs.user},
            timeout=30,
        )

    def test_write_then_read_round_trip(self, live_settings: Settings, scratch_path: str) -> None:
        """The single most important Phase 1 check.

        A WebHDFS write is a two-step dance: the NameNode answers with a redirect
        to a DataNode, and the client follows it. If `dfs.datanode.use.datanode
        .hostname` is not set, that redirect carries a container IP the client
        cannot route to, and every lake write in every later phase fails here.
        """
        cfg = live_settings.hdfs
        payload = b"smart-agri integration check"

        create = requests.put(
            f"{cfg.webhdfs_url}/webhdfs/v1{scratch_path}",
            params={"op": "CREATE", "user.name": cfg.user, "overwrite": "true"},
            allow_redirects=False,
            timeout=30,
        )
        assert create.status_code == 307, f"expected a DataNode redirect, got {create.status_code}"

        upload = requests.put(create.headers["Location"], data=payload, timeout=60)
        assert upload.status_code == 201

        read = requests.get(
            f"{cfg.webhdfs_url}/webhdfs/v1{scratch_path}",
            params={"op": "OPEN", "user.name": cfg.user},
            timeout=30,
        )
        read.raise_for_status()
        assert read.content == payload


class TestClickHouse:
    def test_configured_database_exists(self, live_settings: Settings) -> None:
        cfg = live_settings.clickhouse
        response = requests.get(
            f"{cfg.http_url}/",
            params={"query": "SHOW DATABASES FORMAT TabSeparated"},
            auth=(cfg.user, cfg.password.get_secret_value()),
            timeout=30,
        )
        response.raise_for_status()
        assert cfg.db in response.text.split()

    def test_configured_user_can_create_and_drop(self, live_settings: Settings) -> None:
        """Read access is not enough — every Gold load writes."""
        cfg = live_settings.clickhouse
        table = f"{cfg.db}._integration_check"

        def execute(sql: str) -> requests.Response:
            response = requests.post(
                f"{cfg.http_url}/",
                data=sql.encode(),
                auth=(cfg.user, cfg.password.get_secret_value()),
                timeout=30,
            )
            response.raise_for_status()
            return response

        try:
            execute(f"CREATE TABLE {table} (x UInt32) ENGINE = MergeTree ORDER BY x")
            execute(f"INSERT INTO {table} VALUES (1), (2), (3)")
            result = execute(f"SELECT sum(x) FROM {table}")
            assert result.text.strip() == "6"
        finally:
            execute(f"DROP TABLE IF EXISTS {table}")


class TestPostgres:
    def test_accepts_a_transaction(self, live_settings: Settings) -> None:
        import psycopg

        with psycopg.connect(live_settings.postgres.uri, connect_timeout=30) as conn:
            row = conn.execute("SELECT current_database(), current_user").fetchone()

        assert row is not None
        assert row[0] == live_settings.postgres.db
        assert row[1] == live_settings.postgres.user
