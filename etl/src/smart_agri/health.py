"""Connectivity checks for every backing service.

These exist so Phase 1's exit criterion is executable rather than a manual
checklist: `smart-agri doctor` proves the whole stack is reachable from a task
container, which is the only vantage point that actually matters.
"""

from __future__ import annotations

import socket
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

import requests

from smart_agri.config import Settings, get_settings
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = get_logger(__name__)

DEFAULT_TIMEOUT_S = 10.0


@dataclass(frozen=True, slots=True)
class CheckResult:
    """Outcome of a single service check."""

    service: str
    healthy: bool
    detail: str

    def __str__(self) -> str:
        mark = "PASS" if self.healthy else "FAIL"
        return f"[{mark}] {self.service:<16} {self.detail}"


class HealthCheck(ABC):
    """One backing service, one check.

    Subclasses implement `_probe`; the base class turns any exception into a
    failed result so a single unreachable service never masks the others.
    """

    service: str

    def __init__(self, settings: Settings, timeout_s: float = DEFAULT_TIMEOUT_S) -> None:
        self._settings = settings
        self._timeout_s = timeout_s

    @abstractmethod
    def _probe(self) -> str:
        """Contact the service and return a short success detail.

        Raise any exception to signal failure.
        """

    def run(self) -> CheckResult:
        try:
            detail = self._probe()
        except Exception as exc:  # a failed probe is a result, not a crash
            return CheckResult(self.service, healthy=False, detail=f"{type(exc).__name__}: {exc}")
        return CheckResult(self.service, healthy=True, detail=detail)


class PostgresCheck(HealthCheck):
    """Source database reachable and answering queries."""

    service = "postgres"

    def _probe(self) -> str:
        import psycopg

        cfg = self._settings.postgres
        with psycopg.connect(cfg.uri, connect_timeout=int(self._timeout_s)) as conn:
            row = conn.execute("SELECT version()").fetchone()
        version = str(row[0]).split(",")[0] if row else "unknown"
        return f"{cfg.host}:{cfg.port}/{cfg.db} — {version}"


class ClickHouseCheck(HealthCheck):
    """Analytics warehouse reachable over HTTP."""

    service = "clickhouse"

    def _probe(self) -> str:
        cfg = self._settings.clickhouse
        response = requests.get(
            f"{cfg.http_url}/",
            params={"query": "SELECT version()"},
            auth=(cfg.user, cfg.password.get_secret_value()),
            timeout=self._timeout_s,
        )
        response.raise_for_status()
        return f"{cfg.host}:{cfg.http_port}/{cfg.db} — v{response.text.strip()}"


class HdfsCheck(HealthCheck):
    """NameNode reachable over WebHDFS and the lake zones present."""

    service = "hdfs"

    def _probe(self) -> str:
        cfg = self._settings.hdfs
        response = requests.get(
            f"{cfg.webhdfs_url}/webhdfs/v1{cfg.lake_root}",
            params={"op": "LISTSTATUS", "user.name": cfg.user},
            timeout=self._timeout_s,
        )
        response.raise_for_status()
        entries = response.json()["FileStatuses"]["FileStatus"]
        zones = sorted(entry["pathSuffix"] for entry in entries)
        if not zones:
            msg = f"{cfg.lake_root} exists but contains no zones — run `make hdfs-init`"
            raise RuntimeError(msg)
        return f"{cfg.namenode_host}:{cfg.namenode_http_port} — zones: {', '.join(zones)}"


class HiveMetastoreCheck(HealthCheck):
    """Metastore Thrift port accepting connections.

    A plain TCP probe is deliberate: pulling in a Thrift client would add a
    dependency the ETL app never otherwise needs.
    """

    service = "hive-metastore"

    def _probe(self) -> str:
        cfg = self._settings.hive
        with socket.create_connection((cfg.host, cfg.port), timeout=self._timeout_s):
            pass
        return f"{cfg.host}:{cfg.port} — thrift port open"


ALL_CHECKS: tuple[type[HealthCheck], ...] = (
    PostgresCheck,
    ClickHouseCheck,
    HdfsCheck,
    HiveMetastoreCheck,
)


def run_health_checks(
    settings: Settings | None = None,
    checks: Sequence[type[HealthCheck]] | None = None,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> list[CheckResult]:
    """Run every check and return the results in declaration order."""
    settings = settings or get_settings()
    selected = checks if checks is not None else ALL_CHECKS

    results = [check_cls(settings, timeout_s).run() for check_cls in selected]
    for result in results:
        logger.info(
            "health_check", service=result.service, healthy=result.healthy, detail=result.detail
        )
    return results
