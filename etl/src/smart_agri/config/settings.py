"""Environment-driven configuration.

Every setting arrives as an environment variable, because each ETL task runs in
a throwaway container that Airflow launches with an env block. Nothing is read
from disk, so there is no config file to keep in sync across services.
"""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from functools import lru_cache

from pydantic import Field, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LakeZone(StrEnum):
    """The three zones of the Parquet lake, plus the rejects path."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    QUARANTINE = "quarantine"


class _Base(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=None,
        extra="ignore",
        case_sensitive=False,
    )


class PostgresSettings(_Base):
    """Connection details for the OLTP source database."""

    model_config = SettingsConfigDict(env_prefix="POSTGRES_", extra="ignore")

    host: str = "postgres"
    port: int = 5432
    db: str = "agri"
    user: str = "agri"
    password: SecretStr = SecretStr("agri")

    # Deliberately a plain property, not a `computed_field`: a computed field is
    # included in `repr()` and `model_dump()`, which would leak the password into
    # every log line that touches a settings object.
    @property
    def uri(self) -> str:
        """libpq-style URI, accepted by both psycopg and the ADBC driver."""
        return (
            f"postgresql://{self.user}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.db}"
        )


class ClickHouseSettings(_Base):
    """Connection details for the analytics warehouse."""

    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_", extra="ignore")

    host: str = "clickhouse"
    http_port: int = 8123
    db: str = "agri_analytics"
    user: str = "agri"
    password: SecretStr = SecretStr("agri")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def http_url(self) -> str:
        return f"http://{self.host}:{self.http_port}"


class HdfsSettings(_Base):
    """WebHDFS access to the Parquet lake.

    WebHDFS rather than libhdfs is a deliberate choice: it keeps the ETL image
    free of a JVM, which matters because a container starts per Airflow task.
    """

    model_config = SettingsConfigDict(env_prefix="HDFS_", extra="ignore")

    namenode_host: str = "namenode"
    namenode_http_port: int = 9870
    #: The native RPC port. The ETL never speaks it — WebHDFS is the data path —
    #: but Hive does, so table locations have to be addressed through it.
    namenode_rpc_port: int = 8020
    user: str = "root"
    lake_root: str = "/lake"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def webhdfs_url(self) -> str:
        return f"http://{self.namenode_host}:{self.namenode_http_port}"

    def zone_path(self, zone: LakeZone, *parts: str) -> str:
        """Absolute HDFS path inside a lake zone.

        >>> HdfsSettings().zone_path(LakeZone.BRONZE, "farm", "snapshot_date=2026-07-31")
        '/lake/bronze/farm/snapshot_date=2026-07-31'
        """
        suffix = "/".join(p.strip("/") for p in parts if p)
        base = f"{self.lake_root.rstrip('/')}/{zone.value}"
        return f"{base}/{suffix}" if suffix else base

    def zone_uri(self, zone: LakeZone, *parts: str) -> str:
        """`zone_path` as a fully-qualified `hdfs://` URI.

        Hive resolves a scheme-less location against its own `fs.defaultFS`,
        which in this stack is the local filesystem — so a bare path would
        register a table over an empty directory inside the Hive container
        rather than over the lake. Qualifying the URI here keeps the DDL correct
        regardless of how Hive is configured.

        >>> HdfsSettings().zone_uri(LakeZone.BRONZE, "farm")
        'hdfs://namenode:8020/lake/bronze/farm'
        """
        authority = f"hdfs://{self.namenode_host}:{self.namenode_rpc_port}"
        return f"{authority}{self.zone_path(zone, *parts)}"


class HiveSettings(_Base):
    """Hive Metastore — the catalog over the lake zones."""

    model_config = SettingsConfigDict(env_prefix="HIVE_METASTORE_", extra="ignore")

    host: str = "hive-metastore"
    port: int = 9083


class OpenMeteoSettings(_Base):
    """The weather API — the platform's only external data source.

    Free tier, no API key, but genuinely rate limited: a backfill across many
    farms will be throttled without `max_rps`, and a throttled request that is
    not retried loses that farm's history silently.
    """

    model_config = SettingsConfigDict(env_prefix="OPEN_METEO_", extra="ignore")

    archive_url: str = "https://archive-api.open-meteo.com/v1/archive"
    forecast_url: str = "https://api.open-meteo.com/v1/forecast"

    max_rps: float = Field(default=1.0, gt=0, description="Requests per second ceiling")
    max_retries: int = Field(default=5, ge=0)
    timeout_s: float = Field(default=30.0, gt=0)

    #: The archive lags real time by several days, so anything inside this
    #: window has to come from the forecast endpoint's `past_days` instead.
    archive_lag_days: int = Field(default=6, ge=0, le=30)

    #: How far either side of today the daily run reaches.
    forecast_past_days: int = Field(default=7, ge=0, le=92)
    forecast_days: int = Field(default=7, ge=1, le=16)

    #: Earliest date the backfill will request. Defaults to the start of the
    #: generator's `small` window so weather covers the whole dataset.
    backfill_start: date = Field(default=date(2025, 8, 1))


class Settings(_Base):
    """Root settings object. Compose the service blocks; do not nest env prefixes."""

    env: str = Field(default="dev", alias="SMART_AGRI_ENV")
    log_level: str = Field(default="INFO", alias="SMART_AGRI_LOG_LEVEL")

    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    clickhouse: ClickHouseSettings = Field(default_factory=ClickHouseSettings)
    hdfs: HdfsSettings = Field(default_factory=HdfsSettings)
    hive: HiveSettings = Field(default_factory=HiveSettings)
    open_meteo: OpenMeteoSettings = Field(default_factory=OpenMeteoSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings singleton.

    Cached because a task container reads its environment once and never changes
    it; tests clear the cache via `get_settings.cache_clear()`.
    """
    return Settings()
