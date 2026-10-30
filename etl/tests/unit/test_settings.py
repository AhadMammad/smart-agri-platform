"""Tests for configuration loading and derived values."""

from __future__ import annotations

import pytest

from smart_agri.config import (
    ClickHouseSettings,
    DataQualitySettings,
    EtlMetaSettings,
    HdfsSettings,
    LakeZone,
    PostgresSettings,
    Settings,
    get_settings,
)


class TestPostgresSettings:
    def test_uri_is_assembled_from_parts(self) -> None:
        cfg = PostgresSettings(host="db", port=5433, db="agri", user="u")
        assert cfg.uri == "postgresql://u:agri@db:5433/agri"

    def test_password_is_not_leaked_by_repr(self) -> None:
        cfg = PostgresSettings(password="s3cret")  # type: ignore[arg-type]
        assert "s3cret" not in repr(cfg)
        assert cfg.password.get_secret_value() == "s3cret"

    def test_password_is_not_leaked_by_model_dump(self) -> None:
        """`uri` must stay a plain property; as a computed field it would be
        serialised here, putting the password into any settings dump."""
        cfg = PostgresSettings(password="s3cret")  # type: ignore[arg-type]
        assert "s3cret" not in str(cfg.model_dump())
        assert "uri" not in cfg.model_dump()

    def test_reads_prefixed_environment_variables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("POSTGRES_HOST", "pg.internal")
        monkeypatch.setenv("POSTGRES_PORT", "6543")
        cfg = PostgresSettings()
        assert (cfg.host, cfg.port) == ("pg.internal", 6543)


class TestEtlMetaSettings:
    def test_defaults_point_at_the_metadata_database_not_the_source(self) -> None:
        cfg = EtlMetaSettings()
        assert cfg.uri == "postgresql://etl:etl@etl-meta-db:5432/etl_meta"

    def test_reads_its_own_prefix_and_ignores_the_source_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sharing POSTGRES_* would silently put the run log back in the source."""
        monkeypatch.setenv("POSTGRES_HOST", "source.internal")
        monkeypatch.setenv("ETL_META_HOST", "meta.internal")
        assert EtlMetaSettings().host == "meta.internal"

    def test_password_is_not_leaked_by_model_dump(self) -> None:
        cfg = EtlMetaSettings(password="s3cret")  # type: ignore[arg-type]
        assert "s3cret" not in str(cfg.model_dump())


class TestDataQualitySettings:
    def test_enforcement_is_on_by_default(self) -> None:
        """Off by default would let every error-severity breach through."""
        assert DataQualitySettings().enforce is True

    def test_enforcement_can_be_disabled_for_a_backfill(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SMART_AGRI_DQ_ENFORCE", "false")
        assert Settings().dq.enforce is False


class TestClickHouseSettings:
    def test_http_url(self) -> None:
        cfg = ClickHouseSettings(host="ch", http_port=8124)
        assert cfg.http_url == "http://ch:8124"


class TestHdfsSettings:
    def test_webhdfs_url(self) -> None:
        cfg = HdfsSettings(namenode_host="nn", namenode_http_port=9871)
        assert cfg.webhdfs_url == "http://nn:9871"

    @pytest.mark.parametrize(
        ("zone", "parts", "expected"),
        [
            (LakeZone.BRONZE, (), "/lake/bronze"),
            (LakeZone.SILVER, ("farm",), "/lake/silver/farm"),
            (
                LakeZone.GOLD,
                ("field_daily", "dt=2026-07-31"),
                "/lake/gold/field_daily/dt=2026-07-31",
            ),
            (LakeZone.QUARANTINE, ("sensor_reading",), "/lake/quarantine/sensor_reading"),
        ],
    )
    def test_zone_path(self, zone: LakeZone, parts: tuple[str, ...], expected: str) -> None:
        assert HdfsSettings().zone_path(zone, *parts) == expected

    def test_zone_path_normalises_stray_slashes(self) -> None:
        cfg = HdfsSettings(lake_root="/lake/")
        assert cfg.zone_path(LakeZone.BRONZE, "/farm/", "/dt=2026-07-31") == (
            "/lake/bronze/farm/dt=2026-07-31"
        )

    def test_zone_path_skips_empty_parts(self) -> None:
        assert HdfsSettings().zone_path(LakeZone.BRONZE, "", "farm") == "/lake/bronze/farm"


class TestSettings:
    def test_defaults_compose_every_service_block(self, settings: Settings) -> None:
        assert settings.postgres.host == "postgres"
        assert settings.etl_meta.host == "etl-meta-db"
        assert settings.clickhouse.host == "clickhouse"
        assert settings.hdfs.namenode_host == "namenode"
        assert settings.hive.port == 9083

    def test_root_aliases_are_honoured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SMART_AGRI_ENV", "ci")
        monkeypatch.setenv("SMART_AGRI_LOG_LEVEL", "DEBUG")
        cfg = Settings()
        assert (cfg.env, cfg.log_level) == ("ci", "DEBUG")

    def test_get_settings_is_cached(self) -> None:
        assert get_settings() is get_settings()
