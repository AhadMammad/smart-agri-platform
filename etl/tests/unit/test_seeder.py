"""Tests for the dataset seeder."""

from __future__ import annotations

from datetime import date

import pytest
from tests.fakes import FakePostgresSource

from smart_agri.generator import GeneratorConfig
from smart_agri.generator.seeder import DatasetSeeder

TINY = GeneratorConfig(
    n_farms=2,
    fields_per_farm=2,
    sensors_per_field=2,
    start_date=date(2026, 7, 1),
    end_date=date(2026, 7, 1),
    reading_interval_minutes=720,
)


@pytest.fixture
def postgres() -> FakePostgresSource:
    return FakePostgresSource()


@pytest.fixture
def seeder(postgres: FakePostgresSource) -> DatasetSeeder:
    return DatasetSeeder(postgres, TINY)  # type: ignore[arg-type]


class TestSeeding:
    def test_writes_every_level(self, seeder: DatasetSeeder, postgres: FakePostgresSource) -> None:
        result = seeder.seed()

        assert result.farms == 2
        assert result.fields == 4
        assert result.sensors == 8
        assert result.readings > 0
        assert result.total == result.farms + result.fields + result.sensors + result.readings

        assert {"agri.farm", "agri.field", "agri.sensor", "agri.sensor_reading"} <= set(
            postgres.tables
        )

    def test_truncates_child_tables_first(
        self, seeder: DatasetSeeder, postgres: FakePostgresSource
    ) -> None:
        """Foreign keys would otherwise block the removal order."""
        seeder.seed()
        assert postgres.truncated[0] == "agri.sensor_reading"
        assert postgres.truncated[-1] == "agri.farm"

    def test_keep_skips_the_truncate(
        self, seeder: DatasetSeeder, postgres: FakePostgresSource
    ) -> None:
        seeder.seed(truncate=False)
        assert postgres.truncated == []

    def test_foreign_keys_are_resolved_to_surrogate_ids(
        self, seeder: DatasetSeeder, postgres: FakePostgresSource
    ) -> None:
        """Postgres assigns the ids, so each level is read back before the next
        is written; a broken lookup would surface as a null or a KeyError."""
        seeder.seed()

        farm_ids = set(postgres.tables["agri.farm"]["farm_id"].to_list())
        field_farm_ids = set(postgres.tables["agri.field"]["farm_id"].to_list())
        assert field_farm_ids <= farm_ids

        field_ids = set(postgres.tables["agri.field"]["field_id"].to_list())
        sensor_field_ids = set(postgres.tables["agri.sensor"]["field_id"].to_list())
        assert sensor_field_ids <= field_ids

    def test_readings_reference_real_sensors(
        self, seeder: DatasetSeeder, postgres: FakePostgresSource
    ) -> None:
        seeder.seed()
        sensor_ids = set(postgres.tables["agri.sensor"]["sensor_id"].to_list())
        reading_sensor_ids = set(postgres.tables["agri.sensor_reading"]["sensor_id"].to_list())
        assert reading_sensor_ids <= sensor_ids
        assert reading_sensor_ids  # not vacuously true

    def test_is_deterministic(self, postgres: FakePostgresSource) -> None:
        first = DatasetSeeder(postgres, TINY).seed()  # type: ignore[arg-type]
        second = DatasetSeeder(FakePostgresSource(), TINY).seed()  # type: ignore[arg-type]
        assert first == second

    def test_exposes_the_config_it_used(self, seeder: DatasetSeeder) -> None:
        assert seeder.config.n_farms == TINY.n_farms
