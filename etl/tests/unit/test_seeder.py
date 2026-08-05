"""Tests for the dataset seeder.

The seeder's real job is resolving natural keys to the surrogate ids Postgres
assigns, across a graph fifteen tables deep. These tests exercise that against
the in-memory fake, which mimics identity assignment — so a broken lookup
surfaces as an orphaned reference rather than passing silently.
"""

from __future__ import annotations

from datetime import date

import pytest
from tests.fakes import FakePostgresSource

from smart_agri.generator import GeneratorConfig
from smart_agri.generator.seeder import (
    TABLES_IN_DEPENDENCY_ORDER,
    DatasetSeeder,
    SeedResult,
)

# A full year, so every crop cycle completes and the harvest tables populate.
SMALL = GeneratorConfig(
    n_farms=3,
    fields_per_farm=2,
    sensors_per_field=1,
    machines_per_farm=3,
    start_date=date(2025, 8, 1),
    end_date=date(2026, 8, 1),
    reading_interval_minutes=720,
)


@pytest.fixture
def postgres() -> FakePostgresSource:
    return FakePostgresSource()


@pytest.fixture
def seeder(postgres: FakePostgresSource) -> DatasetSeeder:
    return DatasetSeeder(postgres, SMALL)  # type: ignore[arg-type]


@pytest.fixture
def seeded(seeder: DatasetSeeder, postgres: FakePostgresSource) -> FakePostgresSource:
    seeder.seed()
    return postgres


class TestCoverage:
    def test_every_table_receives_rows(self, seeder: DatasetSeeder) -> None:
        """Phase 3's exit criterion: `make seed` populates every table."""
        result = seeder.seed()
        empty = [name for name, count in result.counts.items() if count == 0]
        assert not empty, f"tables left empty: {empty}"

    def test_result_totals_and_summary(self, seeder: DatasetSeeder) -> None:
        result = seeder.seed()
        assert result.total == sum(result.counts.values())
        assert "total" in result.summary()

    def test_writes_to_every_declared_table(self, seeded: FakePostgresSource) -> None:
        assert set(TABLES_IN_DEPENDENCY_ORDER) <= set(seeded.tables)


class TestTruncation:
    def test_truncates_children_before_parents(
        self, seeder: DatasetSeeder, postgres: FakePostgresSource
    ) -> None:
        """Foreign keys would otherwise block the removal order."""
        seeder.seed()
        assert postgres.truncated[0] == "agri.sensor_reading"
        assert postgres.truncated[-1] == "agri.crop_variety"
        assert postgres.truncated.index("agri.planting") < postgres.truncated.index("agri.field")

    def test_reference_tables_are_not_truncated(
        self, seeder: DatasetSeeder, postgres: FakePostgresSource
    ) -> None:
        """`crop`, `region` and `soil_type` are Liquibase-managed, and
        `farm.region` carries a foreign key onto one of them."""
        seeder.seed()
        for reference in ("agri.crop", "agri.region", "agri.soil_type"):
            assert reference not in postgres.truncated

    def test_keep_skips_the_truncate(
        self, seeder: DatasetSeeder, postgres: FakePostgresSource
    ) -> None:
        seeder.seed(truncate=False)
        assert postgres.truncated == []


class TestReferentialIntegrity:
    @staticmethod
    def _ids(postgres: FakePostgresSource, table: str, column: str) -> set[int]:
        return set(postgres.tables[table][column].to_list())

    @staticmethod
    def _refs(postgres: FakePostgresSource, table: str, column: str) -> set[int]:
        return set(postgres.tables[table][column].drop_nulls().to_list())

    def test_fields_reference_real_farms(self, seeded: FakePostgresSource) -> None:
        assert self._refs(seeded, "agri.field", "farm_id") <= self._ids(
            seeded, "agri.farm", "farm_id"
        )

    def test_sensors_reference_real_fields(self, seeded: FakePostgresSource) -> None:
        assert self._refs(seeded, "agri.sensor", "field_id") <= self._ids(
            seeded, "agri.field", "field_id"
        )

    def test_plantings_reference_real_fields_and_varieties(
        self, seeded: FakePostgresSource
    ) -> None:
        assert self._refs(seeded, "agri.planting", "field_id") <= self._ids(
            seeded, "agri.field", "field_id"
        )
        assert self._refs(seeded, "agri.planting", "variety_id") <= self._ids(
            seeded, "agri.crop_variety", "variety_id"
        )

    @pytest.mark.parametrize(
        "table",
        [
            "agri.irrigation_event",
            "agri.input_application",
            "agri.harvest",
            "agri.field_cost",
            "agri.field_index_observation",
        ],
    )
    def test_operational_tables_reference_real_plantings(
        self, seeded: FakePostgresSource, table: str
    ) -> None:
        """The planting key is reassembled from field, season and sowing date.
        A mismatch there would orphan rows across all of these at once."""
        assert self._refs(seeded, table, "planting_id") <= self._ids(
            seeded, "agri.planting", "planting_id"
        )

    def test_machines_reference_real_farms(self, seeded: FakePostgresSource) -> None:
        assert self._refs(seeded, "agri.machine", "farm_id") <= self._ids(
            seeded, "agri.farm", "farm_id"
        )

    def test_machine_operations_reference_real_machines_and_fields(
        self, seeded: FakePostgresSource
    ) -> None:
        assert self._refs(seeded, "agri.machine_operation", "machine_id") <= self._ids(
            seeded, "agri.machine", "machine_id"
        )
        assert self._refs(seeded, "agri.machine_operation", "field_id") <= self._ids(
            seeded, "agri.field", "field_id"
        )

    def test_telemetry_references_real_machines_and_operations(
        self, seeded: FakePostgresSource
    ) -> None:
        assert self._refs(seeded, "agri.machine_telemetry", "machine_id") <= self._ids(
            seeded, "agri.machine", "machine_id"
        )
        assert self._refs(seeded, "agri.machine_telemetry", "operation_id") <= self._ids(
            seeded, "agri.machine_operation", "operation_id"
        )

    def test_some_telemetry_is_linked_to_an_operation(self, seeded: FakePostgresSource) -> None:
        """A parked heartbeat has no operation, but working samples must — an
        all-null column would mean the operation key never resolved."""
        assert seeded.tables["agri.machine_telemetry"]["operation_id"].drop_nulls().len() > 0

    def test_readings_reference_real_sensors(self, seeded: FakePostgresSource) -> None:
        assert self._refs(seeded, "agri.sensor_reading", "sensor_id") <= self._ids(
            seeded, "agri.sensor", "sensor_id"
        )

    def test_harvest_is_unique_per_planting(self, seeded: FakePostgresSource) -> None:
        planting_ids = seeded.tables["agri.harvest"]["planting_id"].to_list()
        assert len(planting_ids) == len(set(planting_ids))


class TestDeterminism:
    def test_same_config_and_seed_produce_the_same_counts(self) -> None:
        first = DatasetSeeder(FakePostgresSource(), SMALL).seed()  # type: ignore[arg-type]
        second = DatasetSeeder(FakePostgresSource(), SMALL).seed()  # type: ignore[arg-type]
        assert first == second

    def test_a_different_seed_changes_the_data(self) -> None:
        other = SMALL.model_copy(update={"seed": SMALL.seed + 1})
        first = FakePostgresSource()
        second = FakePostgresSource()
        DatasetSeeder(first, SMALL).seed()  # type: ignore[arg-type]
        DatasetSeeder(second, other).seed()  # type: ignore[arg-type]
        assert not first.tables["agri.farm"].equals(second.tables["agri.farm"])


class TestConfigExposure:
    def test_exposes_the_config_it_used(self, seeder: DatasetSeeder) -> None:
        assert seeder.config.n_farms == SMALL.n_farms

    def test_seed_result_is_comparable(self) -> None:
        assert SeedResult({"farm": 1}) == SeedResult({"farm": 1})
