"""Loads a generated dataset into Postgres.

Kept separate from the generator so the generator stays pure and testable
without a database, and so the same dataset could be written somewhere else
without touching generation logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

from smart_agri.generator.generator import DatasetGenerator
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from smart_agri.generator.config import GeneratorConfig
    from smart_agri.io import PostgresSource

logger = get_logger(__name__)

# Child-first, so foreign keys never block the truncate.
_TABLES_IN_DEPENDENCY_ORDER = (
    "agri.sensor_reading",
    "agri.sensor",
    "agri.field",
    "agri.farm",
)


@dataclass(frozen=True, slots=True)
class SeedResult:
    """Row counts written per table."""

    farms: int
    fields: int
    sensors: int
    readings: int

    @property
    def total(self) -> int:
        return self.farms + self.fields + self.sensors + self.readings


class DatasetSeeder:
    """Generates a dataset and writes it to the source database.

    Surrogate keys are assigned by Postgres, so each level is inserted and then
    read back to resolve the natural key to the generated id before the next
    level is written.
    """

    def __init__(self, postgres: PostgresSource, config: GeneratorConfig | None = None) -> None:
        self._postgres = postgres
        self._generator = DatasetGenerator(config)
        self.config = self._generator.config

    def seed(self, *, truncate: bool = True) -> SeedResult:
        """Populate the source database.

        Args:
            truncate: Empty the tables first. Default because re-seeding on top
                of existing data would violate the natural-key uniqueness
                constraints rather than fail cleanly.
        """
        if truncate:
            self._postgres.truncate(_TABLES_IN_DEPENDENCY_ORDER)

        farms = self._generator.farms()
        fields = self._generator.fields(farms)
        sensors = self._generator.sensors(fields)

        n_farms = self._insert_farms(farms)
        farm_ids = self._id_map("agri.farm", "farm_code", "farm_id")

        n_fields = self._insert_fields(fields, farm_ids)
        field_ids = self._id_map("agri.field", "field_code", "field_id")

        n_sensors = self._insert_sensors(sensors, field_ids)
        sensor_ids = self._id_map("agri.sensor", "sensor_code", "sensor_id")

        readings = self._generator.readings(sensors, fields, farms)
        n_readings = self._insert_readings(readings, sensor_ids)

        result = SeedResult(n_farms, n_fields, n_sensors, n_readings)
        logger.info(
            "seed_complete",
            farms=result.farms,
            fields=result.fields,
            sensors=result.sensors,
            readings=result.readings,
        )
        return result

    # -- inserts -------------------------------------------------------------
    def _insert_farms(self, farms: list) -> int:  # type: ignore[type-arg]
        frame = pl.DataFrame(
            [
                {
                    "farm_code": f.farm_code,
                    "name": f.name,
                    "country_code": f.country_code,
                    "region": f.region,
                    "latitude": float(f.latitude),
                    "longitude": float(f.longitude),
                    "area_ha": float(f.area_ha),
                }
                for f in farms
            ]
        )
        return self._postgres.copy_frame(
            frame,
            "agri.farm",
            ["farm_code", "name", "country_code", "region", "latitude", "longitude", "area_ha"],
        )

    def _insert_fields(self, fields: list, farm_ids: dict[str, int]) -> int:  # type: ignore[type-arg]
        frame = pl.DataFrame(
            [
                {
                    "field_code": f.field_code,
                    "farm_id": farm_ids[f.farm_code],
                    "name": f.name,
                    "area_ha": float(f.area_ha),
                    "soil_type": f.soil_type.value,
                }
                for f in fields
            ]
        )
        return self._postgres.copy_frame(
            frame, "agri.field", ["field_code", "farm_id", "name", "area_ha", "soil_type"]
        )

    def _insert_sensors(self, sensors: list, field_ids: dict[str, int]) -> int:  # type: ignore[type-arg]
        frame = pl.DataFrame(
            [
                {
                    "sensor_code": s.sensor_code,
                    "field_id": field_ids[s.field_code],
                    "sensor_type": s.sensor_type,
                    "model": s.model,
                    "depth_cm": s.depth_cm,
                    "installed_on": s.installed_on.isoformat(),
                    "status": s.status.value,
                }
                for s in sensors
            ]
        )
        return self._postgres.copy_frame(
            frame,
            "agri.sensor",
            [
                "sensor_code",
                "field_id",
                "sensor_type",
                "model",
                "depth_cm",
                "installed_on",
                "status",
            ],
        )

    def _insert_readings(self, readings: pl.DataFrame, sensor_ids: dict[str, int]) -> int:
        if readings.is_empty():
            return 0

        resolved = readings.with_columns(
            pl.col("sensor_code")
            .replace_strict(sensor_ids, return_dtype=pl.Int64)
            .alias("sensor_id")
        )
        return self._postgres.copy_frame(
            resolved,
            "agri.sensor_reading",
            [
                "sensor_id",
                "reading_ts",
                "soil_moisture_pct",
                "soil_temperature_c",
                "soil_ph",
                "soil_ec_ds_m",
                "battery_pct",
            ],
        )

    # -- helpers -------------------------------------------------------------
    def _id_map(self, table: str, code_column: str, id_column: str) -> dict[str, int]:
        """Read back the surrogate keys Postgres assigned, keyed by natural key."""
        frame = self._postgres.read_query(f"SELECT {code_column}, {id_column} FROM {table}")
        return dict(zip(frame[code_column].to_list(), frame[id_column].to_list(), strict=True))
