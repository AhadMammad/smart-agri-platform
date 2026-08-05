"""Synthetic farm and telemetry generator.

Dimensions are produced as pydantic models — there are few of them and per-record
validation is cheap and useful. Readings are produced straight into a Polars
DataFrame, because at the `large` profile there are tens of millions of them and
building a model per row would dominate runtime. Their correctness is the job of
the pandera contracts instead.
"""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from random import Random
from typing import TYPE_CHECKING

import polars as pl

from smart_agri.domain import Farm, Field_, Sensor, SensorReading, SensorStatus
from smart_agri.generator.config import GeneratorConfig
from smart_agri.generator.regions import ALL_REGIONS, Region
from smart_agri.generator.signals import (
    SoilProfile,
    battery_pct,
    soil_ec_ds_m,
    soil_moisture_pct,
    soil_ph,
    soil_temperature_c,
)
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = get_logger(__name__)

_SENSOR_MODELS: tuple[str, ...] = (
    "TerraProbe TP-200",
    "TerraProbe TP-400",
    "AgriSense AS-12",
    "AgriSense AS-30",
    "SoilWatch SW-7",
)

_FIELD_NAME_PARTS: tuple[str, ...] = (
    "North",
    "South",
    "East",
    "West",
    "Upper",
    "Lower",
    "River",
    "Hill",
    "Valley",
    "Border",
)

# A solar-assisted unit completes a drain/recharge cycle roughly weekly.
_BATTERY_CYCLE_HOURS = 24 * 7


class DatasetGenerator:
    """Produces a coherent farm dataset from a configuration and a seed.

    Every draw comes from a single seeded `Random`, so the same config yields
    the same dataset on every machine and in every process.
    """

    def __init__(
        self, config: GeneratorConfig | None = None, regions: Sequence[Region] | None = None
    ) -> None:
        self.config = config or GeneratorConfig()
        self.regions = tuple(regions) if regions else ALL_REGIONS
        if not self.regions:
            msg = "at least one region is required"
            raise ValueError(msg)
        self._rng = Random(self.config.seed)

    # -- dimensions ----------------------------------------------------------
    def farms(self) -> list[Farm]:
        """One farm per slot, spread round-robin across the configured regions.

        Round-robin rather than random so that a small run still touches both
        North and West Africa instead of landing five farms in one country.
        """
        result: list[Farm] = []
        for index in range(self.config.n_farms):
            region = self.regions[index % len(self.regions)]
            sequence = index // len(self.regions) + 1
            result.append(
                Farm(
                    farm_code=f"{region.country_code}-{sequence:03d}",
                    name=f"{region.name} Farm {sequence}",
                    country_code=region.country_code,
                    region=region.name,
                    latitude=round(self._rng.uniform(region.lat_min, region.lat_max), 6),
                    longitude=round(self._rng.uniform(region.lon_min, region.lon_max), 6),
                    area_ha=round(self._rng.uniform(40, 600), 2),  # type: ignore[arg-type]
                )
            )
        return result

    def fields(self, farms: Sequence[Farm]) -> list[Field_]:
        result: list[Field_] = []
        for farm in farms:
            region = self._region_for(farm)
            for n in range(1, self.config.fields_per_farm + 1):
                part = self._rng.choice(_FIELD_NAME_PARTS)
                result.append(
                    Field_(
                        field_code=f"{farm.farm_code}-F{n:02d}",
                        farm_code=farm.farm_code,
                        name=f"{part} Block {n}",
                        area_ha=round(float(farm.area_ha) / self.config.fields_per_farm * 0.9, 2),  # type: ignore[arg-type]
                        soil_type=self._rng.choice(region.soil_types),
                    )
                )
        return result

    def sensors(self, fields: Sequence[Field_]) -> list[Sensor]:
        """Probes per field, a minority of them not healthy.

        The install date is backdated before the reading window so no sensor
        appears to have produced readings before it existed.
        """
        install_floor = self.config.start_date - timedelta(days=400)
        install_ceiling = self.config.start_date - timedelta(days=1)
        install_span = (install_ceiling - install_floor).days

        result: list[Sensor] = []
        for field in fields:
            for n in range(1, self.config.sensors_per_field + 1):
                result.append(
                    Sensor(
                        sensor_code=f"{field.field_code}-S{n:02d}",
                        field_code=field.field_code,
                        sensor_type="soil_probe",
                        model=self._rng.choice(_SENSOR_MODELS),
                        depth_cm=self._rng.choice([10, 20, 30, 45, 60]),
                        installed_on=install_floor
                        + timedelta(days=self._rng.randint(0, install_span)),
                        status=self._draw_status(),
                    )
                )
        return result

    # -- facts ---------------------------------------------------------------
    def readings(
        self, sensors: Sequence[Sensor], fields: Sequence[Field_], farms: Sequence[Farm]
    ) -> pl.DataFrame:
        """Telemetry for every sensor across the configured window.

        Decommissioned sensors emit nothing — they are the reason the Silver
        layer must join to the sensor dimension rather than trust the fact table
        alone.
        """
        field_by_code = {f.field_code: f for f in fields}
        region_by_farm_code = {farm.farm_code: self._region_for(farm) for farm in farms}

        interval = timedelta(minutes=self.config.reading_interval_minutes)
        readings_per_cycle = max(
            1, _BATTERY_CYCLE_HOURS * 60 // self.config.reading_interval_minutes
        )
        start = datetime.combine(self.config.start_date, time.min, tzinfo=UTC)
        n_steps = self.config.readings_per_sensor

        rows: list[dict[str, object]] = []
        for sensor in sensors:
            if sensor.status is SensorStatus.DECOMMISSIONED:
                continue

            field = field_by_code[sensor.field_code]
            region = region_by_farm_code[field.farm_code]
            profile = SoilProfile.draw(self._rng, field.soil_type)

            for step in range(n_steps):
                moment = start + step * interval
                moisture = soil_moisture_pct(profile, region, moment, self._rng)
                rows.append(
                    {
                        "sensor_code": sensor.sensor_code,
                        "reading_ts": moment,
                        "soil_moisture_pct": self._maybe_drop(moisture),
                        "soil_temperature_c": self._maybe_drop(
                            soil_temperature_c(profile, region, moment, self._rng)
                        ),
                        "soil_ph": self._maybe_drop(soil_ph(profile, self._rng)),
                        "soil_ec_ds_m": self._maybe_drop(
                            soil_ec_ds_m(moisture, profile, self._rng)
                        ),
                        "battery_pct": self._maybe_drop(
                            battery_pct(step, readings_per_cycle, self._rng)
                        ),
                    }
                )

        logger.info(
            "readings_generated",
            sensors=len(sensors),
            rows=len(rows),
            start=self.config.start_date.isoformat(),
            end=self.config.end_date.isoformat(),
        )
        return pl.DataFrame(rows, schema=READING_SCHEMA)

    # -- helpers -------------------------------------------------------------
    def _region_for(self, farm: Farm) -> Region:
        for region in self.regions:
            if region.name == farm.region:
                return region
        msg = f"no region named {farm.region!r} among the configured regions"
        raise LookupError(msg)

    def _draw_status(self) -> SensorStatus:
        roll = self._rng.random()
        if roll < self.config.decommissioned_sensor_ratio:
            return SensorStatus.DECOMMISSIONED
        if roll < self.config.decommissioned_sensor_ratio + self.config.faulty_sensor_ratio:
            return SensorStatus.FAULTY
        return SensorStatus.ACTIVE

    def _maybe_drop(self, value: float) -> float | None:
        """Occasionally null a channel, the way a real probe does."""
        if self._rng.random() < self.config.missing_channel_ratio:
            return None
        return value


READING_SCHEMA: dict[str, pl.DataType] = {
    "sensor_code": pl.String(),
    "reading_ts": pl.Datetime(time_unit="us", time_zone="UTC"),
    "soil_moisture_pct": pl.Float64(),
    "soil_temperature_c": pl.Float64(),
    "soil_ph": pl.Float64(),
    "soil_ec_ds_m": pl.Float64(),
    "battery_pct": pl.Float64(),
}


def sample_reading_model(row: dict[str, object]) -> SensorReading:
    """Validate one generated row through the domain model.

    Used by the generator's tests to assert that the bulk DataFrame path still
    satisfies the same rules the pydantic model encodes.
    """
    return SensorReading.model_validate(row)
