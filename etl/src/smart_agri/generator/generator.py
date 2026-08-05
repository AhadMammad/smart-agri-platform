"""Synthetic farm and telemetry generator.

Dimensions and operational records are produced as pydantic models — there are
comparatively few of them and per-record validation is cheap and useful. The two
high-volume series, sensor readings and machine telemetry, are produced straight
into Polars DataFrames, because at the `large` profile there are tens of
millions of rows and building a model per row would dominate runtime. Their
correctness is the job of the pandera contracts instead.

`build()` runs the sub-generators in dependency order, which is what makes the
data internally consistent: a planting fixes the crop and the calendar, and
irrigation, machine work, imagery and yield all follow from it.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import UTC, datetime, time, timedelta
from random import Random
from typing import TYPE_CHECKING

import polars as pl

from smart_agri.domain import Farm, Field_, Sensor, SensorReading, SensorStatus
from smart_agri.generator.agronomy import VARIETIES, Variety
from smart_agri.generator.config import GeneratorConfig
from smart_agri.generator.imagery import ImageryGenerator
from smart_agri.generator.machinery import (
    TELEMETRY_SCHEMA,
    MachineryGenerator,
    OperationRecord,
)
from smart_agri.generator.operations import OperationsGenerator, PlantingPlan
from smart_agri.generator.regions import ALL_REGIONS, Region
from smart_agri.generator.signals import (
    IRRIGATION_DECAY_DAYS,
    SoilProfile,
    battery_pct,
    irrigation_boost_pct,
    soil_ec_ds_m,
    soil_moisture_pct,
    soil_ph,
    soil_temperature_c,
)
from smart_agri.utils import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from smart_agri.domain import (
        FieldCost,
        FieldIndexObservation,
        Harvest,
        InputApplication,
        IrrigationEvent,
        Machine,
        MachineFault,
    )

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

READING_SCHEMA: dict[str, pl.DataType] = {
    "sensor_code": pl.String(),
    "reading_ts": pl.Datetime(time_unit="us", time_zone="UTC"),
    "soil_moisture_pct": pl.Float64(),
    "soil_temperature_c": pl.Float64(),
    "soil_ph": pl.Float64(),
    "soil_ec_ds_m": pl.Float64(),
    "battery_pct": pl.Float64(),
}


@dataclass(slots=True)
class Dataset:
    """Everything one generator run produces, ready for the seeder."""

    varieties: tuple[Variety, ...] = VARIETIES
    farms: list[Farm] = dataclass_field(default_factory=list)
    fields: list[Field_] = dataclass_field(default_factory=list)
    sensors: list[Sensor] = dataclass_field(default_factory=list)
    plans: list[PlantingPlan] = dataclass_field(default_factory=list)
    irrigation: list[IrrigationEvent] = dataclass_field(default_factory=list)
    inputs: list[InputApplication] = dataclass_field(default_factory=list)
    harvests: list[Harvest] = dataclass_field(default_factory=list)
    costs: list[FieldCost] = dataclass_field(default_factory=list)
    machines: list[Machine] = dataclass_field(default_factory=list)
    operations: list[OperationRecord] = dataclass_field(default_factory=list)
    faults: list[MachineFault] = dataclass_field(default_factory=list)
    observations: list[FieldIndexObservation] = dataclass_field(default_factory=list)
    readings: pl.DataFrame = dataclass_field(default_factory=pl.DataFrame)
    telemetry: pl.DataFrame = dataclass_field(default_factory=pl.DataFrame)

    def row_counts(self) -> dict[str, int]:
        """Rows per target table, for logging and for the seeder's report."""
        return {
            "crop_variety": len(self.varieties),
            "farm": len(self.farms),
            "field": len(self.fields),
            "sensor": len(self.sensors),
            "planting": len(self.plans),
            "irrigation_event": len(self.irrigation),
            "input_application": len(self.inputs),
            "harvest": len(self.harvests),
            "field_cost": len(self.costs),
            "machine": len(self.machines),
            "machine_operation": len(self.operations),
            "machine_fault": len(self.faults),
            "field_index_observation": len(self.observations),
            "sensor_reading": self.readings.height,
            "machine_telemetry": self.telemetry.height,
        }


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

        self._operations = OperationsGenerator(self.config, self._rng)
        self._machinery = MachineryGenerator(self.config, self._rng)
        self._imagery = ImageryGenerator(self.config, self._rng)

    # -- orchestration -------------------------------------------------------
    def build(self) -> Dataset:
        """Generate the complete dataset in dependency order."""
        dataset = Dataset()

        dataset.farms = self.farms()
        dataset.fields = self.fields(dataset.farms)
        dataset.sensors = self.sensors(dataset.fields)

        regions_by_farm = {farm.farm_code: self._region_for(farm) for farm in dataset.farms}
        dataset.plans = self._operations.plantings(dataset.fields, dataset.farms, regions_by_farm)

        for plan in dataset.plans:
            dataset.irrigation.extend(self._operations.irrigate(plan))
            dataset.inputs.extend(self._operations.apply_inputs(plan))

        dataset.machines = self._machinery.machines(dataset.farms)
        dataset.operations = self._machinery.operations(dataset.plans, dataset.machines)
        dataset.faults = self._machinery.faults(dataset.machines)

        # Harvest depends on the water and the heat the crop actually received,
        # so it has to follow irrigation rather than be drawn alongside it.
        for plan in dataset.plans:
            harvest = self._operations.harvest(plan)
            if harvest is not None:
                dataset.harvests.append(harvest)

        machinery_cost = self._machinery_cost_by_planting(dataset.operations)
        for plan in dataset.plans:
            dataset.costs.extend(self._operations.costs(plan, machinery_cost.get(plan.key, 0.0)))

        dataset.observations = self._imagery.observations(dataset.fields, dataset.plans)

        dataset.readings = self.readings(
            dataset.sensors, dataset.fields, dataset.farms, dataset.irrigation
        )
        dataset.telemetry = self._machinery.telemetry(
            dataset.machines,
            dataset.operations,
            {f.farm_code: (f.latitude, f.longitude) for f in dataset.farms},
        )

        logger.info("dataset_built", **dataset.row_counts())
        return dataset

    @staticmethod
    def _machinery_cost_by_planting(
        operations: Sequence[OperationRecord],
    ) -> dict[str, float]:
        """Fuel spend per planting, at a nominal diesel price."""
        diesel_usd_per_litre = 1.05
        totals: dict[str, float] = {}
        for record in operations:
            key = record.operation.planting_key
            if key is None:
                continue
            totals[key] = (
                totals.get(key, 0.0) + record.operation.fuel_used_litres * diesel_usd_per_litre
            )
        return totals

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
        self,
        sensors: Sequence[Sensor],
        fields: Sequence[Field_],
        farms: Sequence[Farm],
        irrigation: Sequence[IrrigationEvent] = (),
    ) -> pl.DataFrame:
        """Telemetry for every sensor across the configured window.

        Decommissioned sensors emit nothing — they are the reason the Silver
        layer must join to the sensor dimension rather than trust the fact table
        alone.

        Irrigation applied to a sensor's field raises its moisture reading and
        then decays, so watering is visible in the series instead of sitting in
        an unrelated table.
        """
        field_by_code = {f.field_code: f for f in fields}
        region_by_farm_code = {farm.farm_code: self._region_for(farm) for farm in farms}
        irrigation_by_field = self._irrigation_index(irrigation)

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

            # Rolling window over the field's irrigation schedule. Readings are
            # produced in chronological order, so each event is examined once
            # rather than the whole schedule being rescanned per reading.
            pending = deque(irrigation_by_field.get(sensor.field_code, []))
            active: deque[tuple[datetime, float]] = deque()

            for step in range(n_steps):
                moment = start + step * interval

                while pending and pending[0][0] <= moment:
                    active.append(pending.popleft())
                horizon = moment - timedelta(days=IRRIGATION_DECAY_DAYS)
                while active and active[0][0] < horizon:
                    active.popleft()

                boost = irrigation_boost_pct(active, moment) if active else 0.0
                moisture = soil_moisture_pct(profile, region, moment, self._rng, boost)

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
    @staticmethod
    def _irrigation_index(
        events: Sequence[IrrigationEvent],
    ) -> dict[str, list[tuple[datetime, float]]]:
        """Irrigation timestamps and depths per field, in chronological order."""
        index: dict[str, list[tuple[datetime, float]]] = {}
        for event in events:
            index.setdefault(event.field_code, []).append((event.event_ts, event.depth_mm))
        for schedule in index.values():
            schedule.sort(key=lambda item: item[0])
        return index

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


def sample_reading_model(row: dict[str, object]) -> SensorReading:
    """Validate one generated row through the domain model.

    Used by the generator's tests to assert that the bulk DataFrame path still
    satisfies the same rules the pydantic model encodes.
    """
    return SensorReading.model_validate(row)


__all__ = [
    "READING_SCHEMA",
    "TELEMETRY_SCHEMA",
    "Dataset",
    "DatasetGenerator",
    "sample_reading_model",
]
