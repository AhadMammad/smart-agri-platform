"""Domain models for farm operations, machinery and imagery.

As in `models.py`, these describe individual records and are used where the
generator constructs them one at a time. Bulk DataFrame validation remains the
job of the pandera contracts.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _Record(BaseModel):
    """Shared configuration for every operational model."""

    model_config = ConfigDict(frozen=True, extra="forbid", str_strip_whitespace=True)


def _require_utc(value: datetime) -> datetime:
    """Reject naive timestamps.

    A naive value here silently becomes a wrong one downstream, where the lake
    partitions and ClickHouse both assume UTC.
    """
    if value.tzinfo is None:
        msg = "timestamp must be timezone-aware"
        raise ValueError(msg)
    return value


# --- enumerations ------------------------------------------------------------
class PlantingStatus(StrEnum):
    GROWING = "growing"
    HARVESTED = "harvested"
    FAILED = "failed"
    TERMINATED = "terminated"


class IrrigationMethod(StrEnum):
    DRIP = "drip"
    SPRINKLER = "sprinkler"
    FURROW = "furrow"
    FLOOD = "flood"
    PIVOT = "pivot"


class WaterSource(StrEnum):
    CANAL = "canal"
    BOREHOLE = "borehole"
    RIVER = "river"
    RESERVOIR = "reservoir"
    RAINWATER = "rainwater"


class InputType(StrEnum):
    FERTILIZER = "fertilizer"
    PESTICIDE = "pesticide"
    HERBICIDE = "herbicide"
    FUNGICIDE = "fungicide"
    LIME = "lime"


class CostCategory(StrEnum):
    SEED = "seed"
    FERTILIZER = "fertilizer"
    CROP_PROTECTION = "crop_protection"
    IRRIGATION = "irrigation"
    FUEL = "fuel"
    LABOUR = "labour"
    MACHINERY = "machinery"
    OTHER = "other"


class MachineType(StrEnum):
    TRACTOR = "tractor"
    COMBINE_HARVESTER = "combine_harvester"
    SPRAYER = "sprayer"
    PLANTER = "planter"
    TILLAGE = "tillage"
    IRRIGATION_PUMP = "irrigation_pump"
    TELEHANDLER = "telehandler"


class MachineStatus(StrEnum):
    ACTIVE = "active"
    MAINTENANCE = "maintenance"
    RETIRED = "retired"


class OperationType(StrEnum):
    TILLAGE = "tillage"
    PLANTING = "planting"
    SPRAYING = "spraying"
    FERTILISING = "fertilising"
    HARVESTING = "harvesting"
    TRANSPORT = "transport"
    MOWING = "mowing"


class FaultSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class ImagerySource(StrEnum):
    SATELLITE = "satellite"
    DRONE = "drone"
    AERIAL = "aerial"


class QualityGrade(StrEnum):
    A = "A"
    B = "B"
    C = "C"
    REJECT = "reject"


# --- operations --------------------------------------------------------------
class Planting(_Record):
    """One crop cycle on one field."""

    field_code: str
    variety_code: str
    season: str = Field(pattern=r"^\d{4}-(main|second|dry)$")
    planted_on: date
    expected_harvest_on: date
    area_ha: float = Field(gt=0)
    seed_rate_kg_ha: float = Field(gt=0)
    status: PlantingStatus = PlantingStatus.GROWING

    @model_validator(mode="after")
    def _harvest_follows_planting(self) -> Planting:
        if self.expected_harvest_on <= self.planted_on:
            msg = "expected_harvest_on must be after planted_on"
            raise ValueError(msg)
        return self


class IrrigationEvent(_Record):
    """A single application of water to a field."""

    field_code: str
    planting_key: str | None = None
    event_ts: datetime
    method: IrrigationMethod
    water_volume_m3: float = Field(gt=0)
    depth_mm: float = Field(gt=0)
    duration_minutes: int = Field(gt=0)
    energy_kwh: float = Field(ge=0)
    water_source: WaterSource

    _check_ts = field_validator("event_ts")(_require_utc)


class InputApplication(_Record):
    """Fertiliser, pesticide or herbicide applied to a field."""

    field_code: str
    planting_key: str | None = None
    applied_on: date
    input_type: InputType
    product_name: str = Field(min_length=1, max_length=80)
    rate_kg_ha: float = Field(gt=0)
    quantity_kg: float = Field(gt=0)
    unit_cost_usd_per_kg: float = Field(ge=0)
    total_cost_usd: float = Field(ge=0)
    application_method: str = Field(min_length=1, max_length=30)


class Harvest(_Record):
    """What came off a planting."""

    planting_key: str
    field_code: str
    harvested_on: date
    area_harvested_ha: float = Field(gt=0)
    yield_tonnes: float = Field(ge=0)
    yield_t_ha: float = Field(ge=0)
    moisture_pct: float | None = Field(default=None, ge=0, le=100)
    quality_grade: QualityGrade
    price_usd_per_tonne: float = Field(ge=0)
    revenue_usd: float = Field(ge=0)


class FieldCost(_Record):
    """A cost booked against a field."""

    field_code: str
    planting_key: str | None = None
    cost_date: date
    cost_category: CostCategory
    amount_usd: float = Field(ge=0)
    description: str = Field(max_length=200)


# --- machinery ---------------------------------------------------------------
class Machine(_Record):
    """A tractor, harvester or implement."""

    machine_code: str
    farm_code: str
    machine_type: MachineType
    manufacturer: str = Field(min_length=1, max_length=60)
    model: str = Field(min_length=1, max_length=60)
    year_built: int = Field(ge=1980, le=2100)
    rated_power_hp: int = Field(gt=0, le=1000)
    purchase_date: date
    engine_hours_at_purchase: float = Field(ge=0)
    status: MachineStatus = MachineStatus.ACTIVE


class MachineOperation(_Record):
    """A machine working a field for a bounded period."""

    machine_code: str
    field_code: str
    planting_key: str | None = None
    operation_type: OperationType
    started_at: datetime
    finished_at: datetime
    area_covered_ha: float = Field(gt=0)
    fuel_used_litres: float = Field(ge=0)
    distance_km: float = Field(ge=0)
    operator_name: str = Field(min_length=1, max_length=80)

    _check_start = field_validator("started_at")(_require_utc)
    _check_finish = field_validator("finished_at")(_require_utc)

    @model_validator(mode="after")
    def _window_is_forward(self) -> MachineOperation:
        if self.finished_at <= self.started_at:
            msg = "finished_at must be after started_at"
            raise ValueError(msg)
        return self


class MachineFault(_Record):
    """A diagnostic trouble code raised by a machine."""

    machine_code: str
    occurred_at: datetime
    fault_code: str = Field(pattern=r"^[A-Z]{2,4}-\d{3,4}$")
    severity: FaultSeverity
    description: str = Field(min_length=1, max_length=200)
    resolved_at: datetime | None = None
    downtime_hours: float | None = Field(default=None, ge=0)
    repair_cost_usd: float | None = Field(default=None, ge=0)

    _check_occurred = field_validator("occurred_at")(_require_utc)

    @model_validator(mode="after")
    def _resolution_follows_occurrence(self) -> MachineFault:
        if self.resolved_at is not None and self.resolved_at < self.occurred_at:
            msg = "resolved_at must not precede occurred_at"
            raise ValueError(msg)
        return self


# --- imagery -----------------------------------------------------------------
class FieldIndexObservation(_Record):
    """One imagery pass over one field.

    Every index is optional: a cloudy satellite pass produces an observation
    record with no usable values, and the pipeline has to cope with that rather
    than pretend the pass never happened.
    """

    field_code: str
    planting_key: str | None = None
    observed_on: date
    source: ImagerySource
    platform: str = Field(min_length=1, max_length=40)
    ndvi: float | None = Field(default=None, ge=-1, le=1)
    ndwi: float | None = Field(default=None, ge=-1, le=1)
    evi: float | None = Field(default=None, ge=-1, le=1)
    cloud_cover_pct: float | None = Field(default=None, ge=0, le=100)
    resolution_m: float = Field(gt=0)
    usable: bool = True

    @model_validator(mode="after")
    def _unusable_passes_carry_no_index(self) -> FieldIndexObservation:
        if not self.usable and any(value is not None for value in (self.ndvi, self.ndwi, self.evi)):
            msg = "an unusable observation must not carry index values"
            raise ValueError(msg)
        return self
