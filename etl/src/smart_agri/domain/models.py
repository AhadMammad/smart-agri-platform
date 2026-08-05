"""Pydantic models for the agricultural entities.

These describe the *business* shape of the data and are used where individual
records are constructed or validated one at a time — chiefly the generator.
Bulk DataFrame validation is the job of the pandera contracts in
`smart_agri.contracts`; the two are intentionally separate concerns.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class SensorStatus(StrEnum):
    """Lifecycle state of a probe. Mirrors the CHECK constraint on agri.sensor."""

    ACTIVE = "active"
    FAULTY = "faulty"
    DECOMMISSIONED = "decommissioned"


class SoilType(StrEnum):
    """Soil classes present in the North and West African regions modelled."""

    SANDY = "sandy"
    LOAMY = "loamy"
    CLAY = "clay"
    SILT = "silt"
    SANDY_LOAM = "sandy_loam"
    CLAY_LOAM = "clay_loam"


class _Entity(BaseModel):
    """Shared configuration for every domain model."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        str_strip_whitespace=True,
        use_enum_values=False,
    )


class Farm(_Entity):
    """A managed agricultural holding.

    Coordinates sit at farm level: weather is fetched once per farm and joined
    down to its fields, which keeps Open-Meteo calls proportional to farms
    rather than fields.
    """

    farm_code: str = Field(pattern=r"^[A-Z]{2}-\d{3}$", description="e.g. NG-001")
    name: str = Field(min_length=1, max_length=120)
    country_code: str = Field(pattern=r"^[A-Z]{2}$")
    region: str = Field(min_length=1, max_length=80)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    area_ha: Decimal = Field(gt=0, decimal_places=2)


class Field_(_Entity):  # noqa: N801 - trailing underscore avoids shadowing pydantic.Field
    """A cultivated parcel within a farm.

    Named with a trailing underscore because `Field` is pydantic's own symbol,
    which this module uses on nearly every attribute; a class called `Field`
    would shadow it here and at every import site.
    """

    field_code: str = Field(pattern=r"^[A-Z]{2}-\d{3}-F\d{2}$", description="e.g. NG-001-F01")
    farm_code: str = Field(pattern=r"^[A-Z]{2}-\d{3}$")
    name: str = Field(min_length=1, max_length=120)
    area_ha: Decimal = Field(gt=0, decimal_places=2)
    soil_type: SoilType


class Sensor(_Entity):
    """An in-field IoT soil probe."""

    sensor_code: str = Field(pattern=r"^[A-Z]{2}-\d{3}-F\d{2}-S\d{2}$")
    field_code: str = Field(pattern=r"^[A-Z]{2}-\d{3}-F\d{2}$")
    sensor_type: str = Field(min_length=1, max_length=30)
    model: str = Field(min_length=1, max_length=60)
    depth_cm: int = Field(ge=5, le=200)
    installed_on: date
    status: SensorStatus = SensorStatus.ACTIVE


class SensorReading(_Entity):
    """One timestamped measurement from a probe.

    Every channel is optional: real probes drop individual readings, and the
    Silver contract has to handle that rather than assume it away.
    """

    sensor_code: str = Field(pattern=r"^[A-Z]{2}-\d{3}-F\d{2}-S\d{2}$")
    reading_ts: datetime
    soil_moisture_pct: float | None = Field(default=None, ge=0, le=100)
    soil_temperature_c: float | None = Field(default=None, ge=-20, le=70)
    soil_ph: float | None = Field(default=None, ge=0, le=14)
    soil_ec_ds_m: float | None = Field(default=None, ge=0, le=20)
    battery_pct: float | None = Field(default=None, ge=0, le=100)

    @field_validator("reading_ts")
    @classmethod
    def _must_be_timezone_aware(cls, value: datetime) -> datetime:
        """A naive timestamp here silently becomes a wrong one downstream, where
        everything — HDFS partitions, ClickHouse, Superset — assumes UTC."""
        if value.tzinfo is None:
            msg = "reading_ts must be timezone-aware"
            raise ValueError(msg)
        return value
