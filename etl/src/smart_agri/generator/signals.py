"""Physical signal models for soil telemetry.

The point of modelling rather than randomising: the dashboards built in later
phases are only meaningful if the data has the structure they are meant to
reveal. Soil moisture must fall through a dry season and jump after rain,
temperature must swing daily and seasonally, and conductivity must track
moisture. Pure noise would render every chart flat and every correlation zero.

Everything here is deterministic given an RNG — no wall-clock, no global state.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from smart_agri.domain import SoilType
from smart_agri.generator.regions import RainfallPattern

if TYPE_CHECKING:
    from datetime import datetime
    from random import Random

    from smart_agri.generator.regions import Region

# Water-holding capacity by soil class, as a plausible volumetric moisture band.
# Sand drains fast and sits low; clay holds water and sits high.
_MOISTURE_BAND: dict[SoilType, tuple[float, float]] = {
    SoilType.SANDY: (6.0, 22.0),
    SoilType.SANDY_LOAM: (10.0, 28.0),
    SoilType.LOAMY: (14.0, 34.0),
    SoilType.SILT: (16.0, 36.0),
    SoilType.CLAY_LOAM: (18.0, 40.0),
    SoilType.CLAY: (22.0, 45.0),
}

# Hour of day at which soil temperature peaks and soil moisture bottoms out.
# Soil at probe depth lags the air, so this sits after solar noon.
PEAK_HEAT_HOUR = 15.0
DIURNAL_MOISTURE_SWING_PCT = 1.2
DIURNAL_TEMPERATURE_SWING_C = 2.5

_PH_BAND: dict[SoilType, tuple[float, float]] = {
    SoilType.SANDY: (5.2, 6.4),
    SoilType.SANDY_LOAM: (5.6, 6.8),
    SoilType.LOAMY: (6.0, 7.2),
    SoilType.SILT: (6.2, 7.4),
    SoilType.CLAY_LOAM: (6.4, 7.6),
    SoilType.CLAY: (6.8, 8.0),
}


def _day_of_year_fraction(moment: datetime) -> float:
    """Position in the year as a 0..1 fraction."""
    return (moment.timetuple().tm_yday - 1) / 365.0


def _hour_fraction(moment: datetime) -> float:
    """Position in the day as a 0..1 fraction."""
    return (moment.hour + moment.minute / 60.0) / 24.0


def wetness_index(region: Region, moment: datetime) -> float:
    """Seasonal water availability in 0..1, before soil and noise are applied.

    This is the shape that makes a moisture chart legible: a Sahelian farm dries
    out for months and then recovers, whereas an irrigated Nile Delta farm stays
    roughly flat all year.
    """
    year_fraction = _day_of_year_fraction(moment)
    # Northern hemisphere throughout, so a peak in the calendar year is used
    # directly rather than being offset.
    match region.rainfall:
        case RainfallPattern.MEDITERRANEAN:
            # Wettest in January, driest in July.
            return 0.5 + 0.5 * math.cos(2 * math.pi * year_fraction)
        case RainfallPattern.UNIMODAL:
            # Single peak in August; a long dry season either side.
            phase = 2 * math.pi * (year_fraction - 0.60)
            return float(max(0.0, math.cos(phase)) ** 1.5)
        case RainfallPattern.BIMODAL:
            # Peaks around May and October.
            first = max(0.0, math.cos(2 * math.pi * (year_fraction - 0.37)))
            second = max(0.0, math.cos(2 * math.pi * (year_fraction - 0.79)))
            return min(1.0, first + 0.75 * second)
        case RainfallPattern.IRRIGATED_ARID:
            # Irrigation, not rain: high and nearly seasonless by design.
            return 0.72 + 0.08 * math.sin(2 * math.pi * year_fraction)


@dataclass(frozen=True, slots=True)
class SoilProfile:
    """The fixed characteristics of one sensor's location.

    Drawn once per sensor so that its readings form a coherent series rather
    than an independent draw at every timestamp.
    """

    soil_type: SoilType
    base_ph: float
    moisture_offset: float
    """Per-sensor bias, so two probes in one field do not read identically."""

    temperature_offset: float

    @classmethod
    def draw(cls, rng: Random, soil_type: SoilType) -> SoilProfile:
        ph_low, ph_high = _PH_BAND[soil_type]
        return cls(
            soil_type=soil_type,
            base_ph=rng.uniform(ph_low, ph_high),
            moisture_offset=rng.uniform(-2.5, 2.5),
            temperature_offset=rng.uniform(-1.5, 1.5),
        )


def soil_moisture_pct(profile: SoilProfile, region: Region, moment: datetime, rng: Random) -> float:
    """Volumetric soil moisture as a percentage.

    Composed of the seasonal wetness curve scaled into the soil's holding band,
    a mild diurnal dip from daytime evapotranspiration, the sensor's own bias,
    and noise.
    """
    low, high = _MOISTURE_BAND[profile.soil_type]
    seasonal = low + (high - low) * wetness_index(region, moment)

    # Evapotranspiration peaks mid-afternoon, so moisture bottoms out then and
    # recovers overnight. Phrased as a cosine trough anchored on the peak hour,
    # which is unambiguous — a sine with an offset is easy to invert by accident.
    diurnal = -DIURNAL_MOISTURE_SWING_PCT * math.cos(
        2 * math.pi * (_hour_fraction(moment) - PEAK_HEAT_HOUR / 24.0)
    )

    value = seasonal + diurnal + profile.moisture_offset + rng.gauss(0.0, 0.7)
    return round(min(100.0, max(0.0, value)), 2)


def soil_temperature_c(
    profile: SoilProfile, region: Region, moment: datetime, rng: Random
) -> float:
    """Soil temperature at probe depth.

    Warmer near the equator and damped relative to air temperature, because soil
    at depth lags and flattens the daily swing.
    """
    latitude_midpoint = (region.lat_min + region.lat_max) / 2
    annual_mean = 30.0 - 0.35 * abs(latitude_midpoint)
    seasonal_amplitude = 3.0 + 0.25 * abs(latitude_midpoint)

    seasonal = seasonal_amplitude * math.sin(2 * math.pi * (_day_of_year_fraction(moment) - 0.30))
    # Soil at depth lags air temperature, peaking in the mid-afternoon alongside
    # the moisture trough rather than at solar noon.
    diurnal = DIURNAL_TEMPERATURE_SWING_C * math.cos(
        2 * math.pi * (_hour_fraction(moment) - PEAK_HEAT_HOUR / 24.0)
    )

    value = annual_mean + seasonal + diurnal + profile.temperature_offset + rng.gauss(0.0, 0.4)
    return round(min(70.0, max(-20.0, value)), 2)


def soil_ph(profile: SoilProfile, rng: Random) -> float:
    """Soil pH — near-constant per location, with small measurement noise."""
    return round(min(14.0, max(0.0, profile.base_ph + rng.gauss(0.0, 0.05))), 2)


def soil_ec_ds_m(moisture_pct: float, profile: SoilProfile, rng: Random) -> float:
    """Electrical conductivity, a proxy for salinity and nutrient load.

    Deliberately correlated with moisture: conductivity rises with water content
    because ions need water to move. A dashboard that plots the two together
    should show that relationship.
    """
    clay_factor = 1.35 if profile.soil_type in (SoilType.CLAY, SoilType.CLAY_LOAM) else 1.0
    value = 0.15 + 0.022 * moisture_pct * clay_factor + rng.gauss(0.0, 0.03)
    return round(min(20.0, max(0.0, value)), 3)


def battery_pct(elapsed_readings: int, readings_per_cycle: int, rng: Random) -> float:
    """Battery level over a solar recharge cycle.

    Sawtooth rather than monotonic decay: field units are solar-assisted, so the
    level drains and recovers. This is what makes a maintenance-due signal in a
    later phase non-trivial.
    """
    position = (elapsed_readings % readings_per_cycle) / readings_per_cycle
    value = 100.0 - 45.0 * position + rng.gauss(0.0, 0.8)
    return round(min(100.0, max(0.0, value)), 2)
