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
    from collections.abc import Sequence
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


def mean_air_temp_c(region: Region, moment: datetime) -> float:
    """Daily mean air temperature for a region.

    Deliberately the same seasonal shape the soil model uses, offset warmer and
    with a larger swing, since air leads soil and varies more. Growing-degree
    days are accumulated from this, which is what makes yield respond to *when*
    a crop was grown rather than being drawn at random.
    """
    latitude_midpoint = abs((region.lat_min + region.lat_max) / 2)
    # Calibrated against the regions actually modelled: roughly 27 °C mean in
    # the Sahel with a small annual swing, and roughly 21 °C on the Maghreb
    # coast with a larger one. An overstated swing starves winter-sown
    # Mediterranean cereals of degree-days and makes them all fail.
    annual_mean = 30.0 - 0.25 * latitude_midpoint
    amplitude = 2.0 + 0.22 * latitude_midpoint
    seasonal = amplitude * math.sin(2 * math.pi * (_day_of_year_fraction(moment) - 0.30))
    return round(annual_mean + seasonal, 2)


def rainfall_share(pattern: RainfallPattern) -> float:
    """Fraction of a crop's water demand that rain typically supplies.

    The complement is what irrigation has to make up, which is what sets the
    irrigation schedule — and therefore what makes water-use efficiency differ
    between an Egyptian and a Ghanaian farm.
    """
    return {
        RainfallPattern.IRRIGATED_ARID: 0.05,
        RainfallPattern.MEDITERRANEAN: 0.55,
        RainfallPattern.UNIMODAL: 0.70,
        RainfallPattern.BIMODAL: 0.85,
    }[pattern]


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


def soil_moisture_pct(
    profile: SoilProfile,
    region: Region,
    moment: datetime,
    rng: Random,
    irrigation_pct: float = 0.0,
) -> float:
    """Volumetric soil moisture as a percentage.

    Composed of the seasonal wetness curve scaled into the soil's holding band,
    a mild diurnal dip from daytime evapotranspiration, whatever recent
    irrigation is still present, the sensor's own bias, and noise.

    Args:
        irrigation_pct: Percentage points contributed by recent watering, from
            `irrigation_boost_pct`. Defaults to zero for a rain-fed field.
    """
    low, high = _MOISTURE_BAND[profile.soil_type]
    seasonal = low + (high - low) * wetness_index(region, moment)

    # Evapotranspiration peaks mid-afternoon, so moisture bottoms out then and
    # recovers overnight. Phrased as a cosine trough anchored on the peak hour,
    # which is unambiguous — a sine with an offset is easy to invert by accident.
    diurnal = -DIURNAL_MOISTURE_SWING_PCT * math.cos(
        2 * math.pi * (_hour_fraction(moment) - PEAK_HEAT_HOUR / 24.0)
    )

    value = seasonal + diurnal + irrigation_pct + profile.moisture_offset + rng.gauss(0.0, 0.7)
    return round(min(100.0, max(0.0, value)), 2)


# How long an irrigation keeps showing in the profile before it drains and
# evaporates away. Beyond this the event is dropped from the rolling window.
IRRIGATION_DECAY_DAYS = 5.0

# Percentage points of volumetric moisture gained per millimetre applied.
# Roughly 1 mm over a 400 mm root zone, allowing for losses.
IRRIGATION_GAIN_PCT_PER_MM = 0.22


def irrigation_boost_pct(
    recent_events: Sequence[tuple[datetime, float]], moment: datetime
) -> float:
    """Moisture still present from recent irrigation, in percentage points.

    Each event decays exponentially, so a chart shows a sharp rise on the day
    water was applied followed by a gradual fall — the pattern that makes
    irrigation events visible in the sensor series at all. Without this
    coupling, watering a field would leave no trace in its soil-moisture data.

    Args:
        recent_events: `(timestamp, depth_mm)` pairs, already filtered to the
            decay window by the caller.
        moment: When the reading is taken.
    """
    total = 0.0
    for event_ts, depth_mm in recent_events:
        elapsed_days = (moment - event_ts).total_seconds() / 86_400.0
        if elapsed_days < 0 or elapsed_days > IRRIGATION_DECAY_DAYS:
            continue
        decay = math.exp(-elapsed_days / (IRRIGATION_DECAY_DAYS / 3.0))
        total += depth_mm * IRRIGATION_GAIN_PCT_PER_MM * decay
    return total


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
