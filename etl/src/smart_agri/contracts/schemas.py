"""Pandera schemas enforced at each lake zone boundary.

The zones have deliberately different strictness:

* **Bronze** checks only that the extract has the right shape. Raw data is kept
  exactly as it arrived, because a Bronze layer that rejects rows destroys the
  evidence needed to debug the source.
* **Silver** is strict. Ranges, nullability and referential expectations are all
  enforced, and rows that fail are diverted to `quarantine/` rather than
  dropped silently.
* **Gold** validates the aggregates so a broken metric fails here rather than
  surfacing as a wrong number on a dashboard.
"""

from __future__ import annotations

from typing import Any, cast

import pandera.polars as pa
import polars as pl
from pandera.engines import polars_engine


def utc_datetime() -> Any:
    """A timezone-aware datetime dtype for a pandera column.

    `pa.Column` accepts a dtype *instance* at runtime, which is the only way to
    pin the timezone, but its annotation admits only classes. The cast is
    confined here rather than repeated at every timestamp column.

    Enforcing UTC is not cosmetic: a naive timestamp reaching Silver would make
    every daily partition and every ClickHouse rollup silently wrong for any
    farm outside UTC.
    """
    return cast("Any", polars_engine.DateTime(time_zone="UTC"))


# --- Bronze ------------------------------------------------------------------
# Only presence and coarse type. `strict=False` so an added source column lands
# in Bronze rather than failing the extract.

BRONZE_FARM = pa.DataFrameSchema(
    {
        "farm_id": pa.Column(polars_engine.Int64),
        "farm_code": pa.Column(polars_engine.String),
        "name": pa.Column(polars_engine.String),
        "country_code": pa.Column(polars_engine.String),
        "region": pa.Column(polars_engine.String),
        "latitude": pa.Column(polars_engine.Float64, nullable=True),
        "longitude": pa.Column(polars_engine.Float64, nullable=True),
        "area_ha": pa.Column(polars_engine.Float64, nullable=True),
    },
    strict=False,
    name="bronze_farm",
)

BRONZE_FIELD = pa.DataFrameSchema(
    {
        "field_id": pa.Column(polars_engine.Int64),
        "field_code": pa.Column(polars_engine.String),
        "farm_id": pa.Column(polars_engine.Int64),
        "name": pa.Column(polars_engine.String),
        "area_ha": pa.Column(polars_engine.Float64, nullable=True),
        "soil_type": pa.Column(polars_engine.String),
    },
    strict=False,
    name="bronze_field",
)

BRONZE_SENSOR = pa.DataFrameSchema(
    {
        "sensor_id": pa.Column(polars_engine.Int64),
        "sensor_code": pa.Column(polars_engine.String),
        "field_id": pa.Column(polars_engine.Int64),
        "sensor_type": pa.Column(polars_engine.String),
        "status": pa.Column(polars_engine.String),
    },
    strict=False,
    name="bronze_sensor",
)

BRONZE_SENSOR_READING = pa.DataFrameSchema(
    {
        "reading_id": pa.Column(polars_engine.Int64),
        "sensor_id": pa.Column(polars_engine.Int64),
        "reading_ts": pa.Column(utc_datetime()),
        "soil_moisture_pct": pa.Column(polars_engine.Float64, nullable=True),
        "soil_temperature_c": pa.Column(polars_engine.Float64, nullable=True),
        "soil_ph": pa.Column(polars_engine.Float64, nullable=True),
        "soil_ec_ds_m": pa.Column(polars_engine.Float64, nullable=True),
        "battery_pct": pa.Column(polars_engine.Float64, nullable=True),
        "updated_at": pa.Column(utc_datetime()),
    },
    strict=False,
    name="bronze_sensor_reading",
)


# --- Silver ------------------------------------------------------------------
# Strict: exact column set, real ranges, uniqueness.

SILVER_DIM_FARM = pa.DataFrameSchema(
    {
        "farm_id": pa.Column(polars_engine.Int64, unique=True),
        "farm_code": pa.Column(polars_engine.String, unique=True),
        "farm_name": pa.Column(polars_engine.String),
        "country_code": pa.Column(polars_engine.String, pa.Check.str_length(2, 2)),
        "region": pa.Column(polars_engine.String),
        "latitude": pa.Column(polars_engine.Float64, pa.Check.in_range(-90, 90)),
        "longitude": pa.Column(polars_engine.Float64, pa.Check.in_range(-180, 180)),
        "area_ha": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
    },
    strict=True,
    name="silver_dim_farm",
)

SILVER_DIM_FIELD = pa.DataFrameSchema(
    {
        "field_id": pa.Column(polars_engine.Int64, unique=True),
        "field_code": pa.Column(polars_engine.String, unique=True),
        "farm_id": pa.Column(polars_engine.Int64),
        "farm_code": pa.Column(polars_engine.String),
        "field_name": pa.Column(polars_engine.String),
        "area_ha": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "soil_type": pa.Column(polars_engine.String),
    },
    strict=True,
    name="silver_dim_field",
)

SILVER_DIM_SENSOR = pa.DataFrameSchema(
    {
        "sensor_id": pa.Column(polars_engine.Int64, unique=True),
        "sensor_code": pa.Column(polars_engine.String, unique=True),
        "field_id": pa.Column(polars_engine.Int64),
        "field_code": pa.Column(polars_engine.String),
        "farm_id": pa.Column(polars_engine.Int64),
        "sensor_type": pa.Column(polars_engine.String),
        "model": pa.Column(polars_engine.String),
        "depth_cm": pa.Column(polars_engine.Int32, pa.Check.in_range(5, 200)),
        "installed_on": pa.Column(polars_engine.Date),
        "status": pa.Column(
            polars_engine.String,
            pa.Check.isin(["active", "faulty", "decommissioned"]),
        ),
    },
    strict=True,
    name="silver_dim_sensor",
)

SILVER_FACT_SENSOR_READING = pa.DataFrameSchema(
    {
        "reading_id": pa.Column(polars_engine.Int64, unique=True),
        "sensor_id": pa.Column(polars_engine.Int64),
        "field_id": pa.Column(polars_engine.Int64),
        "farm_id": pa.Column(polars_engine.Int64),
        "reading_ts": pa.Column(utc_datetime()),
        "reading_date": pa.Column(polars_engine.Date),
        # Physical bounds. A probe reporting 150% moisture is broken, not wet,
        # and such a row must reach quarantine rather than an average.
        "soil_moisture_pct": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 100), nullable=True
        ),
        "soil_temperature_c": pa.Column(
            polars_engine.Float64, pa.Check.in_range(-20, 70), nullable=True
        ),
        "soil_ph": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 14), nullable=True),
        "soil_ec_ds_m": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 20), nullable=True),
        "battery_pct": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 100), nullable=True),
    },
    strict=True,
    name="silver_fact_sensor_reading",
)


# --- Gold --------------------------------------------------------------------

GOLD_FIELD_SOIL_DAILY = pa.DataFrameSchema(
    {
        "reading_date": pa.Column(polars_engine.Date),
        "farm_id": pa.Column(polars_engine.Int64),
        "farm_code": pa.Column(polars_engine.String),
        "farm_name": pa.Column(polars_engine.String),
        "country_code": pa.Column(polars_engine.String),
        "region": pa.Column(polars_engine.String),
        "field_id": pa.Column(polars_engine.Int64),
        "field_code": pa.Column(polars_engine.String),
        "field_name": pa.Column(polars_engine.String),
        "soil_type": pa.Column(polars_engine.String),
        "field_area_ha": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "active_sensors": pa.Column(polars_engine.Int64, pa.Check.gt(0)),
        "reading_count": pa.Column(polars_engine.Int64, pa.Check.gt(0)),
        "avg_soil_moisture_pct": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 100), nullable=True
        ),
        "min_soil_moisture_pct": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 100), nullable=True
        ),
        "max_soil_moisture_pct": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 100), nullable=True
        ),
        "avg_soil_temperature_c": pa.Column(
            polars_engine.Float64, pa.Check.in_range(-20, 70), nullable=True
        ),
        "avg_soil_ph": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 14), nullable=True),
        "avg_soil_ec_ds_m": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 20), nullable=True
        ),
        "min_battery_pct": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 100), nullable=True
        ),
        # The stress flag the dashboard filters on. Not nullable: an unknown
        # stress state is a bug in the aggregate, not a legitimate value.
        "moisture_stress": pa.Column(
            polars_engine.String,
            pa.Check.isin(["dry", "optimal", "wet", "unknown"]),
        ),
    },
    strict=True,
    name="gold_field_soil_daily",
)


SILVER_SCHEMAS: dict[str, pa.DataFrameSchema] = {
    "dim_farm": SILVER_DIM_FARM,
    "dim_field": SILVER_DIM_FIELD,
    "dim_sensor": SILVER_DIM_SENSOR,
    "fact_sensor_reading": SILVER_FACT_SENSOR_READING,
}


def empty_frame_for(schema: pa.DataFrameSchema) -> pl.DataFrame:
    """An empty DataFrame with the schema's columns and dtypes.

    Lets a pipeline return a correctly-shaped result for a date with no data,
    instead of an untyped empty frame that breaks the next stage.
    """
    return pl.DataFrame(schema={name: column.dtype.type for name, column in schema.columns.items()})


# --- Weather (Phase 4) -------------------------------------------------------
# Bounds are physical rather than climatological: -90 °C and +60 °C bracket the
# planetary record, so a value outside them is a broken feed, not a heatwave.

BRONZE_WEATHER = pa.DataFrameSchema(
    {
        "farm_code": pa.Column(polars_engine.String),
        "latitude": pa.Column(polars_engine.Float64),
        "longitude": pa.Column(polars_engine.Float64),
        "weather_date": pa.Column(polars_engine.Date),
        "source": pa.Column(polars_engine.String, pa.Check.isin(["archive", "forecast"])),
        "temperature_2m_mean": pa.Column(polars_engine.Float64, nullable=True),
        "precipitation_sum": pa.Column(polars_engine.Float64, nullable=True),
        "et0_fao_evapotranspiration": pa.Column(polars_engine.Float64, nullable=True),
    },
    strict=False,
    name="bronze_weather",
)

SILVER_FACT_WEATHER_DAILY = pa.DataFrameSchema(
    {
        "farm_id": pa.Column(polars_engine.Int64),
        "farm_code": pa.Column(polars_engine.String),
        "weather_date": pa.Column(polars_engine.Date),
        "source": pa.Column(polars_engine.String, pa.Check.isin(["archive", "forecast"])),
        "is_actual": pa.Column(polars_engine.Bool),
        "latitude": pa.Column(polars_engine.Float64, pa.Check.in_range(-90, 90)),
        "longitude": pa.Column(polars_engine.Float64, pa.Check.in_range(-180, 180)),
        "temp_max_c": pa.Column(polars_engine.Float64, pa.Check.in_range(-90, 60), nullable=True),
        "temp_min_c": pa.Column(polars_engine.Float64, pa.Check.in_range(-90, 60), nullable=True),
        "temp_mean_c": pa.Column(polars_engine.Float64, pa.Check.in_range(-90, 60), nullable=True),
        "precipitation_mm": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 2000), nullable=True
        ),
        "rain_mm": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 2000), nullable=True),
        "precipitation_hours": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 24), nullable=True
        ),
        "et0_mm": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 30), nullable=True),
        "solar_radiation_mj_m2": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 60), nullable=True
        ),
        "wind_speed_max_kmh": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 500), nullable=True
        ),
        "weather_code": pa.Column(polars_engine.Int32, nullable=True),
    },
    strict=True,
    name="silver_fact_weather_daily",
)

GOLD_DIM_DATE = pa.DataFrameSchema(
    {
        "date_key": pa.Column(polars_engine.Date, unique=True),
        "year": pa.Column(polars_engine.Int32),
        "quarter": pa.Column(polars_engine.Int8, pa.Check.in_range(1, 4)),
        "month": pa.Column(polars_engine.Int8, pa.Check.in_range(1, 12)),
        "month_name": pa.Column(polars_engine.String),
        "day_of_month": pa.Column(polars_engine.Int8, pa.Check.in_range(1, 31)),
        "day_of_week": pa.Column(polars_engine.Int8, pa.Check.in_range(1, 7)),
        "day_name": pa.Column(polars_engine.String),
        "iso_week": pa.Column(polars_engine.Int8, pa.Check.in_range(1, 53)),
        "is_weekend": pa.Column(polars_engine.Bool),
    },
    strict=True,
    name="gold_dim_date",
)

GOLD_FIELD_WEATHER_DAILY = pa.DataFrameSchema(
    {
        "weather_date": pa.Column(polars_engine.Date),
        "field_id": pa.Column(polars_engine.Int64),
        "field_code": pa.Column(polars_engine.String),
        "farm_id": pa.Column(polars_engine.Int64),
        "farm_code": pa.Column(polars_engine.String),
        "region": pa.Column(polars_engine.String),
        "country_code": pa.Column(polars_engine.String),
        "field_area_ha": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "is_actual": pa.Column(polars_engine.Bool),
        "temp_max_c": pa.Column(polars_engine.Float64, nullable=True),
        "temp_min_c": pa.Column(polars_engine.Float64, nullable=True),
        "temp_mean_c": pa.Column(polars_engine.Float64, nullable=True),
        "precipitation_mm": pa.Column(polars_engine.Float64, nullable=True),
        "et0_mm": pa.Column(polars_engine.Float64, nullable=True),
        "solar_radiation_mj_m2": pa.Column(polars_engine.Float64, nullable=True),
        # Derived agronomy — the reason Gold exists for weather at all.
        "gdd_base10": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 40), nullable=True),
        "gdd_cumulative": pa.Column(polars_engine.Float64, nullable=True),
        "rainfall_7d_mm": pa.Column(polars_engine.Float64, nullable=True),
        "rainfall_30d_mm": pa.Column(polars_engine.Float64, nullable=True),
        "et0_7d_mm": pa.Column(polars_engine.Float64, nullable=True),
        "water_balance_mm": pa.Column(polars_engine.Float64, nullable=True),
        "water_balance_7d_mm": pa.Column(polars_engine.Float64, nullable=True),
        "aridity_flag": pa.Column(
            polars_engine.String, pa.Check.isin(["wet", "balanced", "dry", "unknown"])
        ),
    },
    strict=True,
    name="gold_field_weather_daily",
)


# --- Bronze, Phase 5 ---------------------------------------------------------
#
# Deliberately thin. Bronze asserts that the extract has the shape the next
# stage expects — primary key, foreign keys, and the columns Silver renames —
# and nothing more. Checking content here would reject rows before anyone could
# see what the source actually sent.


def _bronze(name: str, columns: dict[str, pa.Column]) -> pa.DataFrameSchema:
    """A permissive Bronze schema: named columns must exist, extras are kept."""
    return pa.DataFrameSchema(columns, strict=False, name=name)


_ID = pa.Column(polars_engine.Int64)
_TEXT = pa.Column(polars_engine.String)
_NUM = pa.Column(polars_engine.Float64, nullable=True)
_INT32 = pa.Column(polars_engine.Int32, nullable=True)
_TS = pa.Column(utc_datetime())
_DAY = pa.Column(polars_engine.Date)

# Reference
BRONZE_REGION = _bronze(
    "bronze_region",
    {"region_code": _TEXT, "name": _TEXT, "country_code": _TEXT, "macro_region": _TEXT},
)
BRONZE_SOIL_TYPE = _bronze(
    "bronze_soil_type",
    {"soil_code": _TEXT, "name": _TEXT, "moisture_min_pct": _NUM, "moisture_max_pct": _NUM},
)
BRONZE_CROP = _bronze(
    "bronze_crop",
    {"crop_code": _TEXT, "name": _TEXT, "category": _TEXT, "water_demand_mm": _INT32},
)
BRONZE_CROP_VARIETY = _bronze(
    "bronze_crop_variety",
    {"variety_id": _ID, "variety_code": _TEXT, "crop_code": _TEXT, "name": _TEXT},
)

# Operations
BRONZE_PLANTING = _bronze(
    "bronze_planting",
    {
        "planting_id": _ID,
        "field_id": _ID,
        "variety_id": _ID,
        "season": _TEXT,
        "planted_on": _DAY,
        "status": _TEXT,
    },
)
BRONZE_IRRIGATION_EVENT = _bronze(
    "bronze_irrigation_event",
    {
        "irrigation_id": _ID,
        "field_id": _ID,
        "event_ts": _TS,
        "method": _TEXT,
        "water_volume_m3": _NUM,
        "updated_at": _TS,
    },
)
BRONZE_INPUT_APPLICATION = _bronze(
    "bronze_input_application",
    {
        "application_id": _ID,
        "field_id": _ID,
        "applied_on": _DAY,
        "input_type": _TEXT,
        "updated_at": _TS,
    },
)
BRONZE_HARVEST = _bronze(
    "bronze_harvest",
    {
        "harvest_id": _ID,
        "planting_id": _ID,
        "field_id": _ID,
        "harvested_on": _DAY,
        "yield_tonnes": _NUM,
        "updated_at": _TS,
    },
)
BRONZE_FIELD_COST = _bronze(
    "bronze_field_cost",
    {
        "cost_id": _ID,
        "field_id": _ID,
        "cost_date": _DAY,
        "cost_category": _TEXT,
        "amount_usd": _NUM,
        "updated_at": _TS,
    },
)

# Machinery
BRONZE_MACHINE = _bronze(
    "bronze_machine",
    {"machine_id": _ID, "machine_code": _TEXT, "farm_id": _ID, "machine_type": _TEXT},
)
BRONZE_MACHINE_OPERATION = _bronze(
    "bronze_machine_operation",
    {
        "operation_id": _ID,
        "machine_id": _ID,
        "field_id": _ID,
        "operation_type": _TEXT,
        "started_at": _TS,
        "finished_at": _TS,
        "updated_at": _TS,
    },
)
BRONZE_MACHINE_TELEMETRY = _bronze(
    "bronze_machine_telemetry",
    {"telemetry_id": _ID, "machine_id": _ID, "reading_ts": _TS, "updated_at": _TS},
)
BRONZE_MACHINE_FAULT = _bronze(
    "bronze_machine_fault",
    {
        "fault_id": _ID,
        "machine_id": _ID,
        "occurred_at": _TS,
        "fault_code": _TEXT,
        "severity": _TEXT,
        "updated_at": _TS,
    },
)

# Imagery
BRONZE_FIELD_INDEX_OBSERVATION = _bronze(
    "bronze_field_index_observation",
    {
        "observation_id": _ID,
        "field_id": _ID,
        "observed_on": _DAY,
        "source": _TEXT,
        "updated_at": _TS,
    },
)


# --- Silver, Phase 5 ---------------------------------------------------------
#
# Strict, unlike Bronze: exact column set, real ranges, and the enumerations the
# source CHECK constraints declare. A value outside these is a broken feed, and
# the row goes to quarantine rather than into an average.

_SID = pa.Column(polars_engine.Int64)
_SKEY = pa.Column(polars_engine.Int64, unique=True)
_STEXT = pa.Column(polars_engine.String)
_SDAY = pa.Column(polars_engine.Date)
_SNUM = pa.Column(polars_engine.Float64, nullable=True)


def _money(nullable: bool = True) -> pa.Column:
    """A currency amount. Negative spend is a data error, not a refund."""
    return pa.Column(polars_engine.Float64, pa.Check.ge(0), nullable=nullable)


# Reference
SILVER_DIM_REGION = pa.DataFrameSchema(
    {
        "region_code": pa.Column(polars_engine.String, unique=True),
        "region_name": _STEXT,
        "country_code": pa.Column(polars_engine.String, pa.Check.str_length(2, 2)),
        "country_name": _STEXT,
        "macro_region": _STEXT,
        "rainfall_pattern": _STEXT,
    },
    strict=True,
    name="silver_dim_region",
)

SILVER_DIM_SOIL_TYPE = pa.DataFrameSchema(
    {
        "soil_code": pa.Column(polars_engine.String, unique=True),
        "soil_name": _STEXT,
        "moisture_min_pct": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 100)),
        "moisture_max_pct": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 100)),
        "drainage": _STEXT,
    },
    strict=True,
    name="silver_dim_soil_type",
)

SILVER_DIM_CROP = pa.DataFrameSchema(
    {
        "crop_code": pa.Column(polars_engine.String, unique=True),
        "crop_name": _STEXT,
        "category": _STEXT,
        "is_perennial": pa.Column(polars_engine.Bool),
        "base_temp_c": pa.Column(polars_engine.Float64, pa.Check.in_range(-5, 30)),
        "gdd_to_maturity": pa.Column(polars_engine.Int32, pa.Check.gt(0)),
        "cycle_days": pa.Column(polars_engine.Int32, pa.Check.gt(0)),
        "water_demand_mm": pa.Column(polars_engine.Int32, pa.Check.gt(0)),
        "peak_ndvi": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 1)),
    },
    strict=True,
    name="silver_dim_crop",
)

SILVER_DIM_CROP_VARIETY = pa.DataFrameSchema(
    {
        "variety_id": _SKEY,
        "variety_code": pa.Column(polars_engine.String, unique=True),
        "crop_code": _STEXT,
        "variety_name": _STEXT,
        "maturity_days": pa.Column(polars_engine.Int32, pa.Check.gt(0)),
        "yield_potential_t_ha": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "drought_tolerance": pa.Column(
            polars_engine.String, pa.Check.isin(["low", "moderate", "high"])
        ),
    },
    strict=True,
    name="silver_dim_crop_variety",
)

# Operations
SILVER_DIM_PLANTING = pa.DataFrameSchema(
    {
        "planting_id": _SKEY,
        "field_id": _SID,
        "farm_id": _SID,
        "variety_id": _SID,
        "season": _STEXT,
        "planted_on": _SDAY,
        "expected_harvest_on": _SDAY,
        "area_ha": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "seed_rate_kg_ha": _SNUM,
        "status": pa.Column(
            polars_engine.String,
            pa.Check.isin(["growing", "harvested", "failed", "terminated"]),
        ),
    },
    strict=True,
    name="silver_dim_planting",
)

SILVER_FACT_IRRIGATION = pa.DataFrameSchema(
    {
        "irrigation_id": _SKEY,
        "field_id": _SID,
        "farm_id": _SID,
        "planting_id": pa.Column(polars_engine.Int64, nullable=True),
        "event_ts": pa.Column(utc_datetime()),
        "event_date": _SDAY,
        "method": pa.Column(
            polars_engine.String,
            pa.Check.isin(["drip", "sprinkler", "furrow", "flood", "pivot"]),
        ),
        "water_source": _STEXT,
        "water_volume_m3": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "depth_mm": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 500)),
        "duration_minutes": pa.Column(polars_engine.Int32, pa.Check.gt(0)),
        "energy_kwh": pa.Column(polars_engine.Float64, pa.Check.ge(0), nullable=True),
    },
    strict=True,
    name="silver_fact_irrigation",
)

SILVER_FACT_INPUT_APPLICATION = pa.DataFrameSchema(
    {
        "application_id": _SKEY,
        "field_id": _SID,
        "farm_id": _SID,
        "planting_id": pa.Column(polars_engine.Int64, nullable=True),
        "applied_on": _SDAY,
        "input_type": pa.Column(
            polars_engine.String,
            pa.Check.isin(["fertilizer", "pesticide", "herbicide", "fungicide", "lime"]),
        ),
        "product_name": _STEXT,
        "application_method": _STEXT,
        "rate_kg_ha": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "quantity_kg": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "unit_cost_usd_per_kg": _money(),
        "total_cost_usd": _money(),
    },
    strict=True,
    name="silver_fact_input_application",
)

SILVER_FACT_HARVEST = pa.DataFrameSchema(
    {
        "harvest_id": _SKEY,
        "planting_id": pa.Column(polars_engine.Int64, unique=True),
        "field_id": _SID,
        "farm_id": _SID,
        "harvested_on": _SDAY,
        "area_harvested_ha": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "yield_tonnes": pa.Column(polars_engine.Float64, pa.Check.ge(0)),
        "yield_t_ha": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 500)),
        "moisture_pct": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 100), nullable=True),
        "quality_grade": pa.Column(polars_engine.String, pa.Check.isin(["A", "B", "C", "reject"])),
        "price_usd_per_tonne": _money(),
        "revenue_usd": _money(),
    },
    strict=True,
    name="silver_fact_harvest",
)

SILVER_FACT_FIELD_COST = pa.DataFrameSchema(
    {
        "cost_id": _SKEY,
        "field_id": _SID,
        "farm_id": _SID,
        "planting_id": pa.Column(polars_engine.Int64, nullable=True),
        "cost_date": _SDAY,
        "cost_category": pa.Column(
            polars_engine.String,
            pa.Check.isin(
                [
                    "seed",
                    "fertilizer",
                    "crop_protection",
                    "irrigation",
                    "fuel",
                    "labour",
                    "machinery",
                    "other",
                ]
            ),
        ),
        "amount_usd": _money(nullable=False),
        "description": pa.Column(polars_engine.String, nullable=True),
    },
    strict=True,
    name="silver_fact_field_cost",
)

# Machinery
SILVER_DIM_MACHINE = pa.DataFrameSchema(
    {
        "machine_id": _SKEY,
        "machine_code": pa.Column(polars_engine.String, unique=True),
        "farm_id": _SID,
        "machine_type": _STEXT,
        "manufacturer": _STEXT,
        "model": _STEXT,
        "year_built": pa.Column(polars_engine.Int32, pa.Check.in_range(1900, 2100)),
        "rated_power_hp": pa.Column(polars_engine.Int32, pa.Check.gt(0)),
        "purchase_date": _SDAY,
        "engine_hours_at_purchase": pa.Column(polars_engine.Float64, pa.Check.ge(0)),
        "status": pa.Column(
            polars_engine.String, pa.Check.isin(["active", "maintenance", "retired"])
        ),
    },
    strict=True,
    name="silver_dim_machine",
)

SILVER_FACT_MACHINE_OPERATION = pa.DataFrameSchema(
    {
        "operation_id": _SKEY,
        "machine_id": _SID,
        "field_id": _SID,
        "farm_id": _SID,
        "planting_id": pa.Column(polars_engine.Int64, nullable=True),
        "operation_type": _STEXT,
        "started_at": pa.Column(utc_datetime()),
        "finished_at": pa.Column(utc_datetime()),
        "operation_date": _SDAY,
        "duration_hours": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "area_covered_ha": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "fuel_used_litres": pa.Column(polars_engine.Float64, pa.Check.ge(0)),
        "distance_km": pa.Column(polars_engine.Float64, pa.Check.ge(0), nullable=True),
        "operator_name": pa.Column(polars_engine.String, nullable=True),
    },
    strict=True,
    name="silver_fact_machine_operation",
)

SILVER_FACT_MACHINE_TELEMETRY = pa.DataFrameSchema(
    {
        "telemetry_id": _SKEY,
        "machine_id": _SID,
        "farm_id": _SID,
        "operation_id": pa.Column(polars_engine.Int64, nullable=True),
        "reading_ts": pa.Column(utc_datetime()),
        "reading_date": _SDAY,
        "engine_hours": pa.Column(polars_engine.Float64, pa.Check.ge(0)),
        "engine_running": pa.Column(polars_engine.Bool),
        "is_idle": pa.Column(polars_engine.Bool),
        "fuel_level_pct": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 100), nullable=True
        ),
        "fuel_rate_l_per_h": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 500), nullable=True
        ),
        "engine_temp_c": pa.Column(
            polars_engine.Float64, pa.Check.in_range(-40, 200), nullable=True
        ),
        "engine_rpm": pa.Column(polars_engine.Int32, pa.Check.in_range(0, 10_000), nullable=True),
        "speed_kmh": pa.Column(polars_engine.Float64, pa.Check.in_range(0, 200), nullable=True),
        "latitude": pa.Column(polars_engine.Float64, pa.Check.in_range(-90, 90), nullable=True),
        "longitude": pa.Column(polars_engine.Float64, pa.Check.in_range(-180, 180), nullable=True),
    },
    strict=True,
    name="silver_fact_machine_telemetry",
)

SILVER_FACT_MACHINE_FAULT = pa.DataFrameSchema(
    {
        "fault_id": _SKEY,
        "machine_id": _SID,
        "farm_id": _SID,
        "occurred_at": pa.Column(utc_datetime()),
        "occurred_date": _SDAY,
        "fault_code": _STEXT,
        "severity": pa.Column(polars_engine.String, pa.Check.isin(["info", "warning", "critical"])),
        "description": _STEXT,
        # Null while the fault is open — which is exactly what a
        # maintenance-due signal keys off.
        "resolved_at": pa.Column(utc_datetime(), nullable=True),
        "is_open": pa.Column(polars_engine.Bool),
        "downtime_hours": pa.Column(polars_engine.Float64, pa.Check.ge(0), nullable=True),
        "repair_cost_usd": _money(),
    },
    strict=True,
    name="silver_fact_machine_fault",
)

# Imagery
SILVER_FACT_FIELD_INDEX = pa.DataFrameSchema(
    {
        "observation_id": _SKEY,
        "field_id": _SID,
        "farm_id": _SID,
        "planting_id": pa.Column(polars_engine.Int64, nullable=True),
        "observed_on": _SDAY,
        "source": pa.Column(polars_engine.String, pa.Check.isin(["satellite", "drone", "aerial"])),
        "platform": pa.Column(polars_engine.String, nullable=True),
        # Physically bounded to [-1, 1]; anything outside is a broken pipeline,
        # not a stressed crop. Null on a pass lost to cloud.
        "ndvi": pa.Column(polars_engine.Float64, pa.Check.in_range(-1, 1), nullable=True),
        "ndwi": pa.Column(polars_engine.Float64, pa.Check.in_range(-1, 1), nullable=True),
        "evi": pa.Column(polars_engine.Float64, pa.Check.in_range(-1, 1), nullable=True),
        "cloud_cover_pct": pa.Column(
            polars_engine.Float64, pa.Check.in_range(0, 100), nullable=True
        ),
        "resolution_m": pa.Column(polars_engine.Float64, pa.Check.gt(0)),
        "usable": pa.Column(polars_engine.Bool),
    },
    strict=True,
    name="silver_fact_field_index",
)
