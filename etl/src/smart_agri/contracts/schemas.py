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
