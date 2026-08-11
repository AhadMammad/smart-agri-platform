"""Every Silver dataset, declared.

One entry per conformed table. Adding a source to the platform is adding a
`SilverSpec` here and a Bronze spec alongside it — no new pipeline class.

Two conventions run through all of them:

* **`farm_id` is carried everywhere.** Every analytics question is eventually
  sliced by farm or region, and joining a fact back through its field at query
  time is both slower and easier to get wrong than carrying the key.
* **A date column accompanies every timestamp.** ClickHouse partitions on it and
  every dashboard groups by it, so materialising it once here saves repeating
  the same `toDate()` in a dozen chart definitions.
"""

from __future__ import annotations

import polars as pl

from smart_agri.contracts import (
    SILVER_DIM_CROP,
    SILVER_DIM_CROP_VARIETY,
    SILVER_DIM_FARM,
    SILVER_DIM_FIELD,
    SILVER_DIM_MACHINE,
    SILVER_DIM_PLANTING,
    SILVER_DIM_REGION,
    SILVER_DIM_SENSOR,
    SILVER_DIM_SOIL_TYPE,
    SILVER_FACT_FIELD_COST,
    SILVER_FACT_FIELD_INDEX,
    SILVER_FACT_HARVEST,
    SILVER_FACT_INPUT_APPLICATION,
    SILVER_FACT_IRRIGATION,
    SILVER_FACT_MACHINE_FAULT,
    SILVER_FACT_MACHINE_OPERATION,
    SILVER_FACT_MACHINE_TELEMETRY,
    SILVER_FACT_SENSOR_READING,
)
from smart_agri.pipelines.silver_spec import JoinSpec, SilverSpec

#: Bring a field's farm across. Used by every fact keyed on a field.
_FARM_VIA_FIELD = JoinSpec(dataset="field", on=("field_id",), columns=("farm_id",))

#: Bring a machine's farm across.
_FARM_VIA_MACHINE = JoinSpec(dataset="machine", on=("machine_id",), columns=("farm_id",))


# --- Phase 2: the soil-sensor slice ------------------------------------------
DIM_FARM = SilverSpec(
    dataset="dim_farm",
    bronze="farm",
    schema=SILVER_DIM_FARM,
    rename={"name": "farm_name"},
    trim=("farm_code", "farm_name", "region"),
    upper=("country_code",),
    cast={
        "farm_id": pl.Int64,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        "area_ha": pl.Float64,
    },
    dedupe_on=("farm_id",),
)

DIM_FIELD = SilverSpec(
    dataset="dim_field",
    bronze="field",
    schema=SILVER_DIM_FIELD,
    joins=(JoinSpec(dataset="farm", on=("farm_id",), columns=("farm_code",)),),
    rename={"name": "field_name"},
    trim=("field_code", "farm_code", "field_name"),
    lower=("soil_type",),
    cast={
        "field_id": pl.Int64,
        "farm_id": pl.Int64,
        "area_ha": pl.Float64,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    },
    dedupe_on=("field_id",),
)

DIM_SENSOR = SilverSpec(
    dataset="dim_sensor",
    bronze="sensor",
    schema=SILVER_DIM_SENSOR,
    joins=(JoinSpec(dataset="field", on=("field_id",), columns=("field_code", "farm_id")),),
    trim=("sensor_code", "field_code", "model"),
    lower=("sensor_type", "status"),
    cast={
        "sensor_id": pl.Int64,
        "field_id": pl.Int64,
        "farm_id": pl.Int64,
        "depth_cm": pl.Int32,
        "installed_on": pl.Date,
    },
    dedupe_on=("sensor_id",),
)

FACT_SENSOR_READING = SilverSpec(
    dataset="fact_sensor_reading",
    bronze="sensor_reading",
    schema=SILVER_FACT_SENSOR_READING,
    incremental=True,
    joins=(
        JoinSpec(dataset="sensor", on=("sensor_id",), columns=("field_id", "status")),
        _FARM_VIA_FIELD,
    ),
    # Readings from a decommissioned probe are stale device noise rather than
    # measurements, and averaging them drags every field metric off. Faulty
    # probes are kept — their readings are real, and Gold decides what to do
    # with them.
    filters=(pl.col("status") != "decommissioned",),
    derive={"reading_date": pl.col("reading_ts").dt.date()},
    cast={"reading_id": pl.Int64, "sensor_id": pl.Int64, "field_id": pl.Int64, "farm_id": pl.Int64},
    dedupe_on=("reading_id",),
)


# --- Phase 5: reference ------------------------------------------------------
DIM_REGION = SilverSpec(
    dataset="dim_region",
    bronze="region",
    schema=SILVER_DIM_REGION,
    rename={"name": "region_name"},
    trim=("region_code", "region_name", "country_name", "macro_region"),
    upper=("country_code",),
    lower=("rainfall_pattern",),
    dedupe_on=("region_code",),
)

DIM_SOIL_TYPE = SilverSpec(
    dataset="dim_soil_type",
    bronze="soil_type",
    schema=SILVER_DIM_SOIL_TYPE,
    rename={"name": "soil_name"},
    trim=("soil_name",),
    lower=("soil_code", "drainage"),
    cast={"moisture_min_pct": pl.Float64, "moisture_max_pct": pl.Float64},
    dedupe_on=("soil_code",),
)

DIM_CROP = SilverSpec(
    dataset="dim_crop",
    bronze="crop",
    schema=SILVER_DIM_CROP,
    rename={"name": "crop_name"},
    trim=("crop_name",),
    lower=("crop_code", "category"),
    cast={
        "base_temp_c": pl.Float64,
        "gdd_to_maturity": pl.Int32,
        "cycle_days": pl.Int32,
        "water_demand_mm": pl.Int32,
        "peak_ndvi": pl.Float64,
    },
    dedupe_on=("crop_code",),
)

DIM_CROP_VARIETY = SilverSpec(
    dataset="dim_crop_variety",
    bronze="crop_variety",
    schema=SILVER_DIM_CROP_VARIETY,
    rename={"name": "variety_name"},
    trim=("variety_code", "variety_name"),
    lower=("crop_code", "drought_tolerance"),
    cast={"variety_id": pl.Int64, "maturity_days": pl.Int32, "yield_potential_t_ha": pl.Float64},
    dedupe_on=("variety_id",),
)


# --- Phase 5: operations -----------------------------------------------------
DIM_PLANTING = SilverSpec(
    dataset="dim_planting",
    bronze="planting",
    schema=SILVER_DIM_PLANTING,
    joins=(_FARM_VIA_FIELD,),
    trim=("season",),
    lower=("status",),
    cast={
        "planting_id": pl.Int64,
        "field_id": pl.Int64,
        "farm_id": pl.Int64,
        "variety_id": pl.Int64,
        "area_ha": pl.Float64,
        "seed_rate_kg_ha": pl.Float64,
        "planted_on": pl.Date,
        "expected_harvest_on": pl.Date,
    },
    dedupe_on=("planting_id",),
)

FACT_IRRIGATION = SilverSpec(
    dataset="fact_irrigation",
    bronze="irrigation_event",
    schema=SILVER_FACT_IRRIGATION,
    incremental=True,
    joins=(_FARM_VIA_FIELD,),
    lower=("method", "water_source"),
    cast={
        "irrigation_id": pl.Int64,
        "field_id": pl.Int64,
        "farm_id": pl.Int64,
        "planting_id": pl.Int64,
        "water_volume_m3": pl.Float64,
        "depth_mm": pl.Float64,
        "duration_minutes": pl.Int32,
        "energy_kwh": pl.Float64,
    },
    derive={"event_date": pl.col("event_ts").dt.date()},
    dedupe_on=("irrigation_id",),
)

FACT_INPUT_APPLICATION = SilverSpec(
    dataset="fact_input_application",
    bronze="input_application",
    schema=SILVER_FACT_INPUT_APPLICATION,
    incremental=True,
    joins=(_FARM_VIA_FIELD,),
    trim=("product_name",),
    lower=("input_type", "application_method"),
    cast={
        "application_id": pl.Int64,
        "field_id": pl.Int64,
        "farm_id": pl.Int64,
        "planting_id": pl.Int64,
        "applied_on": pl.Date,
        "rate_kg_ha": pl.Float64,
        "quantity_kg": pl.Float64,
        "unit_cost_usd_per_kg": pl.Float64,
        "total_cost_usd": pl.Float64,
    },
    dedupe_on=("application_id",),
)

FACT_HARVEST = SilverSpec(
    dataset="fact_harvest",
    bronze="harvest",
    schema=SILVER_FACT_HARVEST,
    incremental=True,
    joins=(_FARM_VIA_FIELD,),
    trim=("quality_grade",),
    cast={
        "harvest_id": pl.Int64,
        "planting_id": pl.Int64,
        "field_id": pl.Int64,
        "farm_id": pl.Int64,
        "harvested_on": pl.Date,
        "area_harvested_ha": pl.Float64,
        "yield_tonnes": pl.Float64,
        "yield_t_ha": pl.Float64,
        "moisture_pct": pl.Float64,
        "price_usd_per_tonne": pl.Float64,
        "revenue_usd": pl.Float64,
    },
    dedupe_on=("harvest_id",),
)

FACT_FIELD_COST = SilverSpec(
    dataset="fact_field_cost",
    bronze="field_cost",
    schema=SILVER_FACT_FIELD_COST,
    incremental=True,
    joins=(_FARM_VIA_FIELD,),
    lower=("cost_category",),
    cast={
        "cost_id": pl.Int64,
        "field_id": pl.Int64,
        "farm_id": pl.Int64,
        "planting_id": pl.Int64,
        "cost_date": pl.Date,
        "amount_usd": pl.Float64,
    },
    dedupe_on=("cost_id",),
)


# --- Phase 5: machinery ------------------------------------------------------
DIM_MACHINE = SilverSpec(
    dataset="dim_machine",
    bronze="machine",
    schema=SILVER_DIM_MACHINE,
    trim=("machine_code", "manufacturer", "model"),
    lower=("machine_type", "status"),
    cast={
        "machine_id": pl.Int64,
        "farm_id": pl.Int64,
        "year_built": pl.Int32,
        "rated_power_hp": pl.Int32,
        "purchase_date": pl.Date,
        "engine_hours_at_purchase": pl.Float64,
    },
    dedupe_on=("machine_id",),
)

FACT_MACHINE_OPERATION = SilverSpec(
    dataset="fact_machine_operation",
    bronze="machine_operation",
    schema=SILVER_FACT_MACHINE_OPERATION,
    incremental=True,
    joins=(_FARM_VIA_MACHINE,),
    trim=("operator_name",),
    lower=("operation_type",),
    cast={
        "operation_id": pl.Int64,
        "machine_id": pl.Int64,
        "field_id": pl.Int64,
        "farm_id": pl.Int64,
        "planting_id": pl.Int64,
        "area_covered_ha": pl.Float64,
        "fuel_used_litres": pl.Float64,
        "distance_km": pl.Float64,
    },
    derive={
        "operation_date": pl.col("started_at").dt.date(),
        # Utilisation is the headline machinery metric; deriving it once here
        # keeps every consumer from recomputing the same subtraction.
        "duration_hours": (pl.col("finished_at") - pl.col("started_at")).dt.total_seconds()
        / 3600.0,
    },
    dedupe_on=("operation_id",),
)

FACT_MACHINE_TELEMETRY = SilverSpec(
    dataset="fact_machine_telemetry",
    bronze="machine_telemetry",
    schema=SILVER_FACT_MACHINE_TELEMETRY,
    incremental=True,
    joins=(_FARM_VIA_MACHINE,),
    cast={
        "telemetry_id": pl.Int64,
        "machine_id": pl.Int64,
        "farm_id": pl.Int64,
        "operation_id": pl.Int64,
        "engine_hours": pl.Float64,
        "fuel_level_pct": pl.Float64,
        "fuel_rate_l_per_h": pl.Float64,
        "engine_temp_c": pl.Float64,
        "engine_rpm": pl.Int32,
        "speed_kmh": pl.Float64,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    },
    derive={"reading_date": pl.col("reading_ts").dt.date()},
    dedupe_on=("telemetry_id",),
)

FACT_MACHINE_FAULT = SilverSpec(
    dataset="fact_machine_fault",
    bronze="machine_fault",
    schema=SILVER_FACT_MACHINE_FAULT,
    incremental=True,
    joins=(_FARM_VIA_MACHINE,),
    trim=("fault_code", "description"),
    lower=("severity",),
    cast={
        "fault_id": pl.Int64,
        "machine_id": pl.Int64,
        "farm_id": pl.Int64,
        "downtime_hours": pl.Float64,
        "repair_cost_usd": pl.Float64,
    },
    derive={
        "occurred_date": pl.col("occurred_at").dt.date(),
        # An unresolved fault is what a maintenance-due signal keys off, so the
        # state is materialised rather than left as "resolved_at IS NULL" for
        # every consumer to rediscover.
        "is_open": pl.col("resolved_at").is_null(),
    },
    dedupe_on=("fault_id",),
)


# --- Phase 5: imagery --------------------------------------------------------
FACT_FIELD_INDEX = SilverSpec(
    dataset="fact_field_index",
    bronze="field_index_observation",
    schema=SILVER_FACT_FIELD_INDEX,
    incremental=True,
    joins=(_FARM_VIA_FIELD,),
    trim=("platform",),
    lower=("source",),
    cast={
        "observation_id": pl.Int64,
        "field_id": pl.Int64,
        "farm_id": pl.Int64,
        "planting_id": pl.Int64,
        "observed_on": pl.Date,
        "ndvi": pl.Float64,
        "ndwi": pl.Float64,
        "evi": pl.Float64,
        "cloud_cover_pct": pl.Float64,
        "resolution_m": pl.Float64,
    },
    dedupe_on=("observation_id",),
)


#: Declaration order is dependency order: a spec's joins may only reference a
#: Bronze dataset, so the only ordering that matters is dimensions before the
#: facts that carry their keys.
SILVER_SPECS: tuple[SilverSpec, ...] = (
    # dimensions and reference
    DIM_FARM,
    DIM_FIELD,
    DIM_SENSOR,
    DIM_REGION,
    DIM_SOIL_TYPE,
    DIM_CROP,
    DIM_CROP_VARIETY,
    DIM_PLANTING,
    DIM_MACHINE,
    # facts
    FACT_SENSOR_READING,
    FACT_IRRIGATION,
    FACT_INPUT_APPLICATION,
    FACT_HARVEST,
    FACT_FIELD_COST,
    FACT_MACHINE_OPERATION,
    FACT_MACHINE_TELEMETRY,
    FACT_MACHINE_FAULT,
    FACT_FIELD_INDEX,
)

SILVER_SPECS_BY_DATASET: dict[str, SilverSpec] = {spec.dataset: spec for spec in SILVER_SPECS}
