"""Canonical pipeline execution order for the soil-sensor slice.

Airflow carries no ETL dependencies, so it cannot import `smart_agri` to read
the registry. This module is therefore the orchestration-side copy of that list.

The duplication is deliberate but guarded: `tests/unit/test_dag_pipeline_sync.py`
in the ETL project parses this file and asserts it matches
`smart_agri.pipelines.SOIL_SENSOR_STAGES`, so a rename on either side fails CI
rather than a 2 a.m. DAG run.

Names within a stage have no dependency on each other and run in parallel;
stages run in order.
"""

from __future__ import annotations

SOIL_SENSOR_STAGES: tuple[tuple[str, ...], ...] = (
    ("bronze.farm", "bronze.field", "bronze.sensor", "bronze.sensor_reading"),
    (
        "silver.dim_farm",
        "silver.dim_field",
        "silver.dim_sensor",
        "silver.fact_sensor_reading",
    ),
    ("gold.field_soil_daily",),
    (
        "load.dim_farm",
        "load.dim_field",
        "load.dim_sensor",
        "load.fact_sensor_reading",
        "load.field_soil_daily",
    ),
)

STAGE_LABELS: tuple[str, ...] = ("bronze", "silver", "gold", "load")


#: Weather, run by both weather DAGs.
#:
#: `bronze.farm` and the Silver dimensions are repeated here rather than assumed:
#: weather joins to farms and fields, and a weather DAG must not silently depend
#: on the soil DAG having run first. Re-running them is cheap — they are small
#: snapshot extracts — and it makes each DAG independently correct.
WEATHER_STAGES: tuple[tuple[str, ...], ...] = (
    ("bronze.farm", "bronze.field"),
    ("bronze.weather_archive", "bronze.weather_forecast"),
    ("silver.dim_farm", "silver.dim_field", "silver.fact_weather_daily"),
    ("gold.dim_date", "gold.field_weather_daily"),
    ("load.dim_date", "load.fact_weather_daily", "load.field_weather_daily"),
)

WEATHER_STAGE_LABELS: tuple[str, ...] = (
    "bronze_source",
    "bronze_weather",
    "silver",
    "gold",
    "load",
)


#: Phase 5 domains. Each extracts its own dimensions rather than assuming
#: another DAG ran first, so a domain can be scheduled, retried or backfilled
#: independently. Kept in step with `smart_agri.pipelines.DOMAIN_STAGES` by
#: tests/unit/test_dag_pipeline_sync.py.
REFERENCE_STAGES: tuple[tuple[str, ...], ...] = (
    ("bronze.region", "bronze.soil_type", "bronze.crop", "bronze.crop_variety"),
    (
        "silver.dim_region",
        "silver.dim_soil_type",
        "silver.dim_crop",
        "silver.dim_crop_variety",
    ),
)

OPERATIONS_STAGES: tuple[tuple[str, ...], ...] = (
    (
        "bronze.farm",
        "bronze.field",
        "bronze.planting",
        "bronze.irrigation_event",
        "bronze.input_application",
        "bronze.harvest",
        "bronze.field_cost",
    ),
    (
        "silver.dim_farm",
        "silver.dim_field",
        "silver.dim_planting",
        "silver.fact_irrigation",
        "silver.fact_input_application",
        "silver.fact_harvest",
        "silver.fact_field_cost",
    ),
)

MACHINERY_STAGES: tuple[tuple[str, ...], ...] = (
    (
        "bronze.farm",
        "bronze.field",
        "bronze.machine",
        "bronze.machine_operation",
        "bronze.machine_telemetry",
        "bronze.machine_fault",
    ),
    (
        "silver.dim_machine",
        "silver.fact_machine_operation",
        "silver.fact_machine_telemetry",
        "silver.fact_machine_fault",
    ),
)

IMAGERY_STAGES: tuple[tuple[str, ...], ...] = (
    ("bronze.farm", "bronze.field", "bronze.field_index_observation"),
    ("silver.dim_field", "silver.fact_field_index"),
)

#: Every domain the platform schedules, by name.
DOMAIN_STAGES: dict[str, tuple[tuple[str, ...], ...]] = {
    "soil_sensor": SOIL_SENSOR_STAGES,
    "weather": WEATHER_STAGES,
    "reference": REFERENCE_STAGES,
    "operations": OPERATIONS_STAGES,
    "machinery": MACHINERY_STAGES,
    "imagery": IMAGERY_STAGES,
}

#: Bronze and Silver stages share these labels across the Phase 5 domains.
LAKE_STAGE_LABELS: tuple[str, ...] = ("bronze", "silver")


def task_id_for(pipeline: str) -> str:
    """Task id for a pipeline name, within its stage's TaskGroup.

    The stage prefix is dropped because the TaskGroup already supplies it: the
    pipeline `silver.dim_farm` becomes the task `dim_farm` inside the `silver`
    group, rendering as `silver.dim_farm` in the UI rather than the doubled-up
    `silver.silver_dim_farm`.
    """
    _, _, leaf = pipeline.partition(".")
    return leaf or pipeline
