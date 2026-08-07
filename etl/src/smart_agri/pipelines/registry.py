"""The pipeline registry.

Every pipeline is reachable by name, so the CLI is a single `run <name>` command
and a DAG is a list of names rather than a set of imports.

Bronze and Silver are built from the specs in `bronze.py` and `silver_specs.py`
rather than registered one by one: adding a source table is a declaration, and
the registry picks it up automatically.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from smart_agri.pipelines.base import BasePipeline, PipelineContext
from smart_agri.pipelines.bronze import (
    INCREMENTAL_SPECS,
    SNAPSHOT_SPECS,
    BronzeIncrementalPipeline,
    BronzeSnapshotPipeline,
)
from smart_agri.pipelines.gold import GoldFieldSoilDailyPipeline
from smart_agri.pipelines.load import (
    DIM_LOAD_SPECS,
    LoadDimensionPipeline,
    LoadFactSensorReadingPipeline,
    LoadGoldFieldSoilDailyPipeline,
)
from smart_agri.pipelines.silver_spec import SilverPipeline
from smart_agri.pipelines.silver_specs import SILVER_SPECS
from smart_agri.pipelines.weather import (
    BronzeWeatherArchivePipeline,
    BronzeWeatherForecastPipeline,
    GoldDimDatePipeline,
    GoldFieldWeatherDailyPipeline,
    LoadDimDatePipeline,
    LoadFactWeatherDailyPipeline,
    LoadFieldWeatherDailyPipeline,
    SilverFactWeatherDailyPipeline,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

PipelineFactory = Callable[[PipelineContext], BasePipeline]


def _bind(builder: Callable[..., BasePipeline], spec: object) -> PipelineFactory:
    """Bind a spec to its pipeline class.

    A closure over the loop variable would capture the last spec for every
    entry, so the spec is bound as a default argument instead.
    """

    def build(context: PipelineContext, _spec: object = spec) -> BasePipeline:
        return builder(_spec, context)

    return build


_REGISTRY: dict[str, PipelineFactory] = {
    # --- Bronze: one entry per declared source ---
    **{f"bronze.{spec.dataset}": _bind(BronzeSnapshotPipeline, spec) for spec in SNAPSHOT_SPECS},
    **{
        f"bronze.{spec.dataset}": _bind(BronzeIncrementalPipeline, spec)
        for spec in INCREMENTAL_SPECS
    },
    # --- Silver: one entry per declared spec ---
    **{f"silver.{spec.dataset}": _bind(SilverPipeline, spec) for spec in SILVER_SPECS},
    # --- Gold and load (Phase 2 slice) ---
    "gold.field_soil_daily": GoldFieldSoilDailyPipeline,
    **{f"load.{spec.dataset}": _bind(LoadDimensionPipeline, spec) for spec in DIM_LOAD_SPECS},
    "load.fact_sensor_reading": LoadFactSensorReadingPipeline,
    "load.field_soil_daily": LoadGoldFieldSoilDailyPipeline,
    # --- Weather (Phase 4) ---
    "bronze.weather_archive": BronzeWeatherArchivePipeline,
    "bronze.weather_forecast": BronzeWeatherForecastPipeline,
    "silver.fact_weather_daily": SilverFactWeatherDailyPipeline,
    "gold.dim_date": GoldDimDatePipeline,
    "gold.field_weather_daily": GoldFieldWeatherDailyPipeline,
    "load.dim_date": LoadDimDatePipeline,
    "load.fact_weather_daily": LoadFactWeatherDailyPipeline,
    "load.field_weather_daily": LoadFieldWeatherDailyPipeline,
}


#: Execution order for the soil-sensor slice, grouped into stages that can run
#: in parallel. The DAG mirrors this, and so does `smart-agri run-all`.
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


#: Weather, as run by both weather DAGs. `bronze.farm` and the Silver
#: dimensions are re-run here rather than assumed: weather joins to farms and
#: fields, and the weather DAGs must not depend on the soil DAG having run.
WEATHER_STAGES: tuple[tuple[str, ...], ...] = (
    ("bronze.farm", "bronze.field"),
    ("bronze.weather_archive", "bronze.weather_forecast"),
    ("silver.dim_farm", "silver.dim_field", "silver.fact_weather_daily"),
    ("gold.dim_date", "gold.field_weather_daily"),
    ("load.dim_date", "load.fact_weather_daily", "load.field_weather_daily"),
)


#: Phase 5 domains. Each is a Bronze stage then a Silver stage; every one
#: re-extracts the dimensions its joins need rather than assuming another DAG
#: ran first, so a domain can be scheduled, retried or backfilled on its own.
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

#: Every domain, by name — used by the DAG factory and `smart-agri run-domain`.
DOMAIN_STAGES: dict[str, tuple[tuple[str, ...], ...]] = {
    "soil_sensor": SOIL_SENSOR_STAGES,
    "weather": WEATHER_STAGES,
    "reference": REFERENCE_STAGES,
    "operations": OPERATIONS_STAGES,
    "machinery": MACHINERY_STAGES,
    "imagery": IMAGERY_STAGES,
}


def pipeline_names() -> Sequence[str]:
    """Every registered pipeline name, sorted."""
    return sorted(_REGISTRY)


def domain_names() -> Sequence[str]:
    """Every domain name, sorted."""
    return sorted(DOMAIN_STAGES)


def get_pipeline(name: str, context: PipelineContext | None = None) -> BasePipeline:
    """Build a pipeline by name.

    Raises:
        KeyError: with the valid names listed, because a typo in a DAG should
            say what was expected rather than just fail.
    """
    try:
        factory = _REGISTRY[name]
    except KeyError:
        valid = ", ".join(pipeline_names())
        msg = f"unknown pipeline {name!r}; registered pipelines: {valid}"
        raise KeyError(msg) from None

    return factory(context or PipelineContext())


def get_stages(domain: str) -> tuple[tuple[str, ...], ...]:
    """Stage list for a domain, failing with the valid names listed."""
    try:
        return DOMAIN_STAGES[domain]
    except KeyError:
        valid = ", ".join(domain_names())
        msg = f"unknown domain {domain!r}; valid domains: {valid}"
        raise KeyError(msg) from None
