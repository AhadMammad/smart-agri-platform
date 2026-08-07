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
from smart_agri.pipelines.gold_marts import (
    GoldFieldCropHealthDailyPipeline,
    GoldFieldIrrigationDailyPipeline,
    GoldMachineDailyPipeline,
    GoldPlantingEconomicsPipeline,
)
from smart_agri.pipelines.load import (
    DIM_LOAD_SPECS,
    FACT_LOAD_SPECS,
    GOLD_LOAD_SPECS,
    LoadDimensionPipeline,
    LoadFactPipeline,
    LoadGoldPipeline,
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
    # --- Gold: the marts, one per analytics domain ---
    "gold.field_soil_daily": GoldFieldSoilDailyPipeline,
    "gold.field_crop_health_daily": GoldFieldCropHealthDailyPipeline,
    "gold.field_irrigation_daily": GoldFieldIrrigationDailyPipeline,
    "gold.machine_daily": GoldMachineDailyPipeline,
    "gold.planting_economics": GoldPlantingEconomicsPipeline,
    # --- Load: one entry per declared spec, across all three shapes ---
    **{f"load.{spec.dataset}": _bind(LoadDimensionPipeline, spec) for spec in DIM_LOAD_SPECS},
    **{f"load.{spec.dataset}": _bind(LoadFactPipeline, spec) for spec in FACT_LOAD_SPECS},
    **{f"load.{spec.dataset}": _bind(LoadGoldPipeline, spec) for spec in GOLD_LOAD_SPECS},
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

#: Phase 6. The marts and the warehouse load, across every domain at once.
#:
#: Deliberately not folded into the domain DAGs. The Phase 5 domains each stand
#: alone because their Silver only needs their own Bronze — but a mart is
#: cross-domain by definition. `field_irrigation_daily` needs weather *and*
#: operations; `planting_economics` needs operations, weather and imagery. A
#: domain DAG that built it would either re-run half the platform or quietly
#: read whatever another DAG happened to leave behind.
#:
#: So this stage runs last, reads Silver and the weather Gold from the lake, and
#: is the only place the warehouse is written.
ANALYTICS_STAGES: tuple[tuple[str, ...], ...] = (
    (
        "gold.field_crop_health_daily",
        "gold.field_irrigation_daily",
        "gold.machine_daily",
        "gold.planting_economics",
    ),
    (
        # Dimensions
        "load.dim_region",
        "load.dim_soil_type",
        "load.dim_crop",
        "load.dim_crop_variety",
        "load.dim_planting",
        "load.dim_machine",
        # Facts
        "load.fact_irrigation",
        "load.fact_input_application",
        "load.fact_harvest",
        "load.fact_field_cost",
        "load.fact_machine_operation",
        "load.fact_machine_telemetry",
        "load.fact_machine_fault",
        "load.fact_field_index",
        # Marts
        "load.field_crop_health_daily",
        "load.field_irrigation_daily",
        "load.machine_daily",
        "load.planting_economics",
    ),
)

#: Every domain, by name — used by the DAG factory and `smart-agri run-domain`.
DOMAIN_STAGES: dict[str, tuple[tuple[str, ...], ...]] = {
    "soil_sensor": SOIL_SENSOR_STAGES,
    "weather": WEATHER_STAGES,
    "reference": REFERENCE_STAGES,
    "operations": OPERATIONS_STAGES,
    "machinery": MACHINERY_STAGES,
    "imagery": IMAGERY_STAGES,
    "analytics": ANALYTICS_STAGES,
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
