"""The pipeline registry.

Every pipeline is reachable by name, so the CLI is a single `run <name>` command
and a DAG is a list of names rather than a set of imports. Phase 5 adds domains
by registering more specs here.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from smart_agri.pipelines.base import BasePipeline, PipelineContext
from smart_agri.pipelines.bronze import (
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
from smart_agri.pipelines.silver import (
    SilverDimFarmPipeline,
    SilverDimFieldPipeline,
    SilverDimSensorPipeline,
    SilverFactSensorReadingPipeline,
)
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


def _snapshot_factory(spec_index: int) -> PipelineFactory:
    """Bind a spec by index so the closure captures a value, not the loop var."""

    def build(context: PipelineContext) -> BasePipeline:
        return BronzeSnapshotPipeline(SNAPSHOT_SPECS[spec_index], context)

    return build


def _dim_load_factory(spec_index: int) -> PipelineFactory:
    def build(context: PipelineContext) -> BasePipeline:
        return LoadDimensionPipeline(DIM_LOAD_SPECS[spec_index], context)

    return build


_REGISTRY: dict[str, PipelineFactory] = {
    # Bronze
    **{
        f"bronze.{spec.dataset}": _snapshot_factory(index)
        for index, spec in enumerate(SNAPSHOT_SPECS)
    },
    "bronze.sensor_reading": BronzeIncrementalPipeline,
    # Silver
    "silver.dim_farm": SilverDimFarmPipeline,
    "silver.dim_field": SilverDimFieldPipeline,
    "silver.dim_sensor": SilverDimSensorPipeline,
    "silver.fact_sensor_reading": SilverFactSensorReadingPipeline,
    # Gold
    "gold.field_soil_daily": GoldFieldSoilDailyPipeline,
    # Load
    **{
        f"load.{spec.dataset}": _dim_load_factory(index)
        for index, spec in enumerate(DIM_LOAD_SPECS)
    },
    "load.fact_sensor_reading": LoadFactSensorReadingPipeline,
    "load.field_soil_daily": LoadGoldFieldSoilDailyPipeline,
    # --- weather (Phase 4) ---
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


def pipeline_names() -> Sequence[str]:
    """Every registered pipeline name, sorted."""
    return sorted(_REGISTRY)


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
