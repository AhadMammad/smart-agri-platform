"""Shared DAG building blocks.

Airflow puts the dags folder on sys.path, so DAG files import this as
`from common.etl_task import etl_task`.
"""

from common.etl_task import etl_environment, etl_task
from common.lake_tasks import build_domain_stages
from common.pipelines import (
    DOMAIN_STAGES,
    LAKE_STAGE_LABELS,
    SOIL_SENSOR_STAGES,
    STAGE_LABELS,
    WEATHER_STAGE_LABELS,
    WEATHER_STAGES,
    task_id_for,
)
from common.weather_tasks import build_weather_stages

__all__ = [
    "DOMAIN_STAGES",
    "LAKE_STAGE_LABELS",
    "SOIL_SENSOR_STAGES",
    "STAGE_LABELS",
    "WEATHER_STAGES",
    "WEATHER_STAGE_LABELS",
    "build_domain_stages",
    "build_weather_stages",
    "etl_environment",
    "etl_task",
    "task_id_for",
]
