"""Connectors to every external system."""

from __future__ import annotations

from smart_agri.io.clickhouse import ClickHouseSink
from smart_agri.io.control import ControlStore, RunStats, RunStatus
from smart_agri.io.hdfs import HdfsStore
from smart_agri.io.openmeteo import (
    DAILY_VARIABLES,
    WEATHER_SCHEMA,
    OpenMeteoError,
    OpenMeteoSource,
    RetryableOpenMeteoError,
    WeatherSource,
    WeatherWindow,
)
from smart_agri.io.postgres import PostgresSource

__all__ = [
    "DAILY_VARIABLES",
    "WEATHER_SCHEMA",
    "ClickHouseSink",
    "ControlStore",
    "HdfsStore",
    "OpenMeteoError",
    "OpenMeteoSource",
    "PostgresSource",
    "RetryableOpenMeteoError",
    "RunStats",
    "RunStatus",
    "WeatherSource",
    "WeatherWindow",
]
