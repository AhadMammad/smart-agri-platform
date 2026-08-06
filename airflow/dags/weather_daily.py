"""Daily weather refresh.

Runs the whole weather chain each morning: pull the archive up to where it has
caught up, pull the forecast endpoint for the days it has not, merge them with
measurements winning, and reload ClickHouse.

Both weather DAGs run the same stages. The difference is cadence and intent —
this one keeps the series current, `weather_backfill` establishes it — so the
two share a factory rather than duplicating a task graph that would drift.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from common.weather_tasks import build_weather_stages

with DAG(
    dag_id="weather_daily",
    description="Refresh Open-Meteo weather for every farm and reload ClickHouse",
    # Early enough that the previous day's archive has settled, before the
    # working day starts asking the dashboards questions.
    schedule="30 5 * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        # More retries than the lake DAGs: this one depends on a third-party
        # API, so a failure is often transient in a way a local one is not.
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        "execution_timeout": pendulum.duration(minutes=30),
    },
    tags=["weather", "irrigation-water", "phase-4"],
    doc_md=__doc__,
) as dag:
    build_weather_stages()
