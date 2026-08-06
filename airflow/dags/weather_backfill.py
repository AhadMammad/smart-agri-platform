"""Weather backfill.

Establishes the full weather history for every farm, from
`OPEN_METEO_BACKFILL_START` up to wherever the archive endpoint has reached.
Run once when the platform is first seeded, and again after adding farms.

Unscheduled and manually triggered. A backfill is a deliberate act with a real
cost — it is the heaviest external call the platform makes — and putting it on a
timer would repeat that cost daily for a result that changes only when the farm
list does. `weather_daily` keeps the series current afterwards.

The archive request is one call per farm covering the whole window, so a
year of history for five farms is five requests, not eighteen hundred.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from common.weather_tasks import build_weather_stages

with DAG(
    dag_id="weather_backfill",
    description="Load the full Open-Meteo history for every farm",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
        # Generous: the rate limiter deliberately paces the archive calls, so a
        # large estate takes minutes of mostly waiting.
        "execution_timeout": pendulum.duration(hours=2),
    },
    tags=["weather", "backfill", "phase-4"],
    doc_md=__doc__,
) as dag:
    # `{{ ds }}` resolves to the trigger date for a manual run, so every task in
    # the run shares one window instead of each resolving its own.
    build_weather_stages()
