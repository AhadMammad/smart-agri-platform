"""Reference data — Bronze and Silver.

Extracts the reference tables — regions, soil classes, crops and varieties into the lake and conforms them.

Every task is one `smart-agri run <pipeline>` in its own container. The domain
re-extracts the dimensions its joins need rather than assuming another DAG ran
first, so it can be scheduled, retried or backfilled on its own.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from common.lake_tasks import build_domain_stages

with DAG(
    dag_id="reference_daily",
    description="Reference data: Postgres to the Bronze and Silver lake zones",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
        "execution_timeout": pendulum.duration(minutes=30),
    },
    tags=["reference", "lake", "phase-5"],
    doc_md=__doc__,
) as dag:
    build_domain_stages("reference")
