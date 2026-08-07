"""Analytics marts and the warehouse load.

Builds the Gold tables the dashboards read — crop health, irrigation, machinery
and planting economics — then replaces every dimension, fact and aggregate in
ClickHouse.

Runs after the domain DAGs rather than beside them. A mart is cross-domain by
definition: the irrigation balance needs weather and operations, and planting
economics needs operations, weather and imagery. Scheduled late in the day so
the `@daily` domain DAGs and the 05:30 weather run have all landed.

Every task is one `smart-agri run <pipeline>` in its own container.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from common.lake_tasks import build_domain_stages
from common.pipelines import ANALYTICS_STAGE_LABELS

with DAG(
    dag_id="analytics_daily",
    description="Gold marts and the ClickHouse star schema",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=2),
        # Longer than the lake DAGs: planting economics joins five facts and a
        # date-range weather window across every planting in the estate.
        "execution_timeout": pendulum.duration(minutes=60),
    },
    tags=["analytics", "gold", "clickhouse", "phase-6"],
    doc_md=__doc__,
) as dag:
    build_domain_stages("analytics", labels=ANALYTICS_STAGE_LABELS)
