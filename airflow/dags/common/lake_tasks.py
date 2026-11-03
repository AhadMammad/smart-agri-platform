"""Factory for the Phase 5 domain DAGs.

Every domain does the same thing — extract its sources to Bronze, conform them
to Silver — and differs only in which tables and on what schedule. Building the
graph once here means adding a domain is a DAG file of a dozen lines, and adding
a *table* to a domain is one entry in `common/pipelines.py`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.utils.task_group import TaskGroup
from common.etl_task import etl_task
from common.pipelines import (
    DOMAIN_QUALITY_SUITES,
    DOMAIN_STAGES,
    LAKE_STAGE_LABELS,
    QUALITY_GROUP,
    task_id_for,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


def build_domain_stages(
    domain: str,
    execution_date: str = "{{ ds }}",
    labels: Sequence[str] = LAKE_STAGE_LABELS,
) -> None:
    """Create a domain's TaskGroups and wire them in order.

    Must be called inside an open `DAG` context.

    Args:
        domain: Key into `DOMAIN_STAGES`.
        execution_date: Templated logical date handed to every task, so all
            stages in a run share one partition rather than each resolving its
            own — which would split a run across midnight.
        labels: TaskGroup names, one per stage.

    Raises:
        KeyError: if the domain is unknown, listing the valid names. A DAG that
            silently produced no tasks would look like a healthy empty schedule.
    """
    try:
        stages = DOMAIN_STAGES[domain]
    except KeyError:
        valid = ", ".join(sorted(DOMAIN_STAGES))
        msg = f"unknown domain {domain!r}; valid domains: {valid}"
        raise KeyError(msg) from None

    if len(labels) != len(stages):
        msg = f"{domain} has {len(stages)} stages but {len(labels)} labels"
        raise ValueError(msg)

    previous: TaskGroup | None = None
    for label, stage in zip(labels, stages, strict=True):
        with TaskGroup(group_id=label) as group:
            for pipeline in stage:
                etl_task(
                    task_id=task_id_for(pipeline),
                    command=["run", pipeline, "--date", execution_date],
                )

        if previous is not None:
            previous >> group
        previous = group

    add_quality_group(domain, previous, execution_date)


def add_quality_group(
    domain: str, after: TaskGroup | None, execution_date: str = "{{ ds }}"
) -> TaskGroup | None:
    """Run the domain's data quality suites once its last stage has finished.

    Last, because reconciliation compares the warehouse with the source and is
    only meaningful once the load is complete. A failed suite fails the DAG run,
    which is what makes it visible; nothing runs after it to block.
    """
    suites = DOMAIN_QUALITY_SUITES.get(domain, ())
    if not suites:
        return None

    with TaskGroup(group_id=QUALITY_GROUP) as group:
        for suite in suites:
            etl_task(
                task_id=suite.replace(".", "_"),
                command=["dq", "run", suite, "--date", execution_date],
            )

    if after is not None:
        after >> group
    return group
