"""Command-line entrypoint.

Airflow's DockerOperator launches this image with a command vector, so every
pipeline is reachable as a subcommand and no Airflow-specific code leaks into
the ETL application. Keep subcommands thin — they parse arguments, build the
pipeline object, and delegate.
"""

from __future__ import annotations

import sys
from typing import Annotated

import typer

from smart_agri import __version__
from smart_agri.config import get_settings
from smart_agri.health import run_health_checks
from smart_agri.utils import configure_logging

app = typer.Typer(
    name="smart-agri",
    help="ETL for the smart agriculture analytics platform.",
    no_args_is_help=True,
    add_completion=False,
)


@app.callback()
def main(
    log_level: Annotated[
        str | None,
        typer.Option("--log-level", help="Override SMART_AGRI_LOG_LEVEL."),
    ] = None,
) -> None:
    """Configure logging before any subcommand runs."""
    settings = get_settings()
    configure_logging(log_level or settings.log_level)


@app.command()
def version() -> None:
    """Print the ETL application version."""
    typer.echo(__version__)


@app.command()
def doctor(
    timeout: Annotated[
        float,
        typer.Option("--timeout", help="Per-service timeout in seconds."),
    ] = 10.0,
) -> None:
    """Check connectivity to Postgres, ClickHouse, HDFS and the Hive Metastore.

    Exits non-zero if any service is unreachable, so it doubles as a gate in the
    Makefile and in CI.
    """
    results = run_health_checks(timeout_s=timeout)

    for result in results:
        typer.echo(str(result))

    failed = [r for r in results if not r.healthy]
    if failed:
        typer.echo(f"\n{len(failed)} of {len(results)} services unhealthy.", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"\nAll {len(results)} services healthy.")


if __name__ == "__main__":  # pragma: no cover
    sys.exit(app())
