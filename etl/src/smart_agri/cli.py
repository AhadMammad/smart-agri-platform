"""Command-line entrypoint.

Airflow's DockerOperator launches this image with a command vector, so every
pipeline is reachable as a subcommand and no Airflow-specific code leaks into
the ETL application. Keep subcommands thin — they parse arguments, build the
pipeline object, and delegate.
"""

from __future__ import annotations

import sys
from datetime import UTC, date, datetime
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

_DateOption = Annotated[
    str | None,
    typer.Option(
        "--date",
        "-d",
        help="Logical date as YYYY-MM-DD. Defaults to today (UTC).",
    ),
]


def _resolve_date(value: str | None) -> date:
    """Parse a logical date, failing with a usable message rather than a stack trace."""
    if value is None:
        return datetime.now(tz=UTC).date()
    try:
        return date.fromisoformat(value)
    except ValueError:
        typer.echo(f"invalid --date {value!r}; expected YYYY-MM-DD", err=True)
        raise typer.Exit(code=2) from None


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


@app.command(name="list-pipelines")
def list_pipelines() -> None:
    """List every registered pipeline, in execution order."""
    from smart_agri.pipelines import SOIL_SENSOR_STAGES, pipeline_names

    staged = {name for stage in SOIL_SENSOR_STAGES for name in stage}

    for index, stage in enumerate(SOIL_SENSOR_STAGES, start=1):
        typer.echo(f"stage {index}:")
        for name in stage:
            typer.echo(f"  {name}")

    unstaged = sorted(set(pipeline_names()) - staged)
    if unstaged:
        typer.echo("\nregistered but not in any stage:")
        for name in unstaged:
            typer.echo(f"  {name}")


@app.command()
def run(
    pipeline: Annotated[str, typer.Argument(help="Pipeline name, e.g. silver.dim_farm.")],
    logical_date: _DateOption = None,
) -> None:
    """Run a single pipeline."""
    from smart_agri.pipelines import get_pipeline

    target = _resolve_date(logical_date)

    try:
        job = get_pipeline(pipeline)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from None

    stats = job.run(target)
    typer.echo(
        f"{pipeline}: read={stats.rows_read} written={stats.rows_written} "
        f"quarantined={stats.rows_quarantined}"
    )


@app.command(name="run-all")
def run_all(logical_date: _DateOption = None) -> None:
    """Run the whole soil-sensor slice in dependency order.

    Convenience for local runs and the integration test. Airflow runs the same
    pipelines as separate tasks so a failure is isolated to one stage.
    """
    from smart_agri.pipelines import SOIL_SENSOR_STAGES, PipelineContext, get_pipeline

    target = _resolve_date(logical_date)
    context = PipelineContext()

    for index, stage in enumerate(SOIL_SENSOR_STAGES, start=1):
        typer.echo(f"--- stage {index} ---")
        for name in stage:
            stats = get_pipeline(name, context).run(target)
            typer.echo(
                f"  {name}: read={stats.rows_read} written={stats.rows_written} "
                f"quarantined={stats.rows_quarantined}"
            )

    typer.echo("\nsoil-sensor slice complete.")


@app.command(name="run-domain")
def run_domain(
    domain: Annotated[str, typer.Argument(help="Domain name, e.g. operations.")],
    logical_date: _DateOption = None,
) -> None:
    """Run every pipeline in a domain, in dependency order.

    Convenience for a local run and for backfills. Airflow runs the same
    pipelines as separate tasks so a failure is isolated to one stage.
    """
    from smart_agri.pipelines import PipelineContext, get_pipeline, get_stages

    target = _resolve_date(logical_date)

    try:
        stages = get_stages(domain)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from None

    context = PipelineContext()
    for index, stage in enumerate(stages, start=1):
        typer.echo(f"--- stage {index} ---")
        for name in stage:
            stats = get_pipeline(name, context).run(target)
            typer.echo(
                f"  {name}: read={stats.rows_read} written={stats.rows_written} "
                f"quarantined={stats.rows_quarantined}"
            )

    typer.echo(f"\n{domain} complete.")


@app.command(name="list-domains")
def list_domains() -> None:
    """List the domains and what each one runs."""
    from smart_agri.pipelines import DOMAIN_STAGES

    for domain, stages in DOMAIN_STAGES.items():
        tasks = sum(len(stage) for stage in stages)
        typer.echo(f"{domain}  ({tasks} pipelines in {len(stages)} stages)")
        for index, stage in enumerate(stages, start=1):
            typer.echo(f"  stage {index}: {', '.join(stage)}")


@app.command()
def weather(logical_date: _DateOption = None) -> None:
    """Refresh weather for every farm and reload ClickHouse.

    Runs the same stages as the `weather_daily` DAG, in one process. Convenience
    for a local run; Airflow runs them as separate tasks so a failure is
    isolated to one stage.
    """
    from smart_agri.pipelines import WEATHER_STAGES, PipelineContext, get_pipeline

    target = _resolve_date(logical_date)
    context = PipelineContext()

    for index, stage in enumerate(WEATHER_STAGES, start=1):
        typer.echo(f"--- stage {index} ---")
        for name in stage:
            stats = get_pipeline(name, context).run(target)
            typer.echo(
                f"  {name}: read={stats.rows_read} written={stats.rows_written} "
                f"quarantined={stats.rows_quarantined}"
            )

    typer.echo("\nweather refresh complete.")


@app.command(name="weather-backfill")
def weather_backfill(logical_date: _DateOption = None) -> None:
    """Load the full Open-Meteo history for every farm.

    Identical to `weather` — the archive pipeline already reaches back to
    `OPEN_METEO_BACKFILL_START` on every run. Exposed under its own name because
    the intent differs: this is the deliberate first load, and the operator
    should see what it is about to request before it runs.
    """
    from smart_agri.config import get_settings as _settings

    open_meteo = _settings().open_meteo
    typer.echo(
        f"backfilling weather from {open_meteo.backfill_start}, "
        f"archive lag {open_meteo.archive_lag_days}d, "
        f"rate limit {open_meteo.max_rps}/s"
    )
    weather(logical_date)


@app.command()
def seed(
    profile: Annotated[
        str, typer.Option("--profile", help="Generator profile: small, medium or large.")
    ] = "small",
    seed_value: Annotated[
        int | None, typer.Option("--seed", help="Override the profile's RNG seed.")
    ] = None,
    keep: Annotated[
        bool,
        typer.Option("--keep", help="Append instead of truncating the tables first."),
    ] = False,
) -> None:
    """Generate a synthetic dataset and load it into Postgres."""
    from smart_agri.generator.config import get_profile
    from smart_agri.generator.seeder import DatasetSeeder
    from smart_agri.io import PostgresSource

    try:
        config = get_profile(profile)
    except KeyError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from None

    if seed_value is not None:
        config = config.model_copy(update={"seed": seed_value})

    typer.echo(
        f"profile={profile} farms={config.n_farms} fields={config.n_fields} "
        f"sensors={config.n_sensors} machines={config.n_machines}\n"
        f"window={config.start_date}..{config.end_date} "
        f"(~{config.estimated_readings:,} sensor readings)"
    )

    seeder = DatasetSeeder(PostgresSource(get_settings().postgres), config)
    result = seeder.seed(truncate=not keep)

    typer.echo("\nseeded:")
    typer.echo(result.summary())


@app.command(name="init-clickhouse")
def init_clickhouse(
    ddl_dir: Annotated[
        str, typer.Option("--ddl-dir", help="Directory of .sql files to apply, in name order.")
    ] = "/opt/clickhouse/ddl",
) -> None:
    """Apply the ClickHouse schema.

    Every statement is written to be idempotent (`CREATE ... IF NOT EXISTS`), so
    this is safe to re-run on every deploy.
    """
    from pathlib import Path

    from smart_agri.io import ClickHouseSink

    root = Path(ddl_dir)
    if not root.is_dir():
        typer.echo(f"ddl directory not found: {root}", err=True)
        raise typer.Exit(code=2)

    files = sorted(root.rglob("*.sql"))
    if not files:
        typer.echo(f"no .sql files under {root}", err=True)
        raise typer.Exit(code=2)

    with ClickHouseSink(get_settings().clickhouse) as sink:
        for path in files:
            count = sink.execute_script(path.read_text())
            typer.echo(f"{path.name}: {count} statement(s)")

    typer.echo(f"\napplied {len(files)} file(s).")


if __name__ == "__main__":  # pragma: no cover
    sys.exit(app())
