# smart-agri-platform

A self-hosted batch analytics platform for smart agriculture. Farm operations,
IoT soil sensors, machinery fleet telemetry, satellite-derived vegetation
indices and real weather land in a Parquet lake on HDFS, are modelled as a star
schema in ClickHouse, and are presented through Superset dashboards.

```
                    ┌──────────────────┐
  Synthetic  ─────► │   PostgreSQL     │  OLTP source (Liquibase-managed)
  generator         │   + etl_control  │  watermarks, run log
                    └────────┬─────────┘
                             │ full snapshot (dims) / incremental (facts)
                             ▼
  Open-Meteo API ──►  ┌─────────────────────────────────────┐
                      │        HDFS  (Parquet)              │
                      │  bronze/  raw, as-extracted         │
                      │  silver/  typed, validated,         │
                      │           conformed                 │
                      │  gold/    business aggregates       │
                      └────────┬────────────────────────────┘
                               │  registered as external tables
                               ▼  in Hive Metastore
                      ┌─────────────────────┐
                      │     ClickHouse      │  dims + facts + MVs
                      └────────┬────────────┘
                               ▼
                      ┌─────────────────────┐
                      │  Apache Superset    │  4 dashboards, YAML-defined
                      └─────────────────────┘

  Airflow (Celery) orchestrates every step; each task is one DockerOperator
  container running the smart-agri-etl image with a CLI subcommand.
```

Analytics covers four domains: **field & crop health**, **irrigation & water**,
**machinery & fleet**, and **yield & economics**.

## Requirements

Runs on an **x86_64 Ubuntu VM** with ~30 GB RAM and 30 GB free disk. The Hadoop
images are published for amd64 only, so an arm64 host would emulate them —
`make preflight` checks this along with Docker socket access and port
availability.

## Quick start

```bash
make preflight       # verify the host can run the stack
make env             # create .env, generate secrets, detect UID/docker GID
make build           # build the ETL, Airflow and Superset images
make up-all          # start every profile
make doctor          # confirm all backing services are reachable
make urls            # print the web UIs
```

`make` on its own lists every target.

## Profiles

The stack is split so you can run only what you need:

| Target | Brings up |
|---|---|
| `make up-core` | Postgres, ClickHouse, HDFS (NameNode + DataNode), Hive Metastore |
| `make up-orchestration` | Airflow webserver, scheduler, Celery worker, triggerer, Redis |
| `make up-bi` | Superset |
| `make up-all` | everything |

`make down` stops containers and keeps data. `make clean` deletes the volumes —
including the lake and both databases — and asks for confirmation first.

## Repository layout

| Path | Contents |
|---|---|
| [etl/](etl/) | The Python ETL application (see its own [README](etl/README.md)) |
| [airflow/dags/](airflow/dags/) | DAGs and the shared `etl_task` DockerOperator factory |
| [airflow/tests/](airflow/tests/) | DAG integrity tests, run inside the Airflow container |
| [docker/](docker/) | Compose stack, Hadoop config, and the custom images |
| [liquibase/](liquibase/) | Versioned Postgres schema changelogs |
| [clickhouse/ddl/](clickhouse/ddl/) | Dimensions, facts and materialized views |
| [superset/assets/](superset/assets/) | Dashboards, charts and datasets as YAML |
| [scripts/](scripts/) | Host-level helper scripts |
| [docs/](docs/) | Architecture notes and decision records |

## Development

```bash
make install         # install the ETL app and dev dependencies
make fmt             # format and auto-fix
make check           # lint + strict types + unit tests with coverage gate
make test-dags       # DAG integrity tests (needs make up-orchestration)
make test-integration  # integration tests against the live stack
```

Integration tests run **inside** a container on the platform network, not on the
host: a WebHDFS write is redirected to `datanode:9864`, a hostname that only
resolves there.

## Key decisions

- **WebHDFS, not libhdfs** — the ETL image stays JVM-free, which matters because
  a container starts per Airflow task.
- **ClickHouse is loaded by the ETL app**, not by ClickHouse's `hdfs()` table
  function, which depends on an optional `libhdfs3` build that is not reliably
  present in the official images.
- **Airflow never imports the ETL package.** It launches the image with a
  command vector, so orchestration and transformation stay independently
  deployable and testable.
- **Parquet now, Iceberg later.** The zone layout and Metastore registration are
  designed so the migration is a storage-layer swap rather than a rewrite.

See [docs/architecture.md](docs/architecture.md) for the reasoning in full.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 1 | Repo foundation and infrastructure | **done** |
| 2 | Thin end-to-end slice: soil sensor readings | next |
| 3 | Full OLTP schema and data generator | |
| 4 | Weather ingestion (Open-Meteo) | |
| 5 | Bronze and Silver for all domains | |
| 6 | Gold layer and full ClickHouse star schema | |
| 7 | Superset dashboards as code | |
| 8 | CI, quality gates, documentation | |

Deferred: Iceberg migration, Spark, and ML (yield forecasting, irrigation-need
prediction, anomaly detection).
