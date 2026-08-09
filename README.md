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
make demo            # migrate, seed, run the whole slice, import dashboards
make urls            # print the web UIs
```

`make` on its own lists every target.
[docs/runbook.md](docs/runbook.md) is the whole path from a clone to four
working dashboards, with what each step should print.

## The soil-sensor slice

`make demo` runs the Phase 2 vertical slice end to end. Individually:

```bash
make migrate                     # Liquibase changelogs -> Postgres
make init-clickhouse             # ClickHouse tables, views, materialized views
make seed PROFILE=small          # synthetic farms, fields, sensors, readings
make run-all                     # Bronze -> Silver -> Gold -> ClickHouse
make weather                     # Open-Meteo -> lake -> ClickHouse
make lake                        # the remaining domains into Bronze and Silver
make analytics                   # the Gold marts and the whole star schema
make superset-import             # dashboards, charts and datasets from YAML
```

`PROFILE` is `small` (5 farms, ~186k rows across 15 tables), `medium` (a
larger estate at hourly resolution) or `large` (three seasons). The generator is
deterministic: the same profile and seed reproduce the same dataset everywhere.

The same pipelines run in Airflow as the `soil_sensor_daily` DAG — fourteen
`DockerOperator` tasks in four stages. To run one by hand:

```bash
make pipelines                                    # list them
make run PIPELINE=silver.dim_farm DATE=2026-08-01
```

## Weather

Weather is the platform's only external data source. `make weather` fetches it
for every farm and reloads ClickHouse; Airflow runs the same stages as
`weather_daily` (05:30 UTC) and `weather_backfill` (manual).

```bash
make weather-backfill   # establish the full history, once
make weather            # keep it current
```

Two endpoints are used because neither covers the whole timeline: the archive
holds measurements but lags real time by about a week, while the forecast
endpoint reaches a fortnight either side of today. Silver merges them and lets
the **measurement win** wherever both cover a date — a chart correlating
rainfall with observed soil moisture must not compare against a prediction.

Gold derives what the dashboards actually ask for: growing-degree days, rolling
7- and 30-day rainfall, and the water balance between what fell and what
evaporated. The **Irrigation & Water** dashboard plots real rainfall against
measured soil moisture — the relationship should be visible in the days after
rain.

The free tier is genuinely rate limited, so the client paces itself, retries
throttling and transient faults with exponential backoff, and caches responses.
Nothing is retried that will stay broken: a 400 fails immediately rather than
burning quota.

## The analytics marts

Every domain lands in the lake, then one Gold mart per dashboard is built and
the whole star schema is loaded:

```bash
make lake        # Bronze + Silver for reference, operations, machinery, imagery
make analytics   # the four marts, then every dimension, fact and aggregate
```

| Mart | Grain | The question it answers |
|---|---|---|
| `agg_field_crop_health_daily` | field × observation | How is the canopy developing, for the crop actually in the ground? |
| `agg_field_irrigation_daily` | field × day | Did water supplied meet what the crop demanded? |
| `agg_machine_daily` | machine × day | What did each machine do, burn and cost? |
| `agg_planting_economics` | planting | What did the cycle return, on what water? |

Each carries its dimension attributes on the row, so a chart is one scan with no
joins. `clickhouse/ddl/views/` adds the rollups and current-state views — the
questions that would otherwise be a `GROUP BY` rebuilt slightly differently in
every chart, which is how two tiles on one dashboard come to disagree.

Two details are load-bearing:

- **Irrigation is built on the weather spine, not on the irrigation events.**
  Aggregating events alone would produce rows only for days something was
  applied — and the days that matter most are the dry ones where nothing was.
- **A mart is cross-domain**, so `analytics_daily` runs after the lake DAGs
  rather than inside them, and fails with the missing upstream named if the
  weather Gold for its date has not been built.

`make validate-ddl` applies the whole schema to a throwaway ClickHouse and
queries every view, catching SQL that no column-level test can.

## Dashboards

Four dashboards, one per analytics domain, defined entirely as YAML in
[superset/assets/](superset/assets/) — 8 datasets, 21 charts and the filters and
layout that go with them. Nothing is configured by hand in the UI.

```bash
make superset-import     # datasets, charts and dashboards from YAML
make verify-dashboards   # run every chart and report whether it returns data
make superset-export     # pull UI edits back into the tree, then review the diff
```

| Dashboard | Reads | Shows |
|---|---|---|
| Field & Crop Health | soil daily + crop health | moisture, stress, and the canopy curve against each crop's own peak |
| Irrigation & Water | water daily + irrigation daily | rainfall against measured moisture, and how much of the water was paid for |
| Machinery & Fleet | machine daily + utilisation | working/idle/parked/down days, fuel per hectare, faults |
| Yield & Economics | planting economics + cost breakdown | margin per hectare, cost structure, yield against water received |

The workflow is export-and-review, not edit-in-place: tweak a chart in the UI,
run `make superset-export`, and commit the diff. What is committed is what a
fresh stack reproduces.

Two checks guard it, because a dashboard is a web of UUID references that
Superset resolves only at import:

- `tests/unit/test_superset_assets.py` walks the whole graph statically — every
  chart a dashboard places exists, every column and metric a chart names is
  declared on its dataset, and every dataset column and metric expression
  resolves against the ClickHouse DDL. It runs in `make check`.
- `make verify-dashboards` runs each chart through Superset's own query
  pipeline and reports its row count, which is the only way to learn that a
  chart imports cleanly and then renders nothing.

## The generated dataset

Generated farms sit in real agricultural regions of **North Africa** (Nile
Delta, Gharb, Cap Bon) and **West Africa** (Kano, Ashanti, Sine-Saloum), each
with its own rainfall pattern and crop mix.

Every profile spans a full year, so each crop cycle completes and the yield and
economics tables are populated. The data is modelled rather than randomised —
the relationships below are asserted by
[test_agronomy_model.py](etl/tests/unit/test_agronomy_model.py):

- **Crops are sown into seasons they can finish.** A candidate is only planted
  if the season banks 85–180% of its degree-day requirement, so cotton goes in
  in May and durum wheat in November.
- **Irrigation covers what rain does not** — and deliberately falls short for
  some fields, which is what makes water genuinely limiting.
- **Soil moisture rises after watering and decays over ~5 days**, so irrigation
  is visible in the sensor series rather than sitting in an unrelated table.
- **NDVI traces canopy development**: bare soil at sowing, a peak around 60% of
  the cycle, then senescence. Perennials keep an evergreen baseline.
- **Yield follows water received and degree-days accumulated**, so the wettest
  half of plantings out-yields the driest.
- **Machines work the crop calendar** — a combine runs on the day a field is
  harvested — and emit telemetry densely while working, sparsely while parked.
- **Costs are derived from what was consumed**: actual inputs, irrigation
  energy and machine fuel, not drawn independently.

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
| [clickhouse/ddl/](clickhouse/ddl/) | Dimensions, facts, aggregates and views |
| [superset/assets/](superset/assets/) | Dashboards, charts and datasets as YAML |
| [scripts/](scripts/) | Host-level helper scripts, including [vm.sh](scripts/vm.sh) |
| [docs/](docs/) | [Runbook](docs/runbook.md), [architecture notes](docs/architecture.md), [how to add a dataset](docs/adding-a-dataset.md) |

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

[docs/adding-a-dataset.md](docs/adding-a-dataset.md) is the end-to-end checklist
for taking a new source table from Postgres to the warehouse — most of it is
declaration rather than code.

### The verification VM

The stack is verified on an x86_64 Ubuntu VM, never on a laptop — the Hadoop
images are amd64-only. [scripts/vm.sh](scripts/vm.sh) is the one entry point:

```bash
cp .env.vm.example .env.vm   # fill in host, user and key; gitignored
scripts/vm.sh status         # what is running
scripts/vm.sh make doctor    # pull, rebuild the ETL image, run a target
scripts/vm.sh ch 'SELECT count(*) FROM agg_field_soil_daily'
scripts/vm.sh logs namenode 60
```

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

| Phase | Scope | Status | Exit criterion, as measured on the VM |
|---|---|---|---|
| 1 | Repo foundation and infrastructure | **done** | `make doctor` — 4/4 services healthy |
| 2 | Thin end-to-end slice: soil sensor readings | **done** | 166,896 readings → ClickHouse in 7 s, 0 quarantined |
| 3 | Full OLTP schema and data generator | **done** | `make seed` — 15 tables, 180,580 rows in 11 s |
| 4 | Weather ingestion (Open-Meteo) | **done** | 1,825 archive rows; soil moisture rises with rainfall |
| 5 | Bronze and Silver for all domains | **done** | 21 Bronze + 19 Silver datasets; re-runs deduplicate to identical counts; 39 tables in Hive |
| 6 | Gold layer and full ClickHouse star schema | **done** | 27 tables + 15 views; every dashboard question answered by one query |
| 7 | Superset dashboards as code | **done** | Four dashboards reproduced from an empty stack |
| 8 | CI, quality gates, documentation | **done** | 8 CI jobs green from a cold clone; [runbook](docs/runbook.md) from clone to dashboards |

Deferred: Iceberg migration, Spark, and ML (yield forecasting, irrigation-need
prediction, anomaly detection).

### Verification status

Phases 1–8 have been **run on the target VM**, not just built and unit-tested.
The full stack — HDFS, Hive Metastore, ClickHouse, Airflow on Celery, Superset —
comes up, the pipelines move real data end to end, the weather chain calls the
live Open-Meteo API, and all four dashboards query ClickHouse.

Also verified there: 22/22 integration tests, all nine DAGs registered with no
import errors, and both the fourteen-task `soil_sensor_daily` and the
twenty-two-task `analytics_daily` DAG green through `DockerOperator`.

Phase 5 specifically, measured on the VM:

- Every source table lands in Bronze and Silver across all six domains.
- **Idempotent.** A re-run with the watermark caught up extracts nothing. A
  re-run after the source is fully rewritten re-extracts everything, and Silver
  deduplicates it back to identical row counts — 21,979 rows in, 10,988 out for
  telemetry. Every ingest partition holds each business key exactly once.
- **Quarantine works.** Three telemetry rows corrupted in Postgres passed Bronze
  as-extracted, were rejected at the Silver boundary, and landed in
  `/lake/quarantine/…/rejected.parquet` beside a `report.parquet` naming both
  failing checks and a 0.000273 rejection rate. Silver's maximum fuel level came
  back to 98.69% and the rows never reached the warehouse.
- **39 external tables** registered in the Hive Metastore, generated from the
  same pandera contracts the pipelines validate against, and queryable —
  `SELECT count(*)` through HiveServer2 matches Polars exactly.

Phase 6, measured on the VM:

- **27 tables and 15 views** in ClickHouse: 9 dimensions, 9 facts, 7 aggregates,
  and the views completing each dashboard. `analytics_daily` runs 22 tasks
  through `DockerOperator`, green.
- **Every dashboard question is one query.** The marts carry their dimension
  attributes, so none of the four dashboards needs a join:
  - Canopy follows the cycle — mean NDVI 0.344 bare, 0.571 developing, 0.742
    peak, 0.427 senescing.
  - Irrigation dependence tracks climate — 92.3% of water supplied in the Nile
    Delta is irrigation, against 4.6% in the Ashanti Belt.
  - Soil moisture responds to supply — 23.79% on deficit days, 30.37% on
    surplus days, with dry-stressed days falling from 902 to 28.
  - The wetter half of plantings yields 10.04 t/ha against 3.02 for the drier
    half.
- **Idempotent.** A second full `make analytics` reproduced every count and
  every total exactly — 1,274 crop-health rows, 7,560 irrigation, 4,392
  machine-days, 37 plantings, $2,319,894 of cost.
- 22/22 integration tests and 10/10 DAG integrity tests.

Phase 8, measured on a **cold clone** rather than on the VM, because that is
what its criterion is about:

- `git clone` into an empty directory, then `make install && make check &&
  make validate-ddl && make check-docs && make validate-compose` — all green,
  619 tests, 91.92% coverage, no local state involved.
- **8 CI jobs green**, including two new gates: `ClickHouse DDL` runs
  `make validate-ddl` against a throwaway server, and `Documentation` runs
  `make check-docs`.
- `scripts/*.py` is now linted. It was the one unchecked corner of the repo —
  `ruff check .` runs inside `etl/` and never saw it.
- Every action is off Node 20. `astral-sh/setup-uv` publishes a `v9.0.0`
  release but no floating `v9` tag, so it is pinned to `v7`; CI found that, not
  review.
- [docs/runbook.md](docs/runbook.md) is the documented path: clone to four
  dashboards, with the output each step should print and what it means when it
  does not.

Phase 7, measured on the VM — and measured from **nothing**, because the stack
was wiped between Phase 6 and this run:

- `make migrate && make init-clickhouse && make seed && make run-all &&
  make weather-backfill && make lake && make analytics` rebuilt the whole
  platform from empty volumes, reproducing **every** count exactly: 180,580
  seeded rows, 1,890 weather rows, 1,274 crop-health rows, 7,560 irrigation,
  4,392 machine-days, 37 plantings.
- `make superset-import` then produced **four dashboards** — Field & Crop
  Health (7 charts), Irrigation & Water (6), Machinery & Fleet (4), Yield &
  Economics (4) — over 8 datasets, from committed YAML alone.
- **All 21 charts return data.** `make verify-dashboards` runs each one through
  Superset's own query pipeline and reports its row count; nothing was empty and
  nothing failed.
- The numbers survive the round trip: the irrigation-dependence chart renders
  92.3% for the Nile Delta against 4.6% for the Ashanti Belt, the same figures
  the warehouse gives directly.

That run found **three bugs that 598 unit tests, strict mypy and the DDL
contract test had all passed** — every one of them in SQL:

| Bug | Why nothing caught it |
|---|---|
| Four views rejected outright: `sum(x) AS x` then `sum(x)` again nests an aggregate | The column-list test compares names, and no test executed the SQL |
| `v_machine_attention` was always empty while 20 faults stood open | It keyed on the fault landing on the machine's *last active* day |
| An edited view definition silently never applied | `CREATE VIEW IF NOT EXISTS` is a no-op once the view exists |

`make validate-ddl` now applies the whole schema to a throwaway ClickHouse and
queries every view, which is what would have caught the first and third on the
laptop. The earlier phases' bug table:

| Bug | Why nothing caught it |
|---|---|
| NameNode never formatted | The sentinel checked for a directory the volume mount creates |
| ClickHouse bound to `127.0.0.1` | Its healthcheck ran inside the container and passed |
| Hive Metastore had no JDBC driver | The image ships none, and `IS_RESUME` skipped schema init |
| Seeder wrote non-existent columns | The test fake accepted any column |
| One machine working two fields at once | Only a unique constraint could reject it |
| Every Postgres extract failed | ADBC returns `NUMERIC` as an Arrow type Polars refuses |
| Airflow had "no username" | Overriding the entrypoint skipped its `/etc/passwd` fixup |

Where a unit test could reasonably have caught one, it now does — the seeder's
column guard parses the Liquibase changelogs and would have flagged the fourth.
The rest are why the plan treats a real run as each phase's exit criterion
rather than a formality.
