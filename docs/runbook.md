# From a clone to four dashboards

The whole path, in order, with what each step should print. Roughly 25 minutes
on a cold machine, most of it image pulls.

If a step does not produce what is shown here, stop — the next one will fail
more confusingly than this one did.

## 0. The host

An **x86_64 Ubuntu VM with ~30 GB RAM and 30 GB free disk**. Not a laptop, and
not arm64: `apache/hadoop` publishes amd64 only, so an Apple-silicon machine
emulates HDFS and the stack becomes unusably slow rather than broken — which is
harder to diagnose.

```bash
git clone https://github.com/AhadMammad/smart-agri-platform.git
cd smart-agri-platform
make preflight
```

`preflight` checks the architecture, Docker socket access, the eight host ports
and free disk. It exits non-zero if any of those would bite later. Expect:

```
  [ok]   x86_64 — the Hadoop images are amd64-only and will run natively
  ...
```

An arm64 `[fail]` here is not advisory. Move to a supported host.

## 1. Configuration and images

```bash
make env      # writes .env: generated secrets, your UID, the host's docker GID
make build    # builds the ETL, Airflow, Hive and Superset images
```

`make env` is a no-op if `.env` already exists, so it is safe to re-run. It
detects `DOCKER_GID` from the host — the Airflow worker needs to be in that
group to launch `DockerOperator` containers.

`make build` takes the longest of any step here.

## 2. Start the platform

```bash
make up-all   # every profile; blocks until HDFS leaves safe mode
make doctor
```

`doctor` runs inside a task container on the platform network, so it tests what
the pipelines will actually experience rather than what the host can reach:

```
[PASS] postgres         postgres:5432/agri — PostgreSQL 16.14 …
[PASS] clickhouse       clickhouse:8123/agri_analytics — v24.8.…
[PASS] hdfs             namenode:9870 — zones: bronze, gold, quarantine, silver
[PASS] hive-metastore   hive-metastore:9083 — thrift port open

All 4 services healthy.
```

Anything other than 4/4 means stop and read `make logs SERVICE=<name>` on the
host — not in an agent session, where a follow-mode log never returns.

If you only need part of the platform, `make up-core`, `make up-orchestration`
and `make up-bi` bring up subsets.

## 3. Schema and data

```bash
make migrate           # 24 Liquibase changesets -> Postgres
make init-clickhouse   # 27 tables and 15 views
make seed              # PROFILE=small|medium|large, default small
```

`seed` is deterministic — the same profile and seed reproduce the same dataset
on any host. Expect `180,580` rows across 15 tables in about 11 seconds for
`small`.

## 4. Fill the lake and the warehouse

Order matters here, and only in one place: the marts need the weather Gold.

```bash
make run-all           # the soil-sensor slice, Bronze -> ClickHouse
make weather-backfill  # the full Open-Meteo history, once. Needs internet
make lake              # reference, operations, machinery, imagery -> Silver
make analytics         # the four Gold marts, then the whole star schema
```

`make analytics` fails with the missing upstream named if `make weather-backfill`
(or a later `make weather`) has not run for the same date. That is deliberate: a
water balance without rainfall is not a degraded mart, it is a wrong one.

Expect, from `make analytics`:

```
  gold.field_crop_health_daily: read=1289 written=1274 quarantined=0
  gold.field_irrigation_daily:  read=7560 written=7560 quarantined=0
  gold.machine_daily:           read=4392 written=4392 quarantined=0
  gold.planting_economics:      read=37   written=37   quarantined=0
```

`make demo` runs steps 3 and 4 together, plus the import below.

## 5. Dashboards

```bash
make superset-import    # datasets, charts and dashboards from committed YAML
make verify-dashboards  # run every chart, report its row count
make urls               # the web UIs
```

`verify-dashboards` is the proof, and it should end:

```
21 returning data, 0 empty, 0 failing
```

Then open Superset at the port `make urls` prints, log in with the credentials in
`.env`, and the four dashboards are under **Dashboards**:

| Dashboard | Answers |
|---|---|
| Field & Crop Health | soil moisture and stress, and the canopy curve per crop |
| Irrigation & Water | rainfall against measured moisture; irrigation dependence |
| Machinery & Fleet | utilisation, fuel per hectare, faults |
| Yield & Economics | margin per hectare, cost structure, yield against water |

## 6. Orchestration

The same pipelines run as DAGs. They ship paused except the healthcheck and the
soil-sensor slice:

```bash
make urls   # the Airflow URL and credentials
```

| DAG | Schedule | Does |
|---|---|---|
| `soil_sensor_daily` | `@daily` | the Phase 2 slice, 14 tasks |
| `reference_daily`, `operations_daily`, `machinery_daily`, `imagery_daily` | `@daily` | Bronze and Silver per domain |
| `weather_daily` | 05:30 UTC | Open-Meteo refresh |
| `weather_backfill` | manual | the full history |
| `analytics_daily` | 07:00 UTC | the marts and the warehouse load |

`analytics_daily` runs after the others because a mart is cross-domain. Every
Gold pipeline reads Silver at its own logical date, so triggering a DAG for a
date the lake has no partitions for fails — which is correct, and is what you
will see if you unpause a DAG and Airflow fires a catch-up run for yesterday.

## Checks

Run before committing; all of them are also CI jobs.

| Command | Checks |
|---|---|
| `make check` | ruff, strict mypy, unit tests, 80% coverage gate |
| `make validate-compose` | every Compose profile resolves |
| `make validate-ddl` | the ClickHouse DDL applies and every view queries |
| `make check-docs` | every documented target and link still resolves |
| `make test-dags` | DAG integrity, needs `make up-orchestration` |
| `make test-integration` | the integration suite, needs `make up-core` |

`make check` is exactly CI's `quality` job. The integration suite is not in CI —
the full stack is too heavy for a hosted runner, which is why every phase's exit
criterion is measured on the VM instead.

## When something is wrong

| Symptom | Cause |
|---|---|
| `make analytics` fails naming `gold.field_weather_daily` | weather has not run for that date — run `make weather` |
| A pipeline writes 0 rows and reports success | its Bronze watermark is caught up; that is idempotent, not broken |
| Rows in `/lake/quarantine/…` | Silver rejected them; `report.parquet` beside them names the failing check |
| A chart renders an error | run `make verify-dashboards`; a renamed mart column is the usual cause |
| A DAG task dies instantly | check `_FORWARDED_ENV_VARS` in `airflow/dags/common/etl_task.py` |

`make down` stops everything and keeps the data. `make clean` deletes the
volumes — the lake, both databases and Superset's metadata — and asks for a
typed confirmation first.
