# Architecture

Reasoning behind the choices in this platform. Read this before changing a
pinned version or swapping a component.

## Data flow

```
PostgreSQL ──► /lake/bronze ──► /lake/silver ──► /lake/gold ──► ClickHouse ──► Superset
   (OLTP)        raw, as-        typed,           business       star schema     dashboards
                 extracted       validated,       aggregates     + MVs
                                 conformed
```

Batch only. No CDC, no streaming: the analytics questions are daily-grain
agronomic and economic ones, and a Kafka/Debezium tier would add operational
weight that nothing in the requirements pays for.

## Decisions

### WebHDFS instead of libhdfs

The ETL application reaches HDFS over the WebHDFS REST API through `fsspec`,
not through `pyarrow.fs.HadoopFileSystem`.

`HadoopFileSystem` requires a JVM and `libhdfs` inside the image. Since Airflow
runs **one container per task**, that would mean paying JVM startup on every
task and carrying a much larger image. WebHDFS has lower raw throughput, which
is irrelevant at the volumes this platform handles.

Two settings in [docker/hadoop/hadoop.env](../docker/hadoop/hadoop.env) make it
work across the Compose network:

```
HDFS-SITE.XML_dfs.client.use.datanode.hostname=true
HDFS-SITE.XML_dfs.datanode.use.datanode.hostname=true
```

A WebHDFS write is a two-step exchange: the NameNode answers `307` with a
redirect to a DataNode, and the client follows it. Without these settings that
redirect carries a container IP the client cannot route to. This is covered by
`test_write_then_read_round_trip` in the integration suite.

**If throughput ever becomes the bottleneck**, the change is contained: add a
JVM and `libhdfs` to the ETL image and reimplement `io/HdfsStore`. Nothing else
touches HDFS directly.

### ClickHouse is loaded by the ETL app

Gold Parquet is read from HDFS into Polars and inserted through
`clickhouse-connect`. ClickHouse's `hdfs()` table function is deliberately
unused: it depends on an optional `libhdfs3` build that is not reliably present
in the official images, and it would move transformation logic out of the tested
Python codebase into SQL strings.

### Airflow never imports the ETL package

Every task is a `DockerOperator` running `smart-agri <subcommand>`. Airflow
holds no ETL dependencies, and the ETL app holds no Airflow dependencies, so
each is testable in isolation and the ETL image is the single deployment unit.

The shared factory in
[airflow/dags/common/etl_task.py](../airflow/dags/common/etl_task.py) fixes the
settings that are easy to get wrong:

- `network_mode` — task containers must join `agri-net` or service hostnames
  will not resolve.
- `mount_tmp_dir=False` — DockerOperator otherwise bind-mounts a host temp
  directory that does not exist inside the worker, and the task dies before the
  command runs.
- `force_pull=False` — the image is built locally and never pushed.

The Airflow worker mounts `/var/run/docker.sock` and is added to the host's
`docker` group via `DOCKER_GID`. `make preflight` checks this.

### Hive Metastore earns its place later

The Metastore registers the lake zones as external Parquet tables. Nothing in
the current pipeline computes through it — Polars reads Parquet paths directly.
It is here as the catalog and as the direct on-ramp to Iceberg, which is why the
zone layout is registered from Phase 5 onward rather than after the migration
starts.

### Parquet now, Iceberg later

Iceberg buys snapshot isolation, schema evolution and time travel. None of those
are needed to get the first dashboards working, and all of them are easier to
adopt against a lake that already has stable zone boundaries and a populated
Metastore. The migration is a storage-layer swap.

### Polars and Arrow, not Spark

At demo scale a single node handles the full transformation comfortably, and
Polars keeps the aggregation logic as testable Python rather than distributed
job configuration. Spark becomes worth its operational cost only if volumes
outgrow single-node compute.

### CeleryExecutor

Heavier than LocalExecutor by a Redis instance and a worker container, and
chosen deliberately: it matches a production topology, so task isolation and
queue behaviour are exercised during development rather than discovered later.

### Incremental Bronze appends; it never replaces

The snapshot pipelines clear their partition before writing, because they
re-extract the whole table every run. `BronzeIncrementalPipeline` does the
opposite, and the distinction is load-bearing.

A watermark extract returns only rows newer than the last successful run. If
that result were written over the partition, two things would break:

1. A re-run after the watermark has caught up extracts **zero** rows. Writing
   that empty frame over a populated partition deletes the readings already
   landed — and Silver, Gold and ClickHouse all empty on the following run.
2. Even a non-empty re-run would drop the earlier batches, which the watermark
   guarantees will never be extracted again.

So each batch lands as its own file inside the partition, named after its
high-water mark, and an empty batch writes nothing at all. Silver therefore
reads the Bronze partition as a *directory*, not a single file.

`test_empty_batch_does_not_overwrite_a_populated_partition` pins this.

### Phase 2 loads are full-refresh

Gold recomputes the whole aggregate each run, and the ClickHouse loads replace
their tables rather than swapping a partition. At demo volumes this costs
nothing and removes an entire class of partial-update bugs while the pipeline is
still being proven end to end.

`ClickHouseSink.delete_partition` already exists for Phase 6, where volumes make
partition-level replacement worth the added complexity.

### Aggregation lives in Polars, not SQL

`agg_field_soil_daily` is built by the Gold pipeline and inserted as a plain
table, rather than being defined as a ClickHouse materialized view over the
fact. The moisture-stress classification and the daily rollups are therefore
unit-tested against fixture frames instead of living in a SQL string no test can
reach.

The one materialized view that does exist — `mv_field_soil_weekly` — only rolls
the already-tested daily aggregate up to weeks.

### The reference catalogue lives twice, and is checked

Regions, soil classes and crops exist as Python constants *and* as INSERT
statements in the Liquibase changelogs. The generator needs them without a
database; the database needs them before `farm.region` can carry a foreign key
onto `agri.region`.

That duplication is guarded by `tests/unit/test_reference_catalogue.py`, which
parses the changelogs and compares them field by field. Without it, a base
temperature edited on one side would skew every yield in the dataset while every
other test still passed — the same failure mode the DAG/registry sync test
exists to prevent.

### Generation order is the design

`DatasetGenerator.build()` runs its sub-generators in dependency order, and the
order is what makes the data internally consistent rather than fifteen
independent random tables:

```
plantings ──► irrigation ──► machine work ──► harvest ──► costs
     │             │                              ▲          ▲
     └─► imagery   └──────► sensor readings       │          │
                            (moisture boost)      │          │
                     water received ──────────────┘          │
                     fuel + inputs + energy ─────────────────┘
```

A planting fixes the crop, the calendar and the water demand. Everything
downstream derives from it, so water-use efficiency, cost per hectare and yield
are related quantities instead of three unrelated series that happen to share a
field id.

### Weather: two endpoints, measurement wins

No single Open-Meteo endpoint covers the timeline the platform needs. The
archive holds measurements but lags real time by roughly a week; the forecast
endpoint reaches a fortnight either side of today, but its recent past is model
output rather than observation.

Both are ingested, into separate Bronze datasets, and Silver merges them with
the archive taking precedence for any date both cover. Every row keeps a
`source` and an `is_actual` flag.

That distinction is not bookkeeping. The dashboard correlating rainfall against
measured soil moisture filters to actuals, because comparing an observation with
a prediction would show a relationship partly produced by the forecast model
rather than by the field. `v_field_water_daily` applies the filter once so every
chart asks the question the same way.

The windows deliberately **overlap**: the forecast's `past_days` reaches back
past where the archive stops. Without that overlap the series would carry a
permanent hole a few days wide, right at the point most dashboards look.

### The weather client is the only place that needs real resilience

Everything else in the platform fails for reasons inside the stack. The weather
client fails for reasons outside it, so it carries what nothing else needs:
client-side rate limiting, exponential backoff, and response caching.

The retry policy distinguishes *retryable* from *terminal*. A 429 or 5xx is
tried again — a throttled request that is dropped would leave that farm's
weather missing while the pipeline reported success. A 400 is not retried: it
will stay wrong, and retrying only consumes the quota the limiter exists to
protect. Open-Meteo also reports some failures in the body with an HTTP 200, so
the body is checked as well as the status.

### A mart is cross-domain, so it runs last

The Phase 5 domains each stand alone: `operations` re-extracts the dimensions its
joins need rather than assuming `reference` ran, so any domain can be scheduled,
retried or backfilled on its own.

That property cannot survive contact with the Gold marts.
`field_irrigation_daily` needs weather *and* operations; `planting_economics`
needs operations, weather and imagery. A domain DAG that built either would have
to re-run half the platform, or read whatever another DAG happened to leave in
the lake — which is the same thing as having no dependency at all, except that
it fails silently.

So the marts and the entire warehouse load live in one `analytics` domain that
runs after the others. The coupling is real, and putting it in one place makes
it visible instead of implicit.

Where a mart needs an upstream Gold partition, it fails with the missing
pipeline named rather than degrading. A water balance without rainfall is not a
partial answer; it is a wrong one.

### The irrigation mart is built on the weather spine

`field_irrigation_daily` starts from `field_weather_daily` and left-joins the
irrigation events, rather than aggregating the events and joining weather on.

Aggregating events first produces rows only for days something was applied. But
the days an irrigation dashboard exists to surface are the dry ones where
*nothing* was — and those rows would simply not exist, so the deficit would
appear as missing data rather than as a deficit.

### Classification lives in Polars, never in SQL

Every threshold the dashboards filter on — canopy stage, vigour, supply status,
machine utilisation — is a module constant in `pipelines/gold_marts.py` with a
test pinning it, not a `CASE WHEN` in a view.

The reason is the same one Phase 2 gave for `agg_field_soil_daily`, and Phase 6
supplied the evidence: four ClickHouse views were rejected outright on first
deploy for nesting an aggregate inside a `SELECT` alias, and a fifth returned
nothing for a year because it keyed on the wrong day. None of that was reachable
by a unit test. What SQL *does* keep is the shape of a rollup, where being wrong
is visible; what it must not keep is a rule that decides whether a field is
stressed.

### The warehouse DDL is hand-written, and therefore guarded twice

`metastore.py` generates Hive DDL from the pandera contracts, so the lake's
external tables cannot drift. ClickHouse does not get the same treatment: its
DDL carries engines, partition keys, sort orders and `LowCardinality`
annotations that no contract describes, and generating it would mean encoding
all of that in Python to avoid writing it in SQL.

The duplication is accepted and then checked from both sides:

- `test_clickhouse_ddl.py` parses the SQL and compares every table's columns
  with the contract it is loaded from.
- `make validate-ddl` applies the whole schema to a throwaway server and queries
  every view, because a column list can be correct while the SQL is invalid.

### Dashboards are exported, reviewed and committed — not edited in place

Superset assets are YAML in `superset/assets/`, imported by `make
superset-import`. Edits happen in the UI, then `make superset-export` pulls them
back and the diff is reviewed like any other change.

A dashboard is a web of UUID references that Superset resolves only at import,
where a broken one is either a silent no-op or a traceback inside the importer.
So the graph is walked statically instead: `test_superset_assets.py` checks that
every placed chart exists, every column and metric a chart names is declared on
its dataset, and every dataset column and metric expression resolves against the
ClickHouse DDL. `make verify-dashboards` then runs each chart through Superset's
own query pipeline, which is the only way to learn that a chart imports cleanly
and renders nothing.

That pair found a filter keyed on `field_name` — a label that repeats across
farms, so it had been quietly averaging two fields on two continents.

## Image pinning

Every tag is pinned in `.env`. Two pins are load-bearing:

| Pin | Why |
|---|---|
| `apache/hadoop:3.4.3` | amd64-only. On arm64 it runs under emulation — hence the `make preflight` architecture check. |
| `typer==0.27.0` | Versions below 0.16 call click's `Parameter.make_metavar()` without a context, which crashes `--help` against click ≥8.2 — and `--help` is the ETL image's default `CMD`. |

The Superset image's `clickhouse-connect` version should be kept aligned with
the ClickHouse server tag when either is upgraded.

## Operational notes

**NameNode formatting.** The `ENSURE_NAMENODE_DIR` environment variable makes
the image format only when `/hadoop/dfs/name` is absent, so a populated volume
is never reformatted. `make clean` removes that volume and the lake with it,
which is why it requires typed confirmation.

**HDFS permissions** are disabled (`dfs.permissions.enabled=false`). There is no
Kerberos here and ownership adds nothing. Revisit before any deployment that is
not a sandbox.

**Compose profiles** exist so a laptop-scale host can run subsets. Shared
services belong to several profiles — ClickHouse is in `core`, `orchestration`
and `bi`, because Superset needs it but does not need HDFS.
