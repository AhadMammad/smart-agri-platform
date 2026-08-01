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
