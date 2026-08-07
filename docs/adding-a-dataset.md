# Adding a dataset

The end-to-end checklist for taking one new source table from Postgres to the
warehouse. Most of it is declaration rather than code — the registry builds
Bronze and Silver pipelines from specs by comprehension, so the pipeline classes
themselves are almost never the thing you write.

Steps 1–8 are always needed. Steps 9–12 depend on where the dataset has to
reach. The order matters: each step's tests depend on the one before it.

## 1. Postgres schema

`liquibase/changelog/changes/0NN-<name>.xml` — a new `<createTable
schemaName="agri">`. Include `created_at` and `updated_at` as `TIMESTAMPTZ` with
defaults; the incremental extract watermarks on `updated_at` and cannot work
without it. Add foreign keys and indexes here, not later.

Changesets are append-only. Never edit one that has been applied — write
another.

Then add the `<include file="changes/0NN-<name>.xml"/>` line to
`liquibase/changelog/db.changelog-master.xml`.

## 2. Domain model and generator

- The entity in `etl/src/smart_agri/domain/models.py` or `domain/operations.py`.
- Generation logic in a `etl/src/smart_agri/generator/` module.
- Wire it into the `Dataset` built by `generator/generator.py`.

`DatasetGenerator.build()` runs its sub-generators in dependency order, and that
order is what makes the data internally consistent rather than fifteen unrelated
random tables — see `docs/architecture.md`. Insert the new generator at the
point where its inputs already exist.

## 3. Seeder

In `etl/src/smart_agri/generator/seeder.py`:

- Add the table to `TABLES_IN_DEPENDENCY_ORDER`, child-first.
- Write the `_insert_*` method.
- Call it from `seed()` in foreign-key order.

Then add the table's identity column to `FakePostgresSource._identity_column` in
`etl/tests/fakes.py`. **A missing entry breaks surrogate-key resolution
silently** — no error, wrong ids.

## 4. Contracts

In `etl/src/smart_agri/contracts/schemas.py`:

- `BRONZE_<NAME>` — `strict=False`, presence and coarse types only. Bronze is
  as-extracted and must not reject anything.
- `SILVER_<DIM|FACT>_<NAME>` — `strict=True`, with ranges, `unique=True` where
  it applies, and `utc_datetime()` on timestamps. This is the boundary that
  fills `/lake/quarantine`, so it is where the real assertions go.

Then in `etl/src/smart_agri/contracts/__init__.py`, add **both** names to the
import block **and** to `__all__`. `no_implicit_reexport` is on: omitting the
`__all__` entry fails type checking at the import site, which is confusing to
diagnose.

## 5. Bronze spec

In `etl/src/smart_agri/pipelines/bronze.py`, import the Bronze schema and append
one spec to the right tuple:

- `SnapshotSpec(dataset, source_table, schema)` → `SNAPSHOT_SPECS`, for
  dimensions and anything re-extracted whole each run.
- `IncrementalSpec(dataset, source_table, schema, watermark_column="updated_at")`
  → `INCREMENTAL_SPECS`, for facts.

The dataset name is the source table's leaf name (`farm`, `sensor_reading`).

Snapshot Bronze clears its partition before writing. Incremental Bronze
**appends and never replaces** — the distinction is load-bearing and explained
in `docs/architecture.md`.

## 6. Silver spec

In `etl/src/smart_agri/pipelines/silver_specs.py`, import the Silver schema,
declare a module-level `SilverSpec` and append it to `SILVER_SPECS` —
dimensions before facts.

```python
FACT_SENSOR_READING = SilverSpec(
    dataset="fact_sensor_reading",
    bronze="sensor_reading",
    schema=SILVER_FACT_SENSOR_READING,
    incremental=True,
    joins=(JoinSpec(dataset="sensor", on=("sensor_id",), columns=("field_id",)),),
    filters=(pl.col("status") != "decommissioned",),
    derive={"reading_date": pl.col("reading_ts").dt.date()},
    cast={"reading_id": pl.Int64, "field_id": pl.Int64},
    dedupe_on=("reading_id",),
)
```

Naming: `dim_*` or `fact_*`. `spec.incremental` must match the Bronze strategy.
Joins may only target snapshot datasets. Every fact needs `farm_id`, and every
fact with a timestamp needs a derived `_date` column —
`tests/unit/test_silver_specs.py` asserts all of this over every spec, so a
mistake here surfaces as a parametrized test failure naming your dataset.

`SilverPipeline` applies the spec in a fixed order: joins → rename → trim/lower/
upper → cast → derive → select in schema order → dedupe → sort. If a
transformation does not fit that order, it is not a spec — see step 9.

## 7. Registry and stages

`etl/src/smart_agri/pipelines/registry.py` needs **no edit** for Bronze and
Silver; the comprehensions pick both specs up from their tuples.

It does need the pipeline names adding to the right `*_STAGES` tuple, and to
`DOMAIN_STAGES` if this is a new domain. Every registered pipeline must appear
in some stage — `test_dag_pipeline_sync.py` enforces it.

## 8. The Airflow mirror

Make the identical change to the `*_STAGES` and `DOMAIN_STAGES` declarations in
`airflow/dags/common/pipelines.py`. This file is a deliberate duplicate: Airflow
must never import the ETL package, so the stage lists exist on both sides.
`test_dag_pipeline_sync.py` parses this file with `ast` and diffs it against the
registry, so the two must match exactly.

## 9. A DAG, if this is a new domain

Copy an existing per-domain DAG — `airflow/dags/operations_daily.py` is the
template — and change four things: `dag_id` (`<domain>_<cadence>`),
`description`, `tags` (`[domain, layer, "phase-N"]`), and the argument to
`build_domain_stages()`. Everything else is fixed: `catchup=False`,
`max_active_runs=1`, `owner="data-platform"`, `doc_md=__doc__`.

If the task needs a new environment variable, add it to `_FORWARDED_ENV_VARS` in
`airflow/dags/common/etl_task.py` or it will silently never reach the container.

## 10. Gold, if the dataset feeds a dashboard

Gold pipelines are hand-written `BasePipeline` subclasses, not specs — an
aggregate is not a sequence of column operations. Put it in
`etl/src/smart_agri/pipelines/gold.py`, give it a `GOLD_*` contract, and
register it by name in `registry.py`.

## 11. ClickHouse

DDL in `clickhouse/ddl/{dimensions,facts,views}/NNN_*.sql`, always
`CREATE TABLE IF NOT EXISTS` — `make init-clickhouse` is re-run routinely.

Then the load pipeline: a `DimLoadSpec(dataset, table)` appended to
`DIM_LOAD_SPECS` in `pipelines/load.py` for a plain dimension, or a bespoke
`Load*Pipeline` for anything else. Gold aggregates take an `agg_` table prefix.

## 12. Hive Metastore

Automatic. `metastore.py::table_specs()` walks the Bronze and Silver spec tuples
and generates the DDL from the pandera contracts. Run `make register-tables`.

The exception is a dataset that does not come from a spec — an API source like
weather — which must be hand-added alongside `_weather_tables()` and mirrored
into `test_metastore.py`.

## 13. Tests

- Fixture rows in `tests/unit/test_pipelines.py` (`_<name>_rows()`), the table
  in the `postgres` fixture, and the pipeline name in the `bronze_loaded` /
  `silver_loaded` fixture lists.
- Behaviour tests for anything the transform decides.

`test_silver_specs.py` and `test_dag_pipeline_sync.py` cover the new spec
automatically once it is declared.

## 14. Verify

```bash
make check                                  # lint, strict types, unit tests, coverage
make test-dags                              # DAG integrity, needs up-orchestration
```

Then on the VM, because nothing above proves it moves real data:

```bash
scripts/vm.sh sync
scripts/vm.sh make migrate
scripts/vm.sh make seed
scripts/vm.sh exec 'make run PIPELINE=bronze.<name> && make run PIPELINE=silver.<name>'
scripts/vm.sh make register-tables
scripts/vm.sh ch 'SELECT count(*) FROM <table>'
```

Run the pipeline **twice**. Idempotence is the property most likely to be wrong:
a snapshot re-run must produce identical counts, and an incremental re-run with
the watermark caught up must extract nothing and leave the partition intact.
