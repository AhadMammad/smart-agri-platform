# Working in this repo

Operational notes for Claude Code. `README.md` is the narrative introduction and
`docs/architecture.md` holds the reasoning; this file holds the things that are
easy to get wrong.

## Two working directories

`make` runs from the repo root. Every Python tool runs against `etl/`:

```bash
make check                                   # from the root
uv --directory etl run pytest -q             # from the root, targeting etl/
```

`uv run` without `--directory etl` resolves the wrong project. If you `cd etl`
first, plain `uv run` works — but `make` will not.

## Commands

| Command | Does | Equivalent CI job |
|---|---|---|
| `make check` | lint + strict types + unit tests with the coverage gate | `quality` |
| `make lint` | `ruff check .` and `ruff format --check .` | part of `quality` |
| `make typecheck` | `mypy` (strict) | part of `quality` |
| `make test` | `pytest -m "not integration" --cov` | part of `quality` |
| `make fmt` | `ruff format .` then `ruff check --fix .` — **mutates files** | — |
| `make validate-compose` | `docker compose config -q` across all three profiles | `compose` |
| `make validate-ddl` | applies the ClickHouse DDL to a throwaway server, queries every view | `ddl` |
| `make check-docs` | every documented `make` target and relative link resolves | `docs` |
| `make test-dags` | DAG integrity tests inside the scheduler container | `dags` |
| `make test-integration` | integration suite on the platform network | not in CI |
| `make verify-dashboards` | runs every Superset chart, reports its row count | not in CI |
| `shellcheck scripts/*.sh docker/superset/bootstrap.sh` | shell lint | `shell` |

Python under `scripts/` is linted with the same ruff config as `etl/`, but is
outside that project, so `make check` does not reach it — CI checks it
separately. Run it by hand with
`uv --directory etl run ruff check --config pyproject.toml ../scripts`.

One test:

```bash
uv --directory etl run pytest tests/unit/test_pipelines.py::TestBronzeIncremental -x
```

`make check` is exactly what CI's `quality` job runs. If it passes locally and
fails in CI, the difference is `uv sync --frozen` — check `uv.lock` is committed.

## Never run these

They block forever in a non-interactive session, or destroy data:

- `make clean` — waits on a typed `yes`, then deletes the HDFS lake, Postgres
  and ClickHouse volumes.
- `make logs` — `docker compose logs -f`, follow mode, never exits.
- `make psql`, `make ch` — interactive shells waiting on stdin.
- `make vm-tunnel` / `scripts/vm.sh tunnel` — foreground SSH port-forward, never exits until Ctrl-C. For the user to run in their own terminal, not from a Claude Code session.

Use instead:

```bash
docker logs --tail 40 smart-agri-namenode-1
docker exec smart-agri-clickhouse-1 clickhouse-client \
  --user agri --password agri --database agri_analytics -q 'SELECT count(*) FROM ...'
```

## Reading command output

Targets that launch containers echo the `docker run` line and emit structured
JSON logs. The filter that leaves only the result:

```bash
make run-all 2>&1 | grep -vE '^docker run|^\{|^[0-9]{4}-' | tail -20
```

## The remote VM

Phases are verified on an x86_64 Ubuntu VM, never on the Mac — the Hadoop images
are amd64-only. Reach it **only** through `scripts/vm.sh`, which holds the SSH
options and the remote repo path:

```bash
scripts/vm.sh status                 # what is running
scripts/vm.sh sync                   # git pull + rebuild the ETL image
scripts/vm.sh make doctor            # sync, then run a target
scripts/vm.sh ch 'SELECT count(*) FROM agg_field_soil_daily'
scripts/vm.sh logs namenode 60
```

Connection details come from a gitignored `.env.vm` (see `.env.vm.example`) and
are deliberately **not** committed — this repo is public. If `vm.sh` reports a
missing variable, ask; do not guess a hostname or username.

`scripts/vm.sh sync` before anything that depends on ETL code. The VM runs the
image built from the last `git pull`, so an un-pushed local edit is invisible
there — this has already cost a debugging session.

## Conventions that are load-bearing

**Adding a table is configuration, not code.** Declare a `SnapshotSpec` or
`IncrementalSpec` in `etl/src/smart_agri/pipelines/bronze.py` and a `SilverSpec`
in `etl/src/smart_agri/pipelines/silver_specs.py`; the registry picks them up by
comprehension. Do not write a pipeline class for a plain source table. Full
checklist: `docs/adding-a-dataset.md`.

**The stage tuples exist twice.** `etl/src/smart_agri/pipelines/registry.py` and
`airflow/dags/common/pipelines.py` hold the same `*_STAGES` and `DOMAIN_STAGES`
declarations, because Airflow must never import the ETL package. Every stage
edit must be made in both, identically. `tests/unit/test_dag_pipeline_sync.py`
parses the Airflow copy with `ast` and diffs it.

**New env vars need registering.** A variable not listed in `_FORWARDED_ENV_VARS`
in `airflow/dags/common/etl_task.py` silently never reaches the task container.

**New contracts need three edits.** The schema in `contracts/schemas.py`, the
import in `contracts/__init__.py`, and the name in its `__all__` —
`no_implicit_reexport` is on, so a missing `__all__` entry fails type checking
at the import site, not the definition.

**Incremental Bronze appends and never replaces.** Writing an extract over the
partition deletes everything already landed. Each batch is its own file named
for its high-water mark, an empty batch writes nothing, and Silver reads the
partition as a directory. `docs/architecture.md` explains why;
`test_empty_batch_does_not_overwrite_a_populated_partition` pins it.

**Hive DDL is generated from the pandera contracts** in
`etl/src/smart_agri/metastore.py` — never hand-written. API-sourced datasets are
the exception and are hand-listed in `_weather_tables()`, guarded by
`test_metastore.py`.

**Read `docs/architecture.md` before changing a pinned version or swapping a
component.** Several pins are load-bearing and the file says which and why.

## A phase is not done until it has run on the VM

Every roadmap row in `README.md` has an exit criterion measured on the VM, not
in CI. Phase 5 passed strict mypy, the whole unit suite and six CI jobs and
still had seven integration-boundary bugs — every one found only by a real run.
Use
`/phase-verify <n>`; update the roadmap row only after it actually passes.

## Commits

One-line subject, imperative mood, lowercase after the first word — match
`git log`. Add terse bullets only for genuinely distinct changes. Never append
`Co-Authored-By` or any other AI attribution trailer.
