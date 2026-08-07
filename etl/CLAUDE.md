# Working in the ETL package

Applies to `etl/`. The root `CLAUDE.md` covers the stack; this covers the Python.

## Running the tools

From the repo root, always with `--directory`:

```bash
uv --directory etl run ruff check .
uv --directory etl run mypy
uv --directory etl run pytest -m "not integration" -q
```

`mypy` takes **no path argument**. `pyproject.toml` sets `mypy_path = "src"` and
`packages = ["smart_agri"]`; passing a path instead type-checks the wrong roots.

Integration tests need `make up-core` and are selected with `-m integration`.
They run inside a container on `agri-net` — a WebHDFS write redirects to
`datanode:9864`, which only resolves there, so they cannot pass from the host.

## What the linters enforce

- **ruff**, line length 100, `target-version = py312`. Selected rule families
  include `TCH` (flake8-type-checking), `PTH`, `SIM`, `RET`, `ARG`, `PL`, `RUF`.
- **Do not move pydantic imports into `TYPE_CHECKING`.** Pydantic evaluates its
  base classes at runtime; `runtime-evaluated-base-classes` is configured for
  this and `TCH` will not ask you to. If it seems to, the model is missing from
  that setting — fix the setting, not the import.
- **mypy is `strict`**, plus `warn_unreachable`, `disallow_any_generics` and
  `no_implicit_reexport`. That last one is why every public name needs an
  explicit `__all__` entry in its package `__init__.py`.
- **Coverage gate: `fail_under = 80`**, branch coverage over `src/smart_agri`.
  A new module with no tests can fail `make check` on coverage alone.

`make fmt` from the root fixes formatting and the auto-fixable lint. A
`PostToolUse` hook already formats each Python file as it is edited, so `make
fmt` is normally only needed after a bulk change.

## Layout

| Path | Responsibility |
|---|---|
| `config/` | `pydantic-settings` blocks, one per service. Environment only — no config file |
| `domain/` | Pydantic entity models |
| `contracts/` | pandera schemas per zone boundary, plus `validate()` |
| `io/` | Postgres (ADBC), WebHDFS, ClickHouse, Open-Meteo, and the control store |
| `pipelines/` | `extract → validate → transform → load`, plus the specs and registry |
| `generator/` | Synthetic data generator and the Postgres seeder |
| `metastore.py` | Hive DDL generated from the contracts |
| `cli.py` | The Typer app — the only entrypoint |

Subcommands import `smart_agri.pipelines` lazily **inside** the function body.
This keeps `--help` fast, and `--help` is the image's default `CMD`. Keep it.

## Contracts

Bronze schemas are `strict=False` — presence and coarse types only, because
Bronze is as-extracted. Silver schemas are `strict=True` with ranges,
uniqueness and `utc_datetime()` on timestamps; that boundary is what fills
`/lake/quarantine`.

Adding one means three edits: the definition in `contracts/schemas.py`, the
import in `contracts/__init__.py`, and the name in `__all__` there.

## Tests

`tests/unit/` and `tests/integration/`, with `tests/fakes.py` holding
**behavioural fakes, not mocks** — `FakeHdfsStore`, `FakePostgresSource`,
`FakeControlStore`, `FakeClickHouseSink`. A mock that accepts any column is how
the seeder shipped writing columns that did not exist.

`FakePostgresSource._identity_column` is a hand-maintained dict. A table missing
from it breaks surrogate-key resolution **silently**. Add every new table.

Naming: `test_<module>.py`, classes `TestSomeBehaviour`, methods named as a full
sentence describing the behaviour —
`test_empty_batch_does_not_overwrite_a_populated_partition`. The docstring says
which failure the test prevents, not what the code does.

Several tests exist to catch drift between things that are declared twice, and
they will fail if you edit one side only:

| Test | Guards |
|---|---|
| `test_dag_pipeline_sync.py` | `registry.py` stage tuples vs `airflow/dags/common/pipelines.py` |
| `test_silver_specs.py` | every `SilverSpec` against its Bronze source and schema |
| `test_seeder_columns.py` | the seeder's COPY columns vs the Liquibase changelogs |
| `test_reference_catalogue.py` | the Python `CROPS`/`ALL_REGIONS` vs the Liquibase INSERTs |
| `test_metastore.py` | the hand-listed weather tables vs the registry |

Treat a failure in any of these as "you edited one of a pair" before assuming
the test is wrong.
