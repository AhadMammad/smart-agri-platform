# smart-agri (ETL application)

The Python application that moves data through the platform:

```
PostgreSQL ──► HDFS /lake/bronze ──► /lake/silver ──► /lake/gold ──► ClickHouse
```

Airflow never imports this package. It launches the `smart-agri-etl` image with
a command vector via `DockerOperator`, so every pipeline is reachable as a CLI
subcommand and the ETL code stays independently testable.

## Layout

| Path | Responsibility |
|---|---|
| `config/` | `pydantic-settings` blocks, one per backing service |
| `domain/` | Pydantic models for agricultural entities |
| `contracts/` | Pandera schemas enforced at each zone boundary |
| `io/` | Connectors: Postgres, WebHDFS, ClickHouse, Open-Meteo |
| `pipelines/` | `extract → validate → transform → load` job classes |
| `generator/` | Synthetic data generator |
| `utils/` | Logging and shared helpers |

## Local development

```bash
uv sync                      # create .venv and install dev dependencies
uv run ruff check .          # lint
uv run ruff format .         # format
uv run mypy                  # strict type check
uv run pytest -m "not integration"
```

Integration tests need the stack running (`make up-core` from the repo root)
and are selected with `uv run pytest -m integration`.

## CLI

```bash
smart-agri --help
smart-agri version
smart-agri doctor            # connectivity check against every backing service
```
