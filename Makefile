# =============================================================================
# smart-agri-platform
#
# Single entrypoint for the whole stack. Run `make` for the target list.
# Everything is expected to run on the Ubuntu x86_64 sandbox VM.
# =============================================================================

SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

ROOT     := $(shell pwd)
ENV_FILE := $(ROOT)/.env
COMPOSE  := docker compose --env-file $(ENV_FILE) -f $(ROOT)/docker/docker-compose.yml
UV       := uv --directory $(ROOT)/etl

# Values needed before .env exists are read with a default.
# Read a key from .env, falling back to a default when .env does not exist yet.
envval = $(or $(shell grep -E '^$(1)=' $(ENV_FILE) 2>/dev/null | cut -d= -f2-),$(2))

ETL_IMAGE       ?= $(call envval,ETL_IMAGE,smart-agri-etl:local)
ETL_TEST_IMAGE  ?= $(ETL_IMAGE)-test
NETWORK         ?= $(call envval,ETL_TASK_NETWORK,smart-agri_agri-net)
LIQUIBASE_IMAGE ?= $(call envval,LIQUIBASE_IMAGE,liquibase/liquibase:4.30)
PG_HOST         ?= $(call envval,POSTGRES_HOST,postgres)
PG_DB           ?= $(call envval,POSTGRES_DB,agri)
PG_USER         ?= $(call envval,POSTGRES_USER,agri)
PG_PASSWORD     ?= $(call envval,POSTGRES_PASSWORD,agri)

.PHONY: help
help: ## Show this help
	@echo "smart-agri-platform"
	@echo ""
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Typical first run:  make env && make build && make up-all && make doctor"

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
.PHONY: env
env: ## Create .env from the template, filling in host-specific values
	@if [ -f $(ENV_FILE) ]; then \
	  echo ".env already exists — leaving it untouched."; \
	else \
	  cp $(ROOT)/.env.example $(ENV_FILE); \
	  fernet=$$(openssl rand -base64 32 | tr '+/' '-_'); \
	  secret=$$(openssl rand -hex 32); \
	  uid=$$(id -u); \
	  gid=$$(getent group docker 2>/dev/null | cut -d: -f3); \
	  [ -n "$${gid}" ] || gid=999; \
	  sed -i.bak \
	    -e "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$${fernet}|" \
	    -e "s|^AIRFLOW_SECRET_KEY=.*|AIRFLOW_SECRET_KEY=$${secret}|" \
	    -e "s|^SUPERSET_SECRET_KEY=.*|SUPERSET_SECRET_KEY=$${secret}|" \
	    -e "s|^AIRFLOW_UID=.*|AIRFLOW_UID=$${uid}|" \
	    -e "s|^DOCKER_GID=.*|DOCKER_GID=$${gid}|" \
	    $(ENV_FILE); \
	  rm -f $(ENV_FILE).bak; \
	  echo ".env created (AIRFLOW_UID=$${uid}, DOCKER_GID=$${gid}, secrets generated)."; \
	fi
	@mkdir -p $(ROOT)/airflow/logs

.PHONY: preflight
preflight: ## Check the host can run the stack (arch, docker, ports, disk)
	@bash $(ROOT)/scripts/preflight.sh

# -----------------------------------------------------------------------------
# Images
# -----------------------------------------------------------------------------
.PHONY: build
build: build-etl build-services ## Build every locally-built image

.PHONY: build-etl
build-etl: ## Build the smart-agri-etl image that DockerOperator tasks run
	docker build -f $(ROOT)/docker/etl/Dockerfile --target runtime -t $(ETL_IMAGE) $(ROOT)

.PHONY: build-etl-test
build-etl-test: ## Build the ETL test image (dev deps + test suite)
	docker build -f $(ROOT)/docker/etl/Dockerfile --target test -t $(ETL_TEST_IMAGE) $(ROOT)

.PHONY: build-services
build-services: ## Build the customised Hive, Airflow and Superset images
	$(COMPOSE) --profile core --profile orchestration --profile bi build

# -----------------------------------------------------------------------------
# Stack lifecycle
# -----------------------------------------------------------------------------
.PHONY: up-core
up-core: ## Start storage and databases (postgres, clickhouse, HDFS, metastore)
	$(COMPOSE) --profile core up -d
	@$(MAKE) --no-print-directory wait-core

.PHONY: up-orchestration
up-orchestration: ## Start Airflow and everything it depends on
	$(COMPOSE) --profile orchestration up -d

.PHONY: up-bi
up-bi: ## Start Superset
	$(COMPOSE) --profile bi up -d

.PHONY: up-all
up-all: ## Start the whole platform
	$(COMPOSE) --profile core --profile orchestration --profile bi up -d
	@$(MAKE) --no-print-directory wait-core

.PHONY: wait-core
wait-core: ## Block until HDFS has left safe mode and the lake zones exist
	@echo "waiting for the NameNode to leave safe mode..."
	@$(COMPOSE) --profile core up -d hdfs-init
	@$(COMPOSE) logs hdfs-init | tail -n 8

.PHONY: down
down: ## Stop all containers, keeping volumes
	$(COMPOSE) --profile core --profile orchestration --profile bi down --remove-orphans

.PHONY: clean
clean: ## Stop everything AND delete all volumes (destroys the lake and both DBs)
	@printf 'This deletes the HDFS lake, Postgres and ClickHouse data. Type "yes" to continue: '; \
	read -r reply; [ "$$reply" = "yes" ] || { echo "aborted."; exit 1; }
	$(COMPOSE) --profile core --profile orchestration --profile bi down -v --remove-orphans
	@echo "all volumes removed."

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) --profile core --profile orchestration --profile bi ps

.PHONY: logs
logs: ## Tail logs; pass SERVICE=<name> to narrow (e.g. make logs SERVICE=namenode)
	$(COMPOSE) logs -f --tail=200 $(SERVICE)

# -----------------------------------------------------------------------------
# Verification
# -----------------------------------------------------------------------------
.PHONY: doctor
doctor: ## Check every backing service from inside a task container
	docker run --rm --network $(NETWORK) --env-file $(ENV_FILE) $(ETL_IMAGE) doctor

# -----------------------------------------------------------------------------
# Data platform — schema, seed data, pipelines
# -----------------------------------------------------------------------------
.PHONY: migrate
migrate: ## Apply the Liquibase changelogs to Postgres
	docker run --rm --network $(NETWORK) \
	  -v $(ROOT)/liquibase/changelog:/liquibase/changelog:ro \
	  $(LIQUIBASE_IMAGE) \
	  --changeLogFile=changelog/db.changelog-master.xml \
	  --url="jdbc:postgresql://$(PG_HOST):5432/$(PG_DB)" \
	  --username=$(PG_USER) --password=$(PG_PASSWORD) \
	  update

.PHONY: migrate-status
migrate-status: ## Show which changesets are pending
	docker run --rm --network $(NETWORK) \
	  -v $(ROOT)/liquibase/changelog:/liquibase/changelog:ro \
	  $(LIQUIBASE_IMAGE) \
	  --changeLogFile=changelog/db.changelog-master.xml \
	  --url="jdbc:postgresql://$(PG_HOST):5432/$(PG_DB)" \
	  --username=$(PG_USER) --password=$(PG_PASSWORD) \
	  status --verbose

.PHONY: init-clickhouse
init-clickhouse: ## Create the ClickHouse tables, views and materialized views
	docker run --rm --network $(NETWORK) --env-file $(ENV_FILE) \
	  -v $(ROOT)/clickhouse/ddl:/opt/clickhouse/ddl:ro \
	  $(ETL_IMAGE) init-clickhouse

.PHONY: register-tables
register-tables: ## Register every Bronze and Silver dataset in the Hive Metastore
	@docker run --rm --env-file $(ENV_FILE) $(ETL_IMAGE) register-tables > $(ROOT)/.register-tables.sql
	@docker cp $(ROOT)/.register-tables.sql $$($(COMPOSE) ps -q hiveserver2):/tmp/register-tables.sql
	@rm -f $(ROOT)/.register-tables.sql
	$(COMPOSE) exec -T hiveserver2 \
	  beeline -u "jdbc:hive2://localhost:10000" -f /tmp/register-tables.sql

.PHONY: hive-tables
hive-tables: ## List the external tables registered over the lake
	$(COMPOSE) exec -T hiveserver2 \
	  beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN agri_lake;"

.PHONY: seed
seed: ## Generate synthetic data into Postgres; PROFILE=small|medium|large
	docker run --rm --network $(NETWORK) --env-file $(ENV_FILE) \
	  $(ETL_IMAGE) seed --profile $(or $(PROFILE),small)

.PHONY: run
run: ## Run one pipeline; PIPELINE=silver.dim_farm [DATE=YYYY-MM-DD]
	@test -n "$(PIPELINE)" || { echo "usage: make run PIPELINE=<name> [DATE=...]"; exit 2; }
	docker run --rm --network $(NETWORK) --env-file $(ENV_FILE) \
	  $(ETL_IMAGE) run $(PIPELINE) $(if $(DATE),--date $(DATE),)

.PHONY: run-all
run-all: ## Run the whole soil-sensor slice locally (Airflow runs it as tasks)
	docker run --rm --network $(NETWORK) --env-file $(ENV_FILE) \
	  $(ETL_IMAGE) run-all $(if $(DATE),--date $(DATE),)

.PHONY: run-domain
run-domain: ## Run one domain's stages; DOMAIN=operations [DATE=YYYY-MM-DD]
	@test -n "$(DOMAIN)" || { echo "usage: make run-domain DOMAIN=<name> [DATE=...]"; exit 2; }
	docker run --rm --network $(NETWORK) --env-file $(ENV_FILE) \
	  $(ETL_IMAGE) run-domain $(DOMAIN) $(if $(DATE),--date $(DATE),)

.PHONY: lake
lake: ## Fill every Bronze and Silver zone: reference, operations, machinery, imagery
	@for domain in reference operations machinery imagery; do \
	  echo "--- $$domain ---"; \
	  $(MAKE) --no-print-directory run-domain DOMAIN=$$domain $(if $(DATE),DATE=$(DATE),) || exit 1; \
	done

.PHONY: analytics
analytics: ## Build the Gold marts and load the whole ClickHouse star schema
	$(MAKE) --no-print-directory run-domain DOMAIN=analytics $(if $(DATE),DATE=$(DATE),)

.PHONY: weather-backfill
weather-backfill: ## Load the full Open-Meteo history for every farm (needs network)
	docker run --rm --network $(NETWORK) --env-file $(ENV_FILE) \
	  $(ETL_IMAGE) weather-backfill $(if $(DATE),--date $(DATE),)

.PHONY: weather
weather: ## Refresh weather for every farm and reload ClickHouse
	docker run --rm --network $(NETWORK) --env-file $(ENV_FILE) \
	  $(ETL_IMAGE) weather $(if $(DATE),--date $(DATE),)

.PHONY: pipelines
pipelines: ## List the registered pipelines in execution order
	docker run --rm --env-file $(ENV_FILE) $(ETL_IMAGE) list-pipelines

.PHONY: demo
demo: ## Zero to dashboard: migrate, seed, build the warehouse, load, import charts
	@$(MAKE) --no-print-directory migrate
	@$(MAKE) --no-print-directory init-clickhouse
	@$(MAKE) --no-print-directory seed
	@$(MAKE) --no-print-directory run-all
	@$(MAKE) --no-print-directory weather
	@$(MAKE) --no-print-directory lake
	@$(MAKE) --no-print-directory analytics
	@$(MAKE) --no-print-directory superset-import
	@echo ""
	@$(MAKE) --no-print-directory urls

# -----------------------------------------------------------------------------
# Superset assets
# -----------------------------------------------------------------------------
.PHONY: superset-import
superset-import: ## Import the YAML dashboards, charts and datasets
	@bash $(ROOT)/scripts/superset_import.sh

.PHONY: superset-export
superset-export: ## Export the live dashboards back into superset/assets/
	@bash $(ROOT)/scripts/superset_export.sh

.PHONY: hdfs-init
hdfs-init: ## (Re-)create the lake zone directories in HDFS
	$(COMPOSE) --profile core up hdfs-init

.PHONY: hdfs-ls
hdfs-ls: ## List a lake path; ZONE=bronze PATH_=farm (defaults to the lake root)
	$(COMPOSE) exec namenode hdfs dfs -ls -R /lake/$(ZONE)/$(PATH_)

.PHONY: psql
psql: ## Open a psql shell on the source database
	$(COMPOSE) exec postgres psql -U $$(grep -E '^POSTGRES_USER=' $(ENV_FILE) | cut -d= -f2) \
	                              -d $$(grep -E '^POSTGRES_DB=' $(ENV_FILE) | cut -d= -f2)

.PHONY: ch
ch: ## Open a clickhouse-client shell on the analytics database
	$(COMPOSE) exec clickhouse clickhouse-client \
	  --user $$(grep -E '^CLICKHOUSE_USER=' $(ENV_FILE) | cut -d= -f2) \
	  --password $$(grep -E '^CLICKHOUSE_PASSWORD=' $(ENV_FILE) | cut -d= -f2) \
	  --database $$(grep -E '^CLICKHOUSE_DB=' $(ENV_FILE) | cut -d= -f2)

.PHONY: vm-tunnel
vm-tunnel: ## SSH-forward a VM service to localhost; SERVICE=hive|postgres|clickhouse|superset (default hive). Foreground — Ctrl-C to close
	@scripts/vm.sh tunnel $(or $(SERVICE),hive)

.PHONY: urls
urls: ## Print the web UIs exposed by the stack
	@echo "Airflow    http://localhost:$$(grep -E '^AIRFLOW_HOST_PORT='  $(ENV_FILE) | cut -d= -f2)"
	@echo "Superset   http://localhost:$$(grep -E '^SUPERSET_HOST_PORT=' $(ENV_FILE) | cut -d= -f2)"
	@echo "HDFS       http://localhost:$$(grep -E '^HDFS_NAMENODE_HTTP_HOST_PORT=' $(ENV_FILE) | cut -d= -f2)"
	@echo "ClickHouse http://localhost:$$(grep -E '^CLICKHOUSE_HTTP_HOST_PORT=' $(ENV_FILE) | cut -d= -f2)/play"

# -----------------------------------------------------------------------------
# ETL application quality gate — the same commands CI runs
# -----------------------------------------------------------------------------
.PHONY: install
install: ## Install the ETL app and its dev dependencies
	$(UV) sync

.PHONY: fmt
fmt: ## Format the ETL application
	$(UV) run ruff format .
	$(UV) run ruff check --fix .

.PHONY: lint
lint: ## Lint the ETL application
	$(UV) run ruff check .
	$(UV) run ruff format --check .

.PHONY: typecheck
typecheck: ## Strict type check
	$(UV) run mypy

.PHONY: test
test: ## Unit tests with the coverage gate
	$(UV) run pytest -m "not integration" --cov

.PHONY: test-integration
test-integration: build-etl-test ## Integration tests, run on the platform network (needs `make up-core`)
	docker run --rm --network $(NETWORK) --env-file $(ENV_FILE) $(ETL_TEST_IMAGE) -m integration

.PHONY: test-dags
test-dags: ## DAG integrity tests, run inside the Airflow container
	$(COMPOSE) exec -T airflow-scheduler pytest /opt/airflow/tests -q

.PHONY: check
check: lint typecheck test ## Everything CI enforces

# -----------------------------------------------------------------------------
# Convenience
# -----------------------------------------------------------------------------
.PHONY: validate-compose
validate-compose: ## Verify the Compose file parses and resolves every variable
	@$(COMPOSE) --profile core --profile orchestration --profile bi config -q \
	  && echo "docker-compose.yml is valid."

.PHONY: validate-ddl
validate-ddl: ## Apply the ClickHouse DDL to a throwaway server and query every view
	@bash $(ROOT)/scripts/validate_ddl.sh

.PHONY: check-docs
check-docs: ## Verify every documented `make` target and relative link still resolves
	@python3 $(ROOT)/scripts/check_docs.py

.PHONY: screenshot-dashboards
screenshot-dashboards: ## Screenshot every dashboard and diff against superset/baselines (needs up-bi). UPDATE=1 to re-record
	@$(COMPOSE) --profile bi ps -q superset | grep -q . \
	  || { echo "superset is not running — try 'make up-bi'" >&2; exit 1; }
	@docker build -q \
	  --build-arg PLAYWRIGHT_IMAGE="$(call envval,PLAYWRIGHT_IMAGE,mcr.microsoft.com/playwright/python:v1.62.0-noble)" \
	  -t smart-agri-screenshot:local $(ROOT)/docker/screenshot >/dev/null
	@docker run --rm \
	  --network "$(call envval,ETL_TASK_NETWORK,smart-agri_agri-net)" \
	  -e SUPERSET_ADMIN_USER="$(call envval,SUPERSET_ADMIN_USER,admin)" \
	  -e SUPERSET_ADMIN_PASSWORD="$(call envval,SUPERSET_ADMIN_PASSWORD,admin)" \
	  -v $(ROOT)/scripts:/work:ro \
	  -v $(ROOT)/superset/baselines:/baselines \
	  smart-agri-screenshot:local \
	  /work/screenshot_dashboards.py $(if $(UPDATE),--update,)

.PHONY: verify-dashboards
verify-dashboards: ## Run every Superset chart and report whether it returns data
	@CID=$$($(COMPOSE) --profile bi ps -q superset); \
	[ -n "$$CID" ] || { echo "superset is not running — try 'make up-bi'" >&2; exit 1; }; \
	docker cp $(ROOT)/scripts/verify_dashboards.py "$$CID:/tmp/verify_dashboards.py" >/dev/null; \
	docker exec \
	  -e SUPERSET_ADMIN_USER="$(call envval,SUPERSET_ADMIN_USER,admin)" \
	  -e SUPERSET_ADMIN_PASSWORD="$(call envval,SUPERSET_ADMIN_PASSWORD,admin)" \
	  "$$CID" python /tmp/verify_dashboards.py
