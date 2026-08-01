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
ETL_IMAGE      ?= $(shell grep -E '^ETL_IMAGE=' $(ENV_FILE) 2>/dev/null | cut -d= -f2 || echo smart-agri-etl:local)
ETL_TEST_IMAGE ?= $(ETL_IMAGE)-test
NETWORK        ?= $(shell grep -E '^ETL_TASK_NETWORK=' $(ENV_FILE) 2>/dev/null | cut -d= -f2 || echo smart-agri_agri-net)

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
build-services: ## Build the customised Airflow and Superset images
	$(COMPOSE) --profile orchestration --profile bi build

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
