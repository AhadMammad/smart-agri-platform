"""Superset configuration for smart-agri-platform.

Loaded via PYTHONPATH=/app/pythonpath. Everything environment-driven so the
image stays reusable and no secret is baked into a layer.
"""

from __future__ import annotations

import os

# --- Core -------------------------------------------------------------------
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DB_URI"]
SQLALCHEMY_TRACK_MODIFICATIONS = False

# --- Behaviour --------------------------------------------------------------
ROW_LIMIT = 50_000
SUPERSET_WEBSERVER_TIMEOUT = 120
SQLLAB_TIMEOUT = 120
SUPERSET_LOAD_EXAMPLES = False

FEATURE_FLAGS = {
    # Required so `superset import-directory` can load the YAML assets in
    # superset/assets/ as version-controlled dashboards.
    "VERSIONED_EXPORT": True,
    "DASHBOARD_RBAC": True,
    "EMBEDDED_SUPERSET": False,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# --- Caching ----------------------------------------------------------------
# Simple in-process caching is enough for a single-node dev stack; swap for
# Redis if the BI profile ever runs more than one Superset worker.
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
DATA_CACHE_CONFIG = CACHE_CONFIG

# --- Convenience ------------------------------------------------------------
# Pre-computed URI for the analytics warehouse, used by bootstrap.sh when it
# registers the ClickHouse database on first start.
CLICKHOUSE_URI = (
    "clickhousedb://{user}:{password}@{host}:{port}/{database}".format(
        user=os.environ.get("CLICKHOUSE_USER", "agri"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "agri"),
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"),
        database=os.environ.get("CLICKHOUSE_DB", "agri_analytics"),
    )
)
