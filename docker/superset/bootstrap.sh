#!/usr/bin/env bash
# Initialise Superset, then hand off to the web server.
# Every step is idempotent, so a restart is always safe.
set -euo pipefail

echo "[superset] applying database migrations"
superset db upgrade

echo "[superset] ensuring admin user"
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" \
  --firstname Admin --lastname User \
  --email "${SUPERSET_ADMIN_EMAIL}" 2>/dev/null || echo "[superset] admin already exists"

echo "[superset] initialising roles and permissions"
superset init

# Assets are imported explicitly via `make superset-import` rather than on every
# boot, so a locally-edited dashboard is never silently overwritten.
echo "[superset] ready — starting web server"
exec /usr/bin/run-server.sh
