#!/usr/bin/env bash
# Import the YAML assets in superset/assets/ into the running Superset.
#
# Superset's importer takes a zip in its export layout, so the tree is bundled
# here rather than committed as a binary. The database URI carries ${...}
# placeholders which are substituted from .env, so no credential is committed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ASSETS="${ROOT}/superset/assets"
ENV_FILE="${ROOT}/.env"

[ -d "$ASSETS" ] || { echo "no assets at ${ASSETS}" >&2; exit 1; }
[ -f "$ENV_FILE" ] || { echo "no .env — run 'make env' first" >&2; exit 1; }

# shellcheck disable=SC1090  # path is computed, not literal
set -a && . "$ENV_FILE" && set +a

STAGE="$(mktemp -d)"
trap 'rm -rf "${STAGE}"' EXIT

BUNDLE="${STAGE}/assets"
cp -R "$ASSETS" "$BUNDLE"

# Substitute only the ClickHouse variables, so a literal ${...} anywhere else
# in the YAML survives untouched.
DB_FILE="${BUNDLE}/databases/clickhouse_agri.yaml"
if [ -f "$DB_FILE" ]; then
  export CLICKHOUSE_USER="${CLICKHOUSE_USER:-agri}"
  export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-agri}"
  export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
  export CLICKHOUSE_HTTP_PORT="${CLICKHOUSE_HTTP_PORT:-8123}"
  export CLICKHOUSE_DB="${CLICKHOUSE_DB:-agri_analytics}"

  # SC2016: single quotes are required here — envsubst takes the literal
  # variable *names* as its allow-list, not their values. Restricting the list
  # means a stray ${...} elsewhere in the YAML survives untouched.
  # shellcheck disable=SC2016
  ALLOWED='${CLICKHOUSE_USER} ${CLICKHOUSE_PASSWORD} ${CLICKHOUSE_HOST} ${CLICKHOUSE_HTTP_PORT} ${CLICKHOUSE_DB}'

  envsubst "$ALLOWED" < "$DB_FILE" > "${DB_FILE}.tmp"
  mv "${DB_FILE}.tmp" "$DB_FILE"
fi

ZIP="${STAGE}/smart-agri-assets.zip"
(cd "$STAGE" && zip -qr "$ZIP" assets)

CONTAINER="$(docker compose --env-file "$ENV_FILE" \
  -f "${ROOT}/docker/docker-compose.yml" ps -q superset)"
[ -n "$CONTAINER" ] || { echo "superset is not running — try 'make up-bi'" >&2; exit 1; }

docker cp "$ZIP" "${CONTAINER}:/tmp/smart-agri-assets.zip"
docker exec "$CONTAINER" superset import-dashboards \
  -p /tmp/smart-agri-assets.zip -u "${SUPERSET_ADMIN_USER:-admin}"

echo "assets imported — open http://localhost:${SUPERSET_HOST_PORT:-8088}"
