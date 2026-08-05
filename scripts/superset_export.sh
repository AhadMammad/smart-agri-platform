#!/usr/bin/env bash
# Export the dashboards from a running Superset back into superset/assets/.
#
# The intended workflow: adjust a chart in the UI, run this, review the diff,
# commit. That keeps the repo the source of truth without forcing every tweak
# to be hand-written as YAML.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ASSETS="${ROOT}/superset/assets"
ENV_FILE="${ROOT}/.env"

[ -f "$ENV_FILE" ] || { echo "no .env — run 'make env' first" >&2; exit 1; }
# shellcheck disable=SC1090
set -a && . "$ENV_FILE" && set +a

CONTAINER="$(docker compose --env-file "$ENV_FILE" \
  -f "${ROOT}/docker/docker-compose.yml" ps -q superset)"
[ -n "$CONTAINER" ] || { echo "superset is not running — try 'make up-bi'" >&2; exit 1; }

STAGE="$(mktemp -d)"
trap 'rm -rf "${STAGE}"' EXIT

docker exec "$CONTAINER" bash -c \
  'rm -rf /tmp/export && superset export-dashboards -f /tmp/export.zip'
docker cp "${CONTAINER}:/tmp/export.zip" "${STAGE}/export.zip"

(cd "$STAGE" && unzip -q export.zip)
EXPORTED="$(find "$STAGE" -mindepth 1 -maxdepth 1 -type d | head -1)"
[ -n "$EXPORTED" ] || { echo "export produced no directory" >&2; exit 1; }

rm -rf "${ASSETS:?}"/*
cp -R "${EXPORTED}"/. "$ASSETS"/

# Put the placeholders back so the committed YAML never carries a credential.
DB_FILE="$(find "${ASSETS}/databases" -name '*.yaml' | head -1)"
if [ -n "$DB_FILE" ]; then
  # SC2016: single quotes are deliberate — the ${...} must land in the file as
  # literal placeholders for superset_import.sh to substitute later.
  # shellcheck disable=SC2016
  sed -i.bak -E \
    's|^sqlalchemy_uri:.*|sqlalchemy_uri: clickhousedb://${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}@${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/${CLICKHOUSE_DB}|' \
    "$DB_FILE"
  rm -f "${DB_FILE}.bak"
fi

echo "exported into ${ASSETS} — review with 'git diff' before committing"
