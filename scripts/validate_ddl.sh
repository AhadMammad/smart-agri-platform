#!/usr/bin/env bash
# Apply the ClickHouse DDL to a throwaway server and query every view.
#
# `test_clickhouse_ddl.py` checks the column lists against the contracts, but it
# cannot tell whether the SQL is *valid* — and two of these views were rejected
# outright by ClickHouse for nesting an aggregate inside a SELECT alias, which no
# amount of parsing would have caught.
#
# Creating a view only proves it resolves; each one is therefore also queried,
# so an error in the body surfaces here rather than on a dashboard.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DDL_DIR="$ROOT/clickhouse/ddl"
CONTAINER="smart-agri-ddl-check-$$"
IMAGE="${CLICKHOUSE_IMAGE:-clickhouse/clickhouse-server:24.8}"
DB="agri_analytics"

cleanup() { docker rm -f "$CONTAINER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

[ -d "$DDL_DIR" ] || { echo "no ddl directory at $DDL_DIR" >&2; exit 1; }

echo "starting $IMAGE ..."
docker run -d --rm --name "$CONTAINER" \
  -e CLICKHOUSE_DB="$DB" \
  -e CLICKHOUSE_USER=agri \
  -e CLICKHOUSE_PASSWORD=agri \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  "$IMAGE" >/dev/null

ch() { docker exec -i "$CONTAINER" clickhouse-client --user agri --password agri --database "$DB" "$@"; }

# Wait for the database, not for /ping. The entrypoint answers HTTP from a
# temporary server *before* it has created CLICKHOUSE_DB, then stops it and
# starts the real one — so a /ping check races both the missing database and a
# refused native port.
ready=0
for _ in $(seq 1 90); do
  if docker exec "$CONTAINER" clickhouse-client --user agri --password agri \
       --database "$DB" -q 'SELECT 1' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
[ "$ready" -eq 1 ] || { echo "clickhouse did not become ready" >&2; exit 1; }

failures=0

# Name order, exactly as `smart-agri init-clickhouse` applies them.
while IFS= read -r file; do
  if output="$(ch -n < "$file" 2>&1)" && [ -z "$output" ]; then
    printf '  ok   %s\n' "${file#"$ROOT"/}"
  else
    printf '  FAIL %s\n%s\n' "${file#"$ROOT"/}" "$output"
    failures=$((failures + 1))
  fi
done < <(find "$DDL_DIR" -name '*.sql' | sort)

echo "querying every view ..."
# Collected up front, not streamed into the loop: `docker exec -i` reads stdin,
# so a piped list would be swallowed after the first iteration.
views="$(ch -q "SELECT name FROM system.tables WHERE database = '$DB' AND engine LIKE '%View' ORDER BY name" </dev/null)"
[ -n "$views" ] || { echo "no views found — the DDL did not create any" >&2; exit 1; }

for view in $views; do
  if output="$(ch -q "SELECT count() FROM $view" </dev/null 2>&1)"; then
    printf '  ok   %s\n' "$view"
  else
    printf '  FAIL %s\n%s\n' "$view" "$(printf '%s' "$output" | head -3)"
    failures=$((failures + 1))
  fi
done

if [ "$failures" -gt 0 ]; then
  echo "$failures DDL problem(s)." >&2
  exit 1
fi

echo "ClickHouse DDL is valid."
