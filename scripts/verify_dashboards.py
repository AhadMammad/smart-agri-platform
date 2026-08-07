"""Run every chart through Superset and report whether it returns data.

`test_superset_assets.py` checks the YAML graph statically — that every UUID
resolves and every column named exists in ClickHouse. It cannot tell whether a
chart actually renders: that needs a running Superset, a real datasource and a
query that reaches the warehouse.

This closes that gap. For each chart it builds a query context from the chart's
own stored params — the same thing the dashboard frontend does — and posts it to
`/api/v1/chart/data`, so the whole path is exercised: datasource resolution, SQL
generation, and the ClickHouse round trip.

Run it with `make verify-dashboards`, which executes it inside the Superset
container where the API and the admin credentials are reachable.

Two details are load-bearing:

* `adhoc_filters` are folded into `extras.where`. The chart data API does not
  read them from `form_data`, so without this the check would query unfiltered
  rows and report totals no chart ever shows.
* `/api/v1/chart/<id>/data/` is deliberately not used. It needs a stored
  `query_context`, which Superset writes only when a chart is saved in the UI
  and which the export format does not carry — so it reports a false failure for
  every chart that has only ever been imported.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any

BASE = os.environ.get("SUPERSET_BASE_URL", "http://localhost:8088")
USER = os.environ.get("SUPERSET_ADMIN_USER", "admin")
PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")
TIMEOUT = 180


def call(
    path: str, method: str = "GET", body: Any = None, token: str | None = None
) -> dict[str, Any]:
    request = urllib.request.Request(BASE + path, method=method)  # noqa: S310
    request.add_header("Content-Type", "application/json")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    payload = json.dumps(body).encode() if body is not None else None
    with urllib.request.urlopen(request, payload, timeout=TIMEOUT) as response:  # noqa: S310
        return json.loads(response.read())


def login() -> str:
    credentials = {
        "username": USER,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True,
    }
    return call("/api/v1/security/login", "POST", credentials)["access_token"]


def query_context(params: dict[str, Any], dataset_id: int) -> dict[str, Any]:
    """A query context equivalent to what the dashboard would send."""
    where = " AND ".join(
        f"({item['sqlExpression']})"
        for item in params.get("adhoc_filters") or []
        if item.get("clause") == "WHERE" and item.get("expressionType") == "SQL"
    )

    query: dict[str, Any] = {
        "filters": [],
        "extras": {"having": "", "where": where},
        "annotation_layers": [],
        "row_limit": params.get("row_limit", 1000),
        "series_limit": 0,
        "order_desc": True,
        "url_params": {},
        "custom_params": {},
        "custom_form_data": {},
        "time_range": "No filter",
    }

    if params.get("query_mode") == "raw":
        query["columns"] = list(params.get("all_columns") or [])
        query["metrics"] = None
        query["orderby"] = []
    else:
        x_axis = params.get("x_axis")
        columns = [x_axis] if x_axis else []
        columns += [c for c in (params.get("groupby") or []) if c != x_axis]
        query["columns"] = columns
        query["metrics"] = list(params.get("metrics") or [])
        if params.get("granularity_sqla"):
            query["granularity"] = params["granularity_sqla"]

    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": True,
        "queries": [query],
        "form_data": params,
        "result_format": "json",
        "result_type": "full",
    }


def main() -> int:
    token = login()

    dashboards = call("/api/v1/dashboard/?q=(page_size:100)", token=token)
    print(f"dashboards: {dashboards['count']}")
    for dashboard in sorted(dashboards["result"], key=lambda d: d["dashboard_title"]):
        charts = call(f"/api/v1/dashboard/{dashboard['id']}/charts", token=token)["result"]
        datasets = call(f"/api/v1/dashboard/{dashboard['id']}/datasets", token=token)["result"]
        print(
            f"  {dashboard['dashboard_title']:24s} slug={str(dashboard.get('slug')):20s} "
            f"charts={len(charts):2d} datasets={len(datasets)}"
        )

    listing = call("/api/v1/dataset/?q=(columns:!(id,table_name,uuid),page_size:100)", token=token)
    by_uuid = {item["uuid"]: item["id"] for item in listing["result"] if item.get("uuid")}

    charts = call("/api/v1/chart/?q=(page_size:100)", token=token)
    print(f"\ncharts: {charts['count']}")

    passed = empty = failed = 0
    for chart in sorted(charts["result"], key=lambda c: c["slice_name"]):
        name = chart["slice_name"]
        detail = call(f"/api/v1/chart/{chart['id']}", token=token)["result"]
        params = json.loads(detail["params"])

        # On import Superset rewrites params.datasource from "<uuid>__table" to
        # "<id>__table", so the stored value is usually already the numeric id.
        reference = str(params.get("datasource", "")).split("__")[0]
        dataset_id = int(reference) if reference.isdigit() else by_uuid.get(reference)
        if dataset_id is None:
            print(f"  FAIL  {name:38s} cannot resolve dataset {reference}")
            failed += 1
            continue

        try:
            result = call(
                "/api/v1/chart/data", "POST", query_context(params, dataset_id), token=token
            )["result"][0]
        except urllib.error.HTTPError as exc:
            body = exc.read()[:200].decode(errors="replace")
            print(f"  FAIL  {name:38s} HTTP {exc.code} {body}")
            failed += 1
            continue

        if result.get("error"):
            print(f"  FAIL  {name:38s} {str(result['error'])[:110]}")
            failed += 1
        elif result.get("rowcount", 0) > 0:
            print(f"  ok    {name:38s} {result['rowcount']:>6d} rows")
            passed += 1
        else:
            # An empty chart is a failure here: every chart on these dashboards
            # is expected to have data once the marts are loaded.
            print(f"  EMPTY {name:38s}      0 rows")
            empty += 1

    print(f"\n{passed} returning data, {empty} empty, {failed} failing")
    return 1 if (empty or failed) else 0


if __name__ == "__main__":
    sys.exit(main())
