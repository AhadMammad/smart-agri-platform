"""Screenshot every dashboard and diff it against its committed baseline.

`test_superset_assets.py` checks the asset graph statically and
`verify_dashboards.py` checks that every chart returns rows. Neither can see the
thing a reader actually complains about: labels overlapping the plot, a title
sitting on top of its tick labels, a legend covering the series. That is a
property of the rendered pixels — it depends on data cardinality, font metrics
and container width — so it needs a real browser.

This runs inside `docker/screenshot/Dockerfile` (see `make screenshot-dashboards`)
rather than on the host, because the comparison is only meaningful if the fonts
and browser are identical between the run that recorded the baseline and the run
being checked.

Determinism is the whole game here. Four things are pinned so that an unchanged
dashboard diffs to zero:

* the browser and fonts, by running in that image;
* the viewport, so nothing reflows;
* CSS animation and ECharts' entry transitions, which are disabled outright —
  otherwise the screenshot catches a bar mid-grow;
* the data, which is already deterministic because the generator is seeded
  (`GENERATOR_SEED`).

Even so the comparison allows a small fraction of pixels to differ:
anti-aliasing is not bit-stable across runs, and failing on a single pixel would
make this cry wolf until someone stopped running it.

Baselines live in `superset/baselines/` and are committed. Record or refresh
them with `--update`, and review the resulting image diff like any other change.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from playwright.sync_api import Page, sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeout

BASE = os.environ.get("SUPERSET_BASE_URL", "http://superset:8088")
USER = os.environ.get("SUPERSET_ADMIN_USER", "admin")
PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")

BASELINES = Path(os.environ.get("BASELINE_DIR", "/baselines"))
CURRENT = BASELINES / "current"

#: Wide enough that a 4-column chart is still a realistic width. Changing this
#: invalidates every baseline.
VIEWPORT = {"width": 1600, "height": 1200}

#: Fraction of pixels allowed to differ before a dashboard counts as changed.
#: Absorbs anti-aliasing jitter without hiding a shifted label.
MAX_DIFF_RATIO = 0.002

#: Per-channel value below which two pixels are "the same colour".
CHANNEL_TOLERANCE = 24

#: Superset keeps a spinner in the DOM per loading chart; the dashboard is
#: settled when none are left.
LOADING = ".loading, [data-test='loading-indicator'], .chart-status-loading"

FREEZE_ANIMATIONS = """
* , *::before, *::after {
  animation-duration: 0s !important;
  animation-delay: 0s !important;
  transition-duration: 0s !important;
  transition-delay: 0s !important;
}
"""


def login(page: Page) -> None:
    page.goto(f"{BASE}/login/", wait_until="domcontentloaded")
    page.fill("input[name='username']", USER)
    page.fill("input[name='password']", PASSWORD)
    page.click("input[type='submit'], button[type='submit']")
    page.wait_for_load_state("networkidle")


def dashboard_slugs(page: Page) -> list[tuple[str, str]]:
    """(slug, title) for every dashboard, read through the API as the logged-in user."""
    response = page.request.get(f"{BASE}/api/v1/dashboard/?q=(page_size:100)")
    payload = response.json()
    out = []
    for item in payload.get("result", []):
        # A dashboard without a slug is still reachable by id.
        out.append((item.get("slug") or str(item["id"]), item["dashboard_title"]))
    return sorted(out)


def capture(page: Page, slug: str, destination: Path) -> None:
    # standalone=3 drops the nav bar and dashboard header, which carry a
    # "last modified" timestamp that would differ on every single run.
    page.goto(f"{BASE}/superset/dashboard/{slug}/?standalone=3", wait_until="domcontentloaded")
    page.add_style_tag(content=FREEZE_ANIMATIONS)
    try:
        page.wait_for_load_state("networkidle", timeout=120_000)
        page.wait_for_selector(LOADING, state="detached", timeout=120_000)
    except PlaywrightTimeout:
        # Capture anyway: a dashboard stuck loading is itself worth seeing in
        # the diff, and failing here would hide it.
        print(f"    warning: {slug} never went idle — capturing as-is", file=sys.stderr)
    page.wait_for_timeout(2_000)
    destination.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(destination), full_page=True)


def diff_ratio(baseline: Path, current: Path) -> float | None:
    """Fraction of pixels that differ, or None if the sizes don't match."""
    from PIL import Image, ImageChops

    with Image.open(baseline) as a, Image.open(current) as b:
        left, right = a.convert("RGB"), b.convert("RGB")
        if left.size != right.size:
            return None
        delta = ImageChops.difference(left, right)
        # A pixel counts as changed when any channel moves more than the
        # tolerance; `point` then `convert` turns that into a countable mask.
        mask = delta.convert("L").point(lambda v: 255 if v > CHANNEL_TOLERANCE else 0)
        changed = sum(1 for pixel in mask.getdata() if pixel)
        return changed / (mask.size[0] * mask.size[1])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--update", action="store_true", help="overwrite the baselines instead of comparing"
    )
    args = parser.parse_args()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context(viewport=VIEWPORT, device_scale_factor=1)
        page = context.new_page()

        login(page)
        dashboards = dashboard_slugs(page)
        if not dashboards:
            print("no dashboards found — is the asset bundle imported?", file=sys.stderr)
            return 1

        print(f"dashboards: {len(dashboards)}")
        target = BASELINES if args.update else CURRENT
        for slug, title in dashboards:
            print(f"  capturing {title} ({slug})")
            capture(page, slug, target / f"{slug}.png")

        browser.close()

    if args.update:
        print(f"\nbaselines written to {BASELINES} — review the image diff before committing")
        return 0

    changed, missing = [], []
    for slug, _title in dashboards:
        baseline = BASELINES / f"{slug}.png"
        current = CURRENT / f"{slug}.png"
        if not baseline.exists():
            missing.append(slug)
            continue
        ratio = diff_ratio(baseline, current)
        if ratio is None:
            changed.append(f"{slug}: size changed (layout reflowed)")
        elif ratio > MAX_DIFF_RATIO:
            changed.append(f"{slug}: {ratio:.2%} of pixels differ")
        else:
            print(f"  ok    {slug:24s} {ratio:.3%}")

    for slug in missing:
        print(f"  NEW   {slug:24s} no baseline — run with --update to record one")
    for line in changed:
        print(f"  DIFF  {line}")

    if changed:
        print(f"\n{len(changed)} dashboard(s) changed. Compare {CURRENT} against {BASELINES}.")
        return 1
    print(f"\n{len(dashboards) - len(missing)} unchanged, {len(missing)} without a baseline")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
