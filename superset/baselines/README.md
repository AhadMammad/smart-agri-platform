# Dashboard screenshot baselines

One committed PNG per dashboard, recorded by `make screenshot-dashboards UPDATE=1`
and compared against by `make screenshot-dashboards`.

These exist to catch what neither `test_superset_assets.py` (static graph) nor
`make verify-dashboards` (does every chart return rows?) can see: a label
overlapping the plot, an axis title landing on its tick labels, a legend covering
the series. That is a property of rendered pixels, so it takes a real browser.

They are only comparable against the exact browser and fonts that recorded them,
which is why capture runs inside `docker/screenshot/Dockerfile` against the
pinned `PLAYWRIGHT_IMAGE` rather than on a laptop. Bumping that pin will diff
every dashboard at once and needs a re-record.

Re-record deliberately, never to make a red check go green: a diff is the tool
working. Review the new image against the old one the same way you would review
a code change, then commit it in the same change that caused it.

`current/` is gitignored — it is where a comparison run writes what it just
captured, for eyeballing against these.
