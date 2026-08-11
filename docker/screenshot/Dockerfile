# Headless browser for the dashboard screenshot check.
#
# A dedicated image rather than the Superset one: Superset 6.x stopped bundling
# Chromium, and the fonts have to be identical on every machine or an unchanged
# dashboard diffs against its baseline purely because the host renders text
# differently. Pinning the whole browser+font stack here is the point.
ARG PLAYWRIGHT_IMAGE=mcr.microsoft.com/playwright/python:v1.62.0-noble
FROM ${PLAYWRIGHT_IMAGE}

# The base image ships the browsers and their system libraries but *not* the
# `playwright` Python package, despite the image name — so install it here, at
# the version matching the tag above, or it will not drive the bundled browser
# build. `PLAYWRIGHT_BROWSERS_PATH=/ms-playwright` is already set in the base,
# so this finds those browsers instead of downloading its own.
#
# Pillow is only used to diff two PNGs.
RUN pip install --no-cache-dir \
      "playwright==1.62.0" \
      "pillow==11.0.0"

WORKDIR /work
ENTRYPOINT ["python"]
