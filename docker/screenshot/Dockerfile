# Headless browser for the dashboard screenshot check.
#
# A dedicated image rather than the Superset one: Superset 6.x stopped bundling
# Chromium, and the fonts have to be identical on every machine or an unchanged
# dashboard diffs against its baseline purely because the host renders text
# differently. Pinning the whole browser+font stack here is the point.
ARG PLAYWRIGHT_IMAGE=mcr.microsoft.com/playwright/python:v1.62.0-noble
FROM ${PLAYWRIGHT_IMAGE}

# Pillow is only used to diff two PNGs; Playwright itself ships in the base.
RUN pip install --no-cache-dir "pillow==11.0.0"

WORKDIR /work
ENTRYPOINT ["python"]
