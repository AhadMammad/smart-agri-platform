ARG SUPERSET_IMAGE=apache/superset:6.1.0
FROM ${SUPERSET_IMAGE}

USER root

# clickhouse-connect is the driver behind the `clickhousedb://` SQLAlchemy URI;
# psycopg2 is what Superset reaches its own metadata database with. Pin
# clickhouse-connect in step with the ClickHouse server tag in .env — Superset
# 6.1 also constrains it to >=0.13.0,<1.0.
#
# Installed into /app/.venv explicitly. Superset runs from that virtualenv, but
# a bare `pip install` targets the system interpreter instead, so the drivers
# land somewhere nothing imports from and the app dies on boot with
# `ModuleNotFoundError: No module named 'psycopg2'`.
RUN uv pip install --no-cache-dir --python /app/.venv/bin/python \
      "clickhouse-connect==0.13.0" \
      "psycopg2-binary==2.9.10"

COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap.sh /app/bootstrap.sh
RUN chmod +x /app/bootstrap.sh

USER superset

ENV PYTHONPATH=/app/pythonpath
ENTRYPOINT ["/app/bootstrap.sh"]
