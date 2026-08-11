ARG SUPERSET_IMAGE=apache/superset:4.1.1
FROM ${SUPERSET_IMAGE}

USER root

# clickhouse-connect is the driver behind the `clickhousedb://` SQLAlchemy URI.
# Keep its version aligned with the ClickHouse server tag pinned in .env.
RUN pip install --no-cache-dir \
      "clickhouse-connect==0.13.0" \
      "psycopg2-binary==2.9.10"

COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap.sh /app/bootstrap.sh
RUN chmod +x /app/bootstrap.sh

USER superset

ENV PYTHONPATH=/app/pythonpath
ENTRYPOINT ["/app/bootstrap.sh"]
