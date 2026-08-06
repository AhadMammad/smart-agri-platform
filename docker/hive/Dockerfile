ARG HIVE_IMAGE=apache/hive:4.0.1
FROM ${HIVE_IMAGE}

# apache/hive ships no PostgreSQL JDBC driver — `/opt/hive/lib` contains only
# hive-jdbc — so a Metastore backed by Postgres dies on startup with
# "java.sql.SQLException: No suitable driver". The driver has to be added.
USER root

ARG POSTGRES_JDBC_VERSION=42.7.4
ADD --chmod=644 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    /opt/hive/lib/postgresql.jar

USER hive
