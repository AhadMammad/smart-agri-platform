ARG AIRFLOW_IMAGE=apache/airflow:2.10.5-python3.12
FROM ${AIRFLOW_IMAGE}

# Airflow itself stays thin: it only orchestrates. Every ETL dependency lives in
# the smart-agri-etl image, which DockerOperator launches as a sibling container.
# The provider below is what lets Airflow do that.
USER airflow
RUN pip install --no-cache-dir \
      "apache-airflow-providers-docker==3.14.1" \
      "apache-airflow-providers-postgres==5.14.0" \
      "pytest==8.3.4"

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow
