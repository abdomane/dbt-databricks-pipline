FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 1. Create an isolated virtual environment for dbt-databricks
#    (best practice with Cosmos to prevent protobuf/pydantic clashes with Airflow)
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-databricks>=1.8.0,<2.0.0"

# 2. Install Airflow providers & Astronomer Cosmos (pinned to current Airflow version)
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /opt/airflow/requirements.txt

