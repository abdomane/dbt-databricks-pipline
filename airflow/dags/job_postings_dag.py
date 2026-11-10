"""
Airflow DAG: Production Job Postings Pipeline (Databricks + dbt via Astronomer Cosmos)

Architecture:
1. Check if a Databricks ingestion Job ID is configured (`start_or_skip_raw_load`):
   - If > 0: triggers `scripts/load_raw.py` on Databricks via `DatabricksRunNowOperator`.
   - If 0: skips raw ingestion (useful when `raw.jobs` is already loaded).
2. Runs the entire dbt project (`analytics/`) as a granular `DbtTaskGroup`:
   - Seeds (`skill_categories`)
   - Staging views (`stg_job_postings`, `stg_job_skills`) + tests
   - SCD2 Snapshot (`job_postings_snapshot`)
   - Intermediate (`int_skill_observations`)
   - Star-schema Marts (`fct_job_postings`, `dim_company`, `dim_skills`, `bridge_job_skills`) + tests
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.trigger_rule import TriggerRule
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
    TestBehavior,
)
from cosmos.profiles import DatabricksTokenProfileMapping

# ---------------------------------------------------------------------------
# 1. Paths & Environment Configuration
# ---------------------------------------------------------------------------
# Inside Docker, `../analytics` is mounted at `/opt/airflow/dbt/analytics`.
# Fallback to relative path so the file can also be inspected/parsed outside Docker.
DBT_PROJECT_PATH = Path(
    os.getenv("DBT_PROJECT_PATH", "/opt/airflow/dbt/analytics")
)
DBT_EXECUTABLE_PATH = os.getenv(
    "DBT_EXECUTABLE_PATH", "/opt/airflow/dbt_venv/bin/dbt"
)
DBT_TARGET_CATALOG = os.getenv("DBT_TARGET_CATALOG", "dev")
DATABRICKS_CONN_ID = "databricks_default"

# ---------------------------------------------------------------------------
# 2. Cosmos ProfileConfig (Zero-Secret profiles.yml generation)
# ---------------------------------------------------------------------------
# Reads Host, Token, and SQL Warehouse HTTP Path from Airflow's `databricks_default`
# connection and dynamically builds a temporary profiles.yml for each task.
profile_config = ProfileConfig(
    profile_name="databricks",  # Must match `profile: databricks` in analytics/dbt_project.yml
    target_name=DBT_TARGET_CATALOG,
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id=DATABRICKS_CONN_ID,
        profile_args={
            "catalog": DBT_TARGET_CATALOG,
            "schema": "default",
            "threads": 4,
        },
    ),
)

# ---------------------------------------------------------------------------
# 3. DAG Definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "Abder",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="job_postings_databricks_pipeline",
    description="End-to-end ELT: Databricks raw ingestion + dbt Star Schema & SCD2 Snapshots via Cosmos",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",  # Runs nightly at 02:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "databricks", "cosmos", "star-schema"],
)
def job_postings_databricks_pipeline():

    # Step 1A: Branching — decide whether to trigger the Databricks raw ingestion job
    @task.branch(task_id="check_raw_ingestion_job")
    def check_raw_ingestion_job() -> str:
        job_id = int(os.getenv("AIRFLOW_VAR_DATABRICKS_LOAD_RAW_JOB_ID", "0"))
        if job_id > 0:
            return "load_raw_job_postings"
        return "skip_raw_ingestion"

    # Step 1B: Trigger existing Databricks Job (`scripts/load_raw.py`) if Job ID is provided
    load_raw = DatabricksRunNowOperator(
        task_id="load_raw_job_postings",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=os.getenv("AIRFLOW_VAR_DATABRICKS_LOAD_RAW_JOB_ID", "0"),
    )

    skip_raw = EmptyOperator(task_id="skip_raw_ingestion")

    # Join point before starting dbt transformations
    ready_for_dbt = EmptyOperator(
        task_id="ready_for_dbt",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Step 2: Cosmos DbtTaskGroup — turns every dbt model/seed/snapshot into Airflow tasks
    dbt_analytics = DbtTaskGroup(
        group_id="dbt_analytics",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        render_config=RenderConfig(
            # Parse YAML/SQL files directly (<1s) without spawning a heavy `dbt ls` subprocess
            load_method=LoadMode.CUSTOM,
            # Run a model's schema/data tests immediately after the model finishes!
            # If a staging test fails, downstream marts won't run on bad data.
            test_behavior=TestBehavior.AFTER_EACH,
            emit_datasets=True,
        ),
    )

    pipeline_complete = EmptyOperator(task_id="pipeline_complete")

    # -----------------------------------------------------------------------
    # 4. Task Dependencies (Orchestration Flow)
    # -----------------------------------------------------------------------
    branch = check_raw_ingestion_job()
    branch >> [load_raw, skip_raw] >> ready_for_dbt >> dbt_analytics >> pipeline_complete


job_postings_databricks_pipeline()
