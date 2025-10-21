"""
Airflow DAG: the_look_orchestration.py
Purpose: Orchestrate The Look pipeline end-to-end:
- BigQuery -> RAW (S3/UC Volume) via Databricks job (PySpark script)
- RAW -> BRONZE via Databricks Auto Loader
- dbt Core transforms:
    * Silver: staging + intermediate per-source
    * Snapshots over intermediate per-source (build SCD inputs)
    * Core (unioned + SCD2 dims)
    * Gold (marts)
- Data tests + (optional) Elementary prep/monitoring

Notes:
- Replace placeholders (JOB IDs, paths, cluster config, DBT_PROJECT_DIR, PROFILES_DIR) with your env.
- Providers: apache-airflow-providers-databricks, -amazon, -google (optional), -cncf-kubernetes (optional).
"""

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3PrefixSensor
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
    DatabricksRunNowOperator,
)

# -----------------------------
# Config (edit for your env)
# -----------------------------
# Airflow Connections
DATABRICKS_CONN_ID = "databricks_default"  # token-based recommended
AWS_CONN_ID = "aws_default"

# Volume/S3 “sensor” path (soft-fail if you can’t see UC Volume via S3)
RAW_VOLUME_S3_PREFIX = "s3://YOUR_BUCKET/Volumes/.../raw_thelook_files"
RAW_TABLE_FOR_SENSOR = "orders"  # e.g. 'orders', 'order_items', 'events'

# dbt project
DBT_PROJECT_DIR = "/opt/airflow/dags/look_dbt"     # path inside your image/volume
DBT_PROFILES_DIR = "/opt/airflow/dags/look_dbt"    # or another mounted path with profiles.yml
DBT_TARGET = "dev"                                 # dev | prod | <your target>

# dbt vars (YAML string). Keep it minimal; your dbt_project.yml handles env-based switching.
DBT_VARS = "catalog_name: sujeet_data_analytics_workspace"

# Optional: Elementary CLI availability (if `elementary-data` is installed in the image)
USE_ELEMENTARY_CLI = False

# If you maintain Databricks Jobs in workspace, set True and supply JOB IDs:
USE_EXISTING_DATABRICKS_JOBS = False
INGEST_JOB_ID = 1234        # replace if using RunNow
RAW_TO_BRONZE_JOB_ID = 5678 # replace if using RunNow

# Otherwise, submit ephemeral runs (adjust to your workspace)
INGEST_SUBMIT_JSON = {
    "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.xlarge",      # replace
        "num_workers": 2,
        "data_security_mode": "SINGLE_USER",
    },
    "spark_python_task": {
        "python_file": "dbfs:/FileStore/jobs/ingest_bigquery_to_raw.py",
        "parameters": [],
    },
    "timeout_seconds": 60 * 60,
}
RAW_TO_BRONZE_SUBMIT_JSON = {
    "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
        "data_security_mode": "SINGLE_USER",
    },
    "spark_python_task": {
        "python_file": "dbfs:/FileStore/jobs/raw_to_bronze_autoloader.py",
        "parameters": [],
    },
    "timeout_seconds": 60 * 60,
}

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="the_look_orchestration",
    description="End-to-end orchestration for The Look (Databricks + dbt)",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 * * * *",  # hourly; adjust as needed
    catchup=False,
    max_active_runs=1,
    tags=["the_look", "databricks", "dbt", "ecommerce"],
) as dag:

    # 0) Ingest BigQuery -> RAW (S3/UC Volume) via Databricks
    if USE_EXISTING_DATABRICKS_JOBS:
        ingest_bigquery_to_raw = DatabricksRunNowOperator(
            task_id="ingest_bigquery_to_raw",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=INGEST_JOB_ID,
            notebook_params={"run_reason": "airflow_scheduled"},
        )
    else:
        ingest_bigquery_to_raw = DatabricksSubmitRunOperator(
            task_id="ingest_bigquery_to_raw",
            databricks_conn_id=DATABRICKS_CONN_ID,
            json=INGEST_SUBMIT_JSON,
        )

    # Optional: wait for new RAW files (soft_fail True since UC Volumes may not be visible via S3)
    wait_for_raw_partition = S3PrefixSensor(
        task_id="wait_for_raw_partition",
        aws_conn_id=AWS_CONN_ID,
        bucket_key=f"{RAW_VOLUME_S3_PREFIX}/{RAW_TABLE_FOR_SENSOR}/ingest_date={{ ds }}/",
        wildcard_match=False,
        poke_interval=60,
        timeout=60 * 30,
        mode="reschedule",
        soft_fail=True,
    )

    # 1) RAW -> BRONZE via Databricks Auto Loader
    if USE_EXISTING_DATABRICKS_JOBS:
        raw_to_bronze = DatabricksRunNowOperator(
            task_id="raw_to_bronze",
            databricks_conn_id=DATABRICKS_CONN_ID,
            job_id=RAW_TO_BRONZE_JOB_ID,
            notebook_params={"run_reason": "airflow_scheduled"},
        )
    else:
        raw_to_bronze = DatabricksSubmitRunOperator(
            task_id="raw_to_bronze",
            databricks_conn_id=DATABRICKS_CONN_ID,
            json=RAW_TO_BRONZE_SUBMIT_JSON,
        )

    # -----------------------------
    # dbt: Silver (staging)
    # -----------------------------
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt run --target {{ params.dbt_target }} "
            "--select 'path:models/staging'"
            " --vars '{{ params.dbt_vars }}'"
        ),
        params={"dbt_dir": DBT_PROJECT_DIR, "dbt_target": DBT_TARGET, "dbt_vars": DBT_VARS},
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    # dbt: Intermediate per-source (exclude core)
    dbt_intermediate_sources = BashOperator(
        task_id="dbt_intermediate_sources",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt run --target {{ params.dbt_target }} "
            "--select 'path:models/intermediate' "
            "--exclude 'tag:core' "
            "--vars '{{ params.dbt_vars }}'"
        ),
        params={"dbt_dir": DBT_PROJECT_DIR, "dbt_target": DBT_TARGET, "dbt_vars": DBT_VARS},
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    # dbt: Snapshots over intermediate (users/products/DCs)
    dbt_snapshots = BashOperator(
        task_id="dbt_snapshots",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt snapshot --target {{ params.dbt_target }} "
            "--select 'path:snapshots' "
            "--vars '{{ params.dbt_vars }}'"
        ),
        params={"dbt_dir": DBT_PROJECT_DIR, "dbt_target": DBT_TARGET, "dbt_vars": DBT_VARS},
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    # dbt: Core (unioned + SCD2 dims built from snapshots)
    dbt_core = BashOperator(
        task_id="dbt_core",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt run --target {{ params.dbt_target }} "
            "--select 'path:models/intermediate tag:core' "
            "--vars '{{ params.dbt_vars }}'"
        ),
        params={"dbt_dir": DBT_PROJECT_DIR, "dbt_target": DBT_TARGET, "dbt_vars": DBT_VARS},
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    # dbt: Gold (marts)
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt run --target {{ params.dbt_target }} "
            "--select 'path:models/marts' "
            "--vars '{{ params.dbt_vars }}'"
        ),
        params={"dbt_dir": DBT_PROJECT_DIR, "dbt_target": DBT_TARGET, "dbt_vars": DBT_VARS},
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    # dbt: Tests (critical + layer sanity)
    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt test --target {{ params.dbt_target }} "
            "--select 'tag:critical, path:models/staging+, path:models/marts' "
            "--vars '{{ params.dbt_vars }}'"
        ),
        params={"dbt_dir": DBT_PROJECT_DIR, "dbt_target": DBT_TARGET, "dbt_vars": DBT_VARS},
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    # Optional: dbt source freshness for key sources
    dbt_freshness = BashOperator(
        task_id="dbt_freshness",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt source freshness --target {{ params.dbt_target }} "
            "--select 'source:look' "
            "--vars '{{ params.dbt_vars }}'"
        ),
        params={"dbt_dir": DBT_PROJECT_DIR, "dbt_target": DBT_TARGET, "dbt_vars": DBT_VARS},
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        trigger_rule="all_done",
    )

    # -----------------------------
    # Elementary (optional)
    # -----------------------------
    # Prepare EDR tables (package models). Your dbt_project.yml already routes them to edr/edr_prod.
    edr_prepare = BashOperator(
        task_id="edr_prepare",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt run --target {{ params.dbt_target }} "
            "--select 'elementary' "
            "--vars '{{ params.dbt_vars }}'"
        ),
        params={"dbt_dir": DBT_PROJECT_DIR, "dbt_target": DBT_TARGET, "dbt_vars": DBT_VARS},
        env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
    )

    # Monitoring run:
    # If Elementary CLI is baked into the image, use it (richer alerts, Slack, etc.).
    # Otherwise, as a basic fallback run the EDR dbt models/tests (less featureful).
    if USE_ELEMENTARY_CLI:
        edr_monitor = BashOperator(
            task_id="edr_monitor",
            bash_command=(
                # requires `pip install elementary-data`
                "cd {{ params.dbt_dir }} && "
                "edr monitor --target {{ params.dbt_target }} "
                "--project-dir {{ params.dbt_dir }} "
                "--profiles-dir {{ params.profiles_dir }} "
                "--vars '{{ params.dbt_vars }}'"
            ),
            params={
                "dbt_dir": DBT_PROJECT_DIR,
                "profiles_dir": DBT_PROFILES_DIR,
                "dbt_target": DBT_TARGET,
                "dbt_vars": DBT_VARS,
            },
            env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
            trigger_rule="all_done",
        )
    else:
        # Fallback: build EDR package (creates artifacts, runs package tests)
        edr_monitor = BashOperator(
            task_id="edr_monitor",
            bash_command=(
                "cd {{ params.dbt_dir }} && "
                "dbt build --target {{ params.dbt_target }} "
                "--select 'elementary' "
                "--vars '{{ params.dbt_vars }}'"
            ),
            params={"dbt_dir": DBT_PROJECT_DIR, "dbt_target": DBT_TARGET, "dbt_vars": DBT_VARS},
            env={"DBT_PROFILES_DIR": DBT_PROFILES_DIR},
            trigger_rule="all_done",
        )

    # -----------------------------
    # Task Graph
    # -----------------------------
    ingest_bigquery_to_raw >> wait_for_raw_partition >> raw_to_bronze

    raw_to_bronze >> dbt_staging >> dbt_intermediate_sources
    dbt_intermediate_sources >> dbt_snapshots >> dbt_core
    dbt_core >> dbt_marts >> dbt_tests
    dbt_tests >> dbt_freshness

    # EDR can run after core/marts are in place (monitors depend on built relations)
    [dbt_core, dbt_marts] >> edr_prepare >> edr_monitor
