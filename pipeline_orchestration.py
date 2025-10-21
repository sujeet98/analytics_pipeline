"""
Airflow DAG: the_look_orchestration.py
Purpose: Orchestrate The Look pipeline end-to-end:
- BigQuery -> RAW (S3/UC Volume) via Databricks job (PySpark script)
- RAW -> BRONZE via Databricks Auto Loader
- dbt Core transforms: Silver (staging + intermediate) -> Gold (marts)
- Data tests (dbt)
Notes:
- Replace placeholders (JOB IDs, paths, cluster config) with your environment values.
- Requires Airflow provider packages: apache-airflow-providers-databricks, apache-airflow-providers-amazon, apache-airflow-providers-cncf-kubernetes (optional), apache-airflow-providers-google (optional).
"""

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3PrefixSensor
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator

# -----------------------------
# Config (edit for your env)
# -----------------------------
DATABRICKS_CONN_ID = "databricks_default"   # Airflow Connection (token-based preferred)
AWS_CONN_ID = "aws_default"                 # Airflow Connection with S3 read perms
RAW_VOLUME_S3_PREFIX = "s3://YOUR_BUCKET/Volumes/.../raw_thelook_files"
RAW_TABLE_FOR_SENSOR = "orders"             # e.g., 'orders', 'order_items', 'events' etc.
DBT_PROJECT_DIR = "/opt/airflow/dags/the-look-dbt"  # path in scheduler/worker image or volume
DBT_TARGET = "dev"                          # dbt target profile name
DBT_VARS = "{catalog_name: sujeet_data_analytics_workspace, silver_schema: silver_dev, gold_schema: gold_dev}"

# If you have pre-created Databricks Jobs in the workspace, prefer RunNow to decouple infra from Airflow:
USE_EXISTING_DATABRICKS_JOBS = False
INGEST_JOB_ID = 1234        # <-- replace if using DatabricksRunNowOperator
RAW_TO_BRONZE_JOB_ID = 5678 # <-- replace if using DatabricksRunNowOperator

# Otherwise, submit ephemeral runs with inline configs (set USE_EXISTING_DATABRICKS_JOBS=False)
# Example clusters & tasks below are intentionally minimal—tune for your workspace.
INGEST_SUBMIT_JSON = {
    "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.xlarge",  # replace with workspace node type
        "num_workers": 2,
        "data_security_mode": "SINGLE_USER"  # or "USER_ISOLATION" depending on workspace
    },
    "spark_python_task": {
        "python_file": "dbfs:/FileStore/jobs/ingest_bigquery_to_raw.py",
        "parameters": []
    },
    "timeout_seconds": 60 * 60
}
RAW_TO_BRONZE_SUBMIT_JSON = {
    "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
        "data_security_mode": "SINGLE_USER"
    },
    "spark_python_task": {
        "python_file": "dbfs:/FileStore/jobs/raw_to_bronze_autoloader.py",
        "parameters": []
    },
    "timeout_seconds": 60 * 60
}

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    # Optional SLAs: each task should normally finish within X minutes
    # "sla": timedelta(minutes=90),
}

with DAG(
    dag_id="the_look_orchestration",
    description="End-to-end orchestration for The Look (Databricks + dbt)",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/30 * * * *",  # run every 30 minutes; adjust to your cadence
    catchup=False,
    max_active_runs=1,
    tags=["the_look", "databricks", "dbt", "ecommerce"],
) as dag:

    # 1) Ingest BigQuery -> RAW (S3/UC Volume) via Databricks
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

    # Optional: wait until RAW partition for today's date exists for a representative table
    # Uses Airflow's S3PrefixSensor to detect new files (partition path is YYYY-MM-DD from {{ ds }})
    wait_for_raw_partition = S3PrefixSensor(
        task_id="wait_for_raw_partition",
        aws_conn_id=AWS_CONN_ID,
        bucket_key=f"{RAW_VOLUME_S3_PREFIX}/{RAW_TABLE_FOR_SENSOR}/ingest_date={{ ds }}/",
        wildcard_match=False,
        poke_interval=60,
        timeout=60 * 30,
        mode="reschedule",
        soft_fail=True,  # don't block the pipeline if sensor can't see Volume path
    )

    # 2) RAW -> BRONZE via Databricks Auto Loader (availableNow batch-like run)
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

    # 3) dbt Silver (staging + intermediate)
    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt run --target {{ params.dbt_target }} "
            "-s staging+ intermediate "
            "--vars '{{ params.dbt_vars }}'"
        ),
        params={
            "dbt_dir": DBT_PROJECT_DIR,
            "dbt_target": DBT_TARGET,
            "dbt_vars": DBT_VARS,
        },
        env={
            # Provide any secrets via env or Airflow Connections-backed env injection
            # "DBT_PROFILES_DIR": "/opt/airflow/dags/the-look-dbt/profiles",
        },
    )

    # 4) dbt Gold (marts)
    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt run --target {{ params.dbt_target }} "
            "-s marts "
            "--vars '{{ params.dbt_vars }}'"
        ),
        params={
            "dbt_dir": DBT_PROJECT_DIR,
            "dbt_target": DBT_TARGET,
            "dbt_vars": DBT_VARS,
        },
    )

    # 5) Data tests (dbt)
    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt test --target {{ params.dbt_target }} "
            "-s tag:critical,staging+,marts "
            "--vars '{{ params.dbt_vars }}'"
        ),
        params={
            "dbt_dir": DBT_PROJECT_DIR,
            "dbt_target": DBT_TARGET,
            "dbt_vars": DBT_VARS,
        },
    )

    # (Optional) 6) Build docs (handy for CI artifacts)
    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=(
            "cd {{ params.dbt_dir }} && "
            "dbt docs generate --target {{ params.dbt_target }} "
            "--vars '{{ params.dbt_vars }}'"
        ),
        params={
            "dbt_dir": DBT_PROJECT_DIR,
            "dbt_target": DBT_TARGET,
            "dbt_vars": DBT_VARS,
        },
        trigger_rule="all_done",
    )

    # -----------------------------
    # Task Graph
    # -----------------------------
    ingest_bigquery_to_raw >> wait_for_raw_partition >> raw_to_bronze
    raw_to_bronze >> dbt_silver >> dbt_gold >> dbt_tests >> dbt_docs

