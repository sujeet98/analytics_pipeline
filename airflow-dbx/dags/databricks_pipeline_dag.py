from datetime import datetime
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

JOB_ID_INGEST   = 2048974003987
JOB_ID_AUTOLOAD = 528484002001271
JOB_ID_DBT      = 146394096237950

with DAG(
    dag_id="databricks_end_to_end_portfolio",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # manual
    catchup=False,
    tags=["portfolio","databricks","airflow"],
) as dag:

    ingest = DatabricksRunNowOperator(
        task_id="ingest_bigquery_to_raw",
        databricks_conn_id="databricks_default",
        job_id=JOB_ID_INGEST,
    )

    autoload = DatabricksRunNowOperator(
        task_id="raw_to_bronze_autoloader",
        databricks_conn_id="databricks_default",
        job_id=JOB_ID_AUTOLOAD,
    )

    dbt = DatabricksRunNowOperator(
        task_id="run_dbt_project",
        databricks_conn_id="databricks_default",
        job_id=JOB_ID_DBT,
    )

    ingest >> autoload >> dbt
