# raw_to_bronze_autoloader.py
#
# ──────────────────────────────────────────────────────────────────────────────
# PURPOSE
# ──────────────────────────────────────────────────────────────────────────────
# Promote immutable RAW Parquet files (written by your BQ→RAW job) into
# Unity Catalog Delta tables in the bronze schema using Databricks Auto Loader.
#
# This script:
#   • Reads from a UC *Volume* path (so it works on Serverless without NAT/IAM).
#   • Uses Auto Loader ("cloudFiles") with exactly-once semantics via checkpoints.
#   • Runs one table at a time with trigger(availableNow=True) (batch-like).
#   • On first run for a table → includeExistingFiles=true (backfill).
#     Subsequent runs → only new files (checkpoint drives this).
#   • Recasts the 4 RAW “system columns” to stable types (defensive).
#
# PREREQS
#   • Volume created: /Volumes/sujeet_data_analytics_workspace/raw/raw_thelook_files
#       ↳ Points to s3://<your-bucket>/raw/thelook
#   • Principal running the job has:
#       - READ FILES / WRITE FILES on that Volume
#       - CREATE/MODIFY on catalog.schema bronze_dev
#
# STORAGE (metadata)
#   • Auto Loader schema & checkpoints are stored under the same Volume:
#       /Volumes/.../raw_thelook_files/_autoloader/_schemas/bronze/<table>
#       /Volumes/.../raw_thelook_files/_autoloader/_checkpoints/bronze/<table>
#     (Keeping metadata alongside the source keeps Serverless happy.)
#
# RUN MODE
#   • Schedule this as a Databricks Job. Small Serverless SQL Warehouse or
#     small Serverless All-Purpose compute both work; start tiny for costs.
# ──────────────────────────────────────────────────────────────────────────────

import os
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils

# ------------------------------------------------------------------------------
# 0) CONFIG — tweak safely (or set via environment variables in your Job)
# ------------------------------------------------------------------------------
CATALOG       = os.getenv("UC_CATALOG", "sujeet_data_analytics_workspace")
BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze_dev")

# Your RAW Volume (already created)
RAW_VOLUME = os.getenv(
    "RAW_VOLUME_PATH",
    "/Volumes/sujeet_data_analytics_workspace/raw/raw_thelook_files"
).rstrip("/")

# Tables to ingest from RAW → BRONZE
TABLES: List[str] = [
    "orders",
    "order_items",
    "events",
    "inventory_items",
    "users",
    "products",
    "distribution_centers",
]

# Optional pacing on first big backfills; lower if you’re cost-sensitive
MAX_FILES_PER_TRIGGER = os.getenv("MAX_FILES_PER_TRIGGER")  # e.g. "500" or unset

# ------------------------------------------------------------------------------
# 1) SPARK SESSION + ensure target schema exists
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("raw-to-bronze-autoloader").getOrCreate()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

dbutils = DBUtils(spark)

# ------------------------------------------------------------------------------
# 2) Helpers
# ------------------------------------------------------------------------------
def normalize_system_cols(df: DataFrame) -> DataFrame:
    """
    Defensive casting so appends never fail if upstream types wobble.
    RAW guarantees these columns exist, but we cast anyway:
      - ingest_date    : string
      - run_ts         : string
      - ingest_ts_utc  : timestamp
      - source_table   : string
    """
    want = {
        "ingest_date":   "string",
        "run_ts":        "string",
        "ingest_ts_utc": "timestamp",
        "source_table":  "string",
    }
    out = df
    for col, dtype in want.items():
        if col in out.columns:
            out = out.withColumn(col, F.col(col).cast(dtype))
        else:
            out = out.withColumn(col, F.lit(None).cast(dtype))
    return out


def path_exists(p: str) -> bool:
    """Minimal 'exists' check for checkpoints/schemas in the Volume."""
    try:
        dbutils.fs.ls(p)
        return True
    except Exception:
        return False


def run_one_table(table: str):
    """
    RAW (files) → BRONZE (Delta/UC) for a single table using Auto Loader.

    Source:
      /Volumes/.../raw_thelook_files/<table>/**

    Target table:
      <CATALOG>.<BRONZE_SCHEMA>.<table>
    """
    source_root = f"{RAW_VOLUME}/{table}"
    schema_loc  = f"{RAW_VOLUME}/_autoloader/_schemas/bronze/{table}"
    checkpoint  = f"{RAW_VOLUME}/_autoloader/_checkpoints/bronze/{table}"
    target_tbl  = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"

    print(f"\n=== RAW → BRONZE: {table} ===")
    print(f"Source       : {source_root}")
    print(f"Schema store : {schema_loc}")
    print(f"Checkpoint   : {checkpoint}")
    print(f"Target table : {target_tbl}")

    # First run heuristic:
    #   If there is no checkpoint dir yet, we backfill existing files.
    include_existing = "true" if not path_exists(checkpoint) else "false"

    # Build Auto Loader reader
    reader = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "parquet")        # RAW files are Parquet
             .option("cloudFiles.schemaLocation", schema_loc)
             .option("cloudFiles.includeExistingFiles", include_existing)
             # Partition columns are encoded in folder names by the RAW writer
             .option("cloudFiles.partitionColumns", "ingest_date,run_ts")
             # Be tolerant to new columns over time
             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
             # (Nice to have) capture unexpected fields
             .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    )

    if MAX_FILES_PER_TRIGGER:
        reader = reader.option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)

    # Let Auto Loader recursively discover partitions
    df_raw = reader.load(source_root)

    # Light hygiene (stable types on system cols)
    df_bronze = normalize_system_cols(df_raw)

    # Write to a UC Delta table with exactly-once guarantees
    query = (
        df_bronze.writeStream
                 .option("checkpointLocation", checkpoint)
                 .option("mergeSchema", "true")           # allow new columns
                 .trigger(availableNow=True)              # process what's available and stop
                 .toTable(target_tbl)                     # creates table if needed
    )

    query.awaitTermination()
    print(f"{table}: done (Auto Loader availableNow run completed).")


# ------------------------------------------------------------------------------
# 3) Run all tables sequentially (safe & simple on small clusters)
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    for t in TABLES:
        run_one_table(t)
    print("\nAll tables processed.")
