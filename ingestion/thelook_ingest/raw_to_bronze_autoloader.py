# raw_to_bronze_autoloader.py
#
# Purpose:
#   Promote immutable RAW Parquet files (partitioned by ingest_date/run_ts)
#   from a Unity Catalog Volume into **managed Delta** tables in the Bronze schema,
#   using Databricks Auto Loader (cloudFiles).
#
# Execution model:
#   - .trigger(availableNow=True) → batch-like: process what's pending and exit.
#   - Sequentially processes each table (resource- and cost-friendly).
#
# Storage model:
#   - RAW files live under a UC **Volume** (e.g., /Volumes/<CAT>/raw/raw_files/thelook/<table>).
#   - Auto Loader metadata (schema + checkpoints) also saved under the same Volume
#     so Serverless can write without NAT/IAM instance profiles.
#   - Bronze are **managed UC Delta tables** (no S3 folder/Volume needed for Bronze).
#
# First run behavior:
#   - includeExistingFiles=true → backfills all existing RAW files once.
#   - Subsequent runs only pick up new files thanks to the checkpoint.

import os
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.dbutils import DBUtils

# ---------------------------------------
# 0) Config — set via env or defaults
# ---------------------------------------
CATALOG        = os.getenv("UC_CATALOG", "sujeet_data_analytics_workspace")
BRONZE_SCHEMA  = os.getenv("BRONZE_SCHEMA", "bronze_dev")  # swap to bronze_prod in prod

# RAW Volume root (where the ingestion job wrote the Parquet files)
# Example existing Volume: /Volumes/<CATALOG>/raw/raw_files
RAW_VOLUME_PATH = os.getenv("RAW_VOLUME_PATH", f"/Volumes/{CATALOG}/raw/raw_files")

# Source namespace under RAW (keeps sources cleanly separated)
# e.g., thelook, stripe, salesforce, etc.
SOURCE_NAME    = os.getenv("SOURCE_NAME", "thelook")
RAW_SOURCE_DIR = f"{RAW_VOLUME_PATH}/{SOURCE_NAME}"

# Auto Loader metadata (schemas + checkpoints) placed under the RAW Volume as well
# to keep the whole pipeline Serverless-friendly.
SCHEMA_BASE    = f"{RAW_SOURCE_DIR}/_autoloader/schemas/bronze"
CHECKPT_BASE   = f"{RAW_SOURCE_DIR}/_autoloader/checkpoints/bronze"

# The RAW tables to promote to Bronze
TABLES: List[str] = [
    "orders",
    "order_items",
    "events",
    "inventory_items",
    "users",
    "products",
    "distribution_centers",
]

# Backfill existing files on the first run for each table (checkpoint prevents re-read later)
INCLUDE_EXISTING_FILES = os.getenv("INCLUDE_EXISTING_FILES", "true")  # "true" / "false"


# ---------------------------------------
# 1) Spark session & target schema
# ---------------------------------------
spark = SparkSession.builder.appName("raw-to-bronze-autoloader").getOrCreate()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

dbutils = DBUtils(spark)

def _ensure_dir(path: str) -> None:
    """Idempotent mkdirs on a UC Volume path."""
    try:
        dbutils.fs.mkdirs(path)
    except Exception:
        pass  # harmless if it already exists


# ---------------------------------------
# 2) Type hygiene for system columns
# ---------------------------------------
def normalize_system_cols(df: DataFrame) -> DataFrame:
    """
    Enforce stable types for our four system columns so append never fails:
      - ingest_date   : STRING
      - run_ts        : STRING
      - ingest_ts_utc : TIMESTAMP
      - source_table  : STRING
    If any are missing, add as NULL with the correct type.
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


# -------------------------------------------------------
# 3) One table: RAW Volume → Auto Loader → Bronze table
# -------------------------------------------------------
def run_one_table(table: str):
    """
    Reads RAW Parquet from:
        {RAW_SOURCE_DIR}/{table}/**  (partition dirs: ingest_date=.../run_ts=...)
    Writes to managed UC Delta table:
        {CATALOG}.{BRONZE_SCHEMA}.{table}
    Uses Auto Loader with schema + checkpoint stored under the RAW Volume.
    """
    raw_root   = f"{RAW_SOURCE_DIR}/{table}"
    schema_loc = f"{SCHEMA_BASE}/{table}"
    checkpoint = f"{CHECKPT_BASE}/{table}"
    target_tbl = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"

    # Make sure our metadata dirs exist on the Volume (serverless-friendly)
    _ensure_dir(schema_loc)
    _ensure_dir(checkpoint)

    print(f"\n=== RAW → BRONZE (Auto Loader): {table} ===")
    print(f"Source RAW dir : {raw_root}")
    print(f"Schema location: {schema_loc}")
    print(f"Checkpoint     : {checkpoint}")
    print(f"Target UC table: {target_tbl}")

    # Build Auto Loader reader:
    # - cloudFiles.format=parquet → our RAW files
    # - schemaLocation           → where AL persists inferred schema state
    # - includeExistingFiles     → backfill all historical files on first run
    # - partitionColumns         → pick up ingest_date/run_ts from folder names
    # - schemaEvolutionMode      → allow new columns to appear later
    reader = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "parquet")
             .option("cloudFiles.schemaLocation", schema_loc)
             .option("cloudFiles.includeExistingFiles", INCLUDE_EXISTING_FILES)
             .option("cloudFiles.partitionColumns", "ingest_date,run_ts")
             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
             # We’re reading from a UC Volume path, so no S3 creds/IAM needed on Serverless.
    )

    # Auto Loader recursively discovers partitioned subfolders
    df_raw = reader.load(raw_root)

    # Bronze is a faithful copy (plus type hygiene for the 4 system cols)
    df_bronze = normalize_system_cols(df_raw)

    # Write stream to a UC managed table:
    # - checkpointLocation: tracks progress → exactly-once semantics across runs
    # - mergeSchema=true  : allow new columns to be added
    # - trigger(availableNow=True): process outstanding data and exit
    query = (
        df_bronze.writeStream
                 .option("checkpointLocation", checkpoint)
                 .option("mergeSchema", "true")
                 .trigger(availableNow=True)
                 .toTable(target_tbl)   # creates the managed table if missing
    )

    query.awaitTermination()
    print(f"{table}: Auto Loader availableNow run completed.")


# ---------------------------------------
# 4) Run all tables sequentially
# ---------------------------------------
if __name__ == "__main__":
    # Optional small tuning for tiny clusters to save cost
    spark.conf.set("spark.sql.shuffle.partitions", "64")

    # Make sure the base metadata folders exist
    _ensure_dir(SCHEMA_BASE)
    _ensure_dir(CHECKPT_BASE)

    for t in TABLES:
        run_one_table(t)

    print("\nAll tables processed.")
