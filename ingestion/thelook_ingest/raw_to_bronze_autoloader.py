# raw_to_bronze_autoloader.py
#
# Purpose:
#   Move immutable RAW Parquet files in S3 (partitioned by ingest_date/run_ts)
#   into Unity Catalog Delta tables in the bronze schema using Auto Loader.
#
# Run mode:
#   - Uses .trigger(availableNow=True): batch-like; processes what's new and exits.
#   - Sequentially processes one table at a time (resource-friendly on small clusters).
#
# Notes:
#   - On FIRST run, we set includeExistingFiles=true so Auto Loader ingests all existing RAW files.
#   - On SUBSEQUENT runs, the checkpoint prevents re-reading old files—only new files are ingested.
#   - Partition columns are recovered from the folder layout (ingest_date=..., run_ts=...).
#   - We normalize the four "system columns" to stable types (string/string/timestamp/string),
#     which prevents future "cannot safely cast" errors on append.

import sys
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# --------------------------
# 0) Config — adjust safely
# --------------------------
CATALOG      = "sujeet_data_analytics_workspace"
BRONZE_SCHEMA= "bronze_dev"     # or "bronze_prod" in prod
BUCKET       = "analyticsbucketdev-sk" 
RAW_PREFIX   = "raw/thelook"  # e.g. "raw/thelook"

# The RAW tables we want to promote to BRONZE
TABLES: List[str] = [
    "orders",
    "order_items",
    "events",
    "inventory_items",
    "users",
    "products",
    "distribution_centers",
]

# Internal (non-data) locations for Auto Loader metadata
# Keep these OUTSIDE of your data paths:
SCHEMA_BASE  = f"s3://{BUCKET}/_autoloader/_schemas/bronze"
CHECKPT_BASE = f"s3://{BUCKET}/_autoloader/_checkpoints/bronze"

# Optionally switch this OFF after the first successful backfill for each table.
INCLUDE_EXISTING_FILES = "true"  # "true" = backfill historical files on first run


# ------------------------------------------------------------
# 1) Spark session + ensure the target schema exists in UC
# ------------------------------------------------------------
spark = SparkSession.builder.appName("raw-to-bronze-autoloader").getOrCreate()

# Create schema once (idempotent)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")


# ----------------------------------------------------------------
# 2) (Optional) normalize system columns to stable, known types
#     - This keeps appends safe across runs/schema evolution.
# ----------------------------------------------------------------
def normalize_system_cols(df: DataFrame) -> DataFrame:
    """
    Enforce stable types for our four system columns so append never fails:
      - ingest_date   : STRING
      - run_ts        : STRING
      - ingest_ts_utc : TIMESTAMP
      - source_table  : STRING
    (If a column isn't present, add it as NULL of the correct type.)
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


# ----------------------------------------------------------------
# 3) One table run: RAW path -> Auto Loader -> Delta table (BRONZE)
# ----------------------------------------------------------------
def run_one_table(table: str):
    """
    Read from RAW Parquet files under:
        s3://<bucket>/<raw_prefix>/<table>/** (partitioned by ingest_date/run_ts)
    and write to UC Delta table:
        <CATALOG>.<BRONZE_SCHEMA>.<table>
    using Auto Loader with a checkpoint for exactly-once semantics.
    """
    raw_root = f"s3://{BUCKET}/{RAW_PREFIX}/{table}"
    schema_loc = f"{SCHEMA_BASE}/{table}"
    checkpoint = f"{CHECKPT_BASE}/{table}"
    target_tbl = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"

    print(f"\n=== RAW -> BRONZE: {table} ===")
    print(f"Source:     {raw_root}")
    print(f"Checkpoint: {checkpoint}")
    print(f"Schema loc: {schema_loc}")
    print(f"Target:     {target_tbl}")

    # Construct the reader:
    # - format("cloudFiles"): turn on Auto Loader
    # - cloudFiles.format=parquet: our RAW files are Parquet
    # - cloudFiles.schemaLocation: where Auto Loader stores inferred schema state
    # - cloudFiles.includeExistingFiles: backfill existing files on 1st run
    # - cloudFiles.partitionColumns: tell AL to derive cols from folder names
    #   (our RAW writer partitioned by ingest_date=YYYY-MM-DD/run_ts=HHMMSS)
    reader = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "parquet")
             .option("cloudFiles.schemaLocation", schema_loc)
             .option("cloudFiles.includeExistingFiles", INCLUDE_EXISTING_FILES)
             .option("cloudFiles.partitionColumns", "ingest_date,run_ts")
             # Optional evolution: add new columns without failing future appends
             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
             # Optional: listing mode is default (cheap); no SQS/SNS needed
             # .option("cloudFiles.useNotifications", "false")
    )

    # Load the RAW folder (Auto Loader recursively discovers subfolders/partitions)
    df_raw = reader.load(raw_root)

    # Bronze = faithful copy (+ very light hygiene to keep types stable)
    df_bronze = normalize_system_cols(df_raw)

    # Write stream to a UC Delta table.
    # - checkpointLocation: AL keeps "what I've processed" here (exactly-once)
    # - mergeSchema=true: allow new columns to be added later
    # - trigger(availableNow=True): process what's pending, then stop
    query = (
        df_bronze.writeStream
                 .option("checkpointLocation", checkpoint)
                 .option("mergeSchema", "true")
                 .trigger(availableNow=True)
                 .toTable(target_tbl)   # creates table if it doesn't exist
    )

    # Wait for this table's backfill/incremental run to finish before moving on
    query.awaitTermination()
    print(f"{table}: done (Auto Loader availableNow run completed).")


# ---------------------------------------------
# 4) Run all tables sequentially (safe & simple)
# ---------------------------------------------
if __name__ == "__main__":
    for t in TABLES:
        run_one_table(t)

    print("\nAll tables processed.")
