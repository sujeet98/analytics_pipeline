# ingest_bigquery_to_raw.py
#
# ──────────────────────────────────────────────────────────────────────────────
# GOAL
# ──────────────────────────────────────────────────────────────────────────────
# Pull data incrementally from BigQuery public dataset
#   bigquery-public-data.thelook_ecommerce
# and write **append-only Parquet files** into a Unity Catalog **External Volume**
# that maps to s3://.../raw/thelook (RAW landing zone).
#
# Files are partitioned as:
#   {RAW_VOLUME}/{table}/ingest_date=YYYY-MM-DD/run_ts=HHMMSS/part-*.snappy.parquet
#
# We also maintain per-table cursor JSON at:
#   {RAW_VOLUME}/_state/{table}.json
#
# Schedule: Every 10–30 minutes (max concurrent runs = 1).
#
# Prereqs (already done per your setup):
#   1) External Location 'raw_thelook' → s3://analyticsbucketdev-sk/raw
#   2) External Volume  'raw_thelook_files' (in catalog.schema = sujeet_data_analytics_workspace.raw)
#      Volume path: /Volumes/sujeet_data_analytics_workspace/raw/raw_thelook_files
#   3) Grants: READ FILES/WRITE FILES on that Volume for the job principal
#   4) Spark BigQuery connector available
#
# Tips:
#   • First run will backfill (from epoch) using a grace window to catch late rows.
#   • Next runs advance a high-watermark cursor and only fetch deltas.
#   • Auto Loader job will read from the same Volume path into Bronze tables.
# 
# RUN MODE
#   • Schedule this as a Databricks Job.
# ──────────────────────────────────────────────────────────────────────────────

import os, datetime, json, uuid
from typing import Optional, List, Dict
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

from ingestion.thelook_ingest.config import (
    get_project,            # returns GCP project for BQ billing
    get_bq_auth_options,    # returns dict of Spark BigQuery connector auth options
)

# ──────────────────────────────────────────────────────────────────────────────
# 0) RUNTIME CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────

# Where we will WRITE the RAW Parquet files and READ/WRITE cursor JSON.
RAW_VOLUME = os.getenv(
    "RAW_VOLUME_PATH",
    "/Volumes/sujeet_data_analytics_workspace/raw/raw_thelook_files"
).rstrip("/")

# BigQuery billing project & connector auth
PROJECT_ID = get_project()
BQ_AUTH    = get_bq_auth_options() 

# Run metadata (UTC so partitions are stable irrespective of cluster TZ)
TODAY  = datetime.date.today().isoformat()                               # "YYYY-MM-DD"
RUN_TS = datetime.datetime.now(datetime.timezone.utc).strftime("%H%M%S") # "HHMMSS"

# Cursor/state lives inside the same RAW volume
STATE_PREFIX = f"{RAW_VOLUME}/_state"

# Output file sizing: coalesce reduces the # of small files (tune if needed)
WRITE_PARTS = int(os.getenv("WRITE_PARTS", "16"))

# First-ever run behavior: include rows missing change_ts columns (NULLs)
INCLUDE_NULLS_ON_FIRST_RUN = True

# Recommended Spark bits (safe to re-run)
spark = SparkSession.builder.appName("thelook-bq-to-raw-volume").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# ──────────────────────────────────────────────────────────────────────────────
# 1) SOURCE TABLE CONFIG
#    - change_cols: columns that tell us “a row changed” (created/updated/etc.)
#    - grace_minutes: re-read window to catch late arriving/updated records
#    - run_every_minutes: cadence gate (we'll skip a table if not yet due)
# ──────────────────────────────────────────────────────────────────────────────
TABLES: Dict[str, Dict] = {
  # Facts (two cadence buckets shown only as example)
  "orders":          {"change_cols": ["created_at","shipped_at","delivered_at","returned_at"],
                      "grace_minutes": 90, "run_every_minutes": 10, "is_dim": False},
  "order_items":     {"change_cols": ["created_at","shipped_at","delivered_at","returned_at"],
                      "grace_minutes": 90, "run_every_minutes": 10, "is_dim": False},

  "events":          {"change_cols": ["created_at"],
                      "grace_minutes": 60, "run_every_minutes": 20, "is_dim": False},
  "inventory_items": {"change_cols": ["created_at","sold_at"],
                      "grace_minutes": 60, "run_every_minutes": 20, "is_dim": False},
  "users":           {"change_cols": ["created_at"],
                      "grace_minutes": 60, "run_every_minutes": 20, "is_dim": False},

  # Dims without change columns → full snapshot when due (every 6 hours here)
  "products":              {"change_cols": [], "grace_minutes": 0, "run_every_minutes": 360, "is_dim": True},
  "distribution_centers":  {"change_cols": [], "grace_minutes": 0, "run_every_minutes": 360, "is_dim": True},
}

# ──────────────────────────────────────────────────────────────────────────────
# 2) SMALL HELPERS (time, paths, state)
# ──────────────────────────────────────────────────────────────────────────────

def _now_utc() -> datetime.datetime:
    """Timezone-aware 'now' in UTC (used for cadence & logging)."""
    return datetime.datetime.now(datetime.timezone.utc)

def _state_path(table: str) -> str:
    """Volume path for this table's state JSON (cursor, last run markers, etc.)."""
    return f"{STATE_PREFIX}/{table}.json"

def _bq_table_id(table: str) -> str:
    """BigQuery identifier that the Spark connector understands."""
    return f"bigquery-public-data.thelook_ecommerce.{table}"

# ── State I/O (cursor + cadence markers) ──────────────────────────────────────

def _load_state_blob(table: str) -> Optional[dict]:
    """Return the saved state dict for a table, or None if missing/invalid."""
    try:
        dbutils = DBUtils(spark)
        raw = dbutils.fs.head(_state_path(table), 4096)  # small file, read first 4KB
        return json.loads(raw)
    except Exception:
        return None

def load_cursor(table: str) -> Optional[str]:
    """Load previous high-watermark cursor ('YYYY-MM-DD HH:MM:SS+00'), or None if first run."""
    blob = _load_state_blob(table)
    return blob.get("cursor") if blob else None

def _get_last_run_utc(table: str) -> Optional[datetime.datetime]:
    """Return last successful run time (used for cadence gating)."""
    blob = _load_state_blob(table) or {}
    for k in ("last_run_utc", "saved_at_utc", "last_dim_snapshot_utc"):
        v = blob.get(k)
        if v:
            try:
                return datetime.datetime.fromisoformat(v.replace("Z", "+00"))
            except Exception:
                pass
    return None

def _is_due(table: str, cadence_minutes: int) -> bool:
    """
    Gate by cadence:
      • cadence_minutes <= 0  → always due
      • otherwise run only if that many minutes elapsed since last success
    """
    if cadence_minutes is None or cadence_minutes <= 0:
        return True
    last = _get_last_run_utc(table)
    if not last:
        return True
    return (_now_utc() - last).total_seconds() >= cadence_minutes * 60

def save_cursor(table: str, cursor_str: str, rows: int) -> None:
    """Persist the new cursor + lightweight metadata."""
    dbutils = DBUtils(spark)
    now_iso = _now_utc().isoformat(timespec="seconds")
    payload = {
        "table": table,
        "cursor": cursor_str,            # 'YYYY-MM-DD HH:MM:SS+00'
        "rows_last_batch": rows,
        "saved_at_utc": now_iso,
        "last_run_utc": now_iso,         # used by cadence gate
    }
    tmp = f"{_state_path(table)}.tmp.{uuid.uuid4().hex}"
    dbutils.fs.put(tmp, json.dumps(payload), overwrite=True)
    dbutils.fs.mv(tmp, _state_path(table), recurse=False)

def mark_dim_snapshot(table: str, rows: int) -> None:
    """For 'full-snapshot' dims, record last snapshot time (no cursor)."""
    dbutils = DBUtils(spark)
    now_iso = _now_utc().isoformat(timespec="seconds")
    payload = {"table": table, "rows_last_batch": rows, "last_run_utc": now_iso}
    tmp = f"{_state_path(table)}.tmp.{uuid.uuid4().hex}"
    dbutils.fs.put(tmp, json.dumps(payload), overwrite=True)
    dbutils.fs.mv(tmp, _state_path(table), recurse=False)

# ──────────────────────────────────────────────────────────────────────────────
# 3) INCREMENTAL WINDOW (lower bound + pushdown predicate)
# ──────────────────────────────────────────────────────────────────────────────

def _parse_cursor_to_dt_utc(cursor: str) -> datetime.datetime:
    """
    Parse stored cursor into timezone-aware UTC datetime.
    Accepts 'YYYY-MM-DD HH:MM:SS+00' or ISO-like 'YYYY-MM-DDTHH:MM:SSZ'.
    """
    s = cursor.replace("T", " ").replace("Z", "+00")
    if s.endswith("+00") and " " in s:
        dt = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S+00")
        return dt.replace(tzinfo=datetime.timezone.utc)
    return datetime.datetime.fromisoformat(s)

def _compute_lower_bound(cursor: Optional[str], grace_minutes: int = 0) -> datetime.datetime:
    """
    Compute the UTC lower bound (LB) for incremental filtering.

      First run  → LB = epoch
      Otherwise  → LB = max(epoch, min(cursor, now_utc) - grace_minutes)

    We cap the saved cursor at 'now' to avoid future-dated bounds (sources sometimes
    have timestamps in the future).
    """
    epoch   = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    now_utc = _now_utc()

    if not cursor:
        return epoch
    try:
        c = _parse_cursor_to_dt_utc(cursor)
    except Exception:
        return epoch

    effective_cursor = min(c, now_utc)
    lb = effective_cursor - datetime.timedelta(minutes=max(0, int(grace_minutes)))
    return max(lb, epoch)

def _build_pushdown_filter(change_cols: List[str],
                           lb_dt: datetime.datetime,
                           include_nulls: bool):
    """
    Build a pushdown-friendly Spark predicate (DataFrame API):
        OR_i (to_timestamp(col_i) >= LB) [OR col_i IS NULL on first run]
    """
    if not change_cols:
        return None  # dims without change columns → full table read

    lb_lit = F.lit(lb_dt.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")

    cond = None
    for c in change_cols:
        ts_col = F.to_timestamp(F.col(c))
        p = (ts_col >= lb_lit)
        if include_nulls:
            p = p | ts_col.isNull()
        cond = p if cond is None else (cond | p)
    return cond

# ──────────────────────────────────────────────────────────────────────────────
# 4) BIGQUERY READ (direct table read via Storage API; no staging)
# ──────────────────────────────────────────────────────────────────────────────

def bq_read_filtered(table: str,
                     change_cols: List[str],
                     grace_minutes: int,
                     cursor: Optional[str]) -> DataFrame:
    """
    Read directly from a BigQuery *table* using the Storage API,
    apply a pushdownable filter when applicable, and return a Spark DataFrame.
    """
    reader = (
        spark.read.format("bigquery")
        .option("parentProject", PROJECT_ID)
        .options(**BQ_AUTH)
        .option("table", _bq_table_id(table))   # table path, not SQL
    )

    lb_dt = _compute_lower_bound(cursor, grace_minutes)
    include_nulls = (cursor is None) and INCLUDE_NULLS_ON_FIRST_RUN
    cond = _build_pushdown_filter(change_cols, lb_dt, include_nulls)

    df = reader.load()
    return df if cond is None else df.filter(cond)

# ──────────────────────────────────────────────────────────────────────────────
# 5) RAW WRITE (Parquet files under the Volume path; no UC tables)
# ──────────────────────────────────────────────────────────────────────────────

def _normalize_system_cols(df: DataFrame) -> DataFrame:
    """
    Guardrails: keep our 4 system columns stable across runs to avoid drift.
      - ingest_date   : STRING   (partition)
      - run_ts        : STRING   (partition)
      - ingest_ts_utc : TIMESTAMP
      - source_table  : STRING
    """
    return (
        df.withColumn("ingest_date", F.col("ingest_date").cast("string"))
          .withColumn("run_ts", F.col("run_ts").cast("string"))
          .withColumn("ingest_ts_utc", F.to_timestamp(F.col("ingest_ts_utc")))
          .withColumn("source_table", F.col("source_table").cast("string"))
    )

def write_raw(df: DataFrame, table: str) -> str:
    """
    Append Parquet files into the RAW Volume, partitioned by (ingest_date, run_ts).

    Layout:
      {RAW_VOLUME}/{table}/
        └── ingest_date=YYYY-MM-DD/
            └── run_ts=HHMMSS/
                └── part-*.snappy.parquet
    """
    target_dir = f"{RAW_VOLUME}/{table}"

    (_normalize_system_cols(df)
        .coalesce(WRITE_PARTS)
        .write
        .format("parquet")
        .mode("append")
        .partitionBy("ingest_date", "run_ts")
        .save(target_dir))

    return f"files://{target_dir}"

# ──────────────────────────────────────────────────────────────────────────────
# 6) CURSOR ADVANCEMENT
# ──────────────────────────────────────────────────────────────────────────────

def compute_max_ts(df: DataFrame, change_cols: List[str], precomputed_count: Optional[int] = None) -> Optional[str]:
    """
    Compute the maximum observed change timestamp across change_cols.
    Returns 'YYYY-MM-DD HH:MM:SS+00' or None.
    """
    if not change_cols:
        return None
    if precomputed_count is not None and precomputed_count == 0:
        return None

    epoch_spark = F.to_timestamp(F.lit("1970-01-01 00:00:00"))
    cols = [F.coalesce(F.to_timestamp(F.col(c)), epoch_spark) for c in change_cols]
    chg_expr = cols[0].alias("chg") if len(cols) == 1 else F.greatest(*cols).alias("chg")

    row = df.select(chg_expr).agg(F.max("chg").alias("m")).first()
    max_ts = row["m"] if row else None
    if not max_ts:
        return None

    # Normalize to a BigQuery-friendly TIMESTAMP string
    if hasattr(max_ts, "strftime"):
        return max_ts.strftime("%Y-%m-%d %H:%M:%S+00")
    s = str(max_ts).replace("T", " ")
    if "Z" in s:
        s = s.replace("Z", "+00")
    if "+" not in s:
        s = s + "+00"
    return s

# ──────────────────────────────────────────────────────────────────────────────
# 7) MAIN LOOP
# ──────────────────────────────────────────────────────────────────────────────

def main():
    results = []

    for table, cfg in TABLES.items():
        change_cols      = cfg["change_cols"]
        grace_minutes    = int(cfg.get("grace_minutes", 0) or 0)
        is_dim           = cfg["is_dim"]
        cadence_minutes  = int(cfg.get("run_every_minutes", 0) or 0)

        print(f"\n=== Ingesting: {table} ===")

        # A) Per-table cadence gate
        if not _is_due(table, cadence_minutes):
            last = _get_last_run_utc(table)
            mins_since = int((_now_utc() - last).total_seconds() / 60) if last else 0
            print(f"{table}: skipped (cadence {cadence_minutes}m; last run {mins_since}m ago).")
            results.append({"table": table, "status": "skipped_cadence",
                            "cadence_min": cadence_minutes, "mins_since": mins_since})
            continue

        # B) Read from BigQuery (filtered when we have change columns)
        cursor = load_cursor(table)  # None on first run
        df = bq_read_filtered(table, change_cols, grace_minutes, cursor)

        # Add RAW lineage/partition columns expected in our file layout
        df = (df
              .withColumn("ingest_date", F.lit(TODAY))
              .withColumn("run_ts", F.lit(RUN_TS))
              .withColumn("ingest_ts_utc", F.current_timestamp())
              .withColumn("source_table", F.lit(table))
        )

        # C) Materialize and check emptiness (single action)
        n = df.count()
        if n == 0:
            print(f"{table}: no new rows; cursor unchanged.")
            # Still stamp last_run so cadence continues to work
            if is_dim:
                mark_dim_snapshot(table, 0)
            else:
                save_cursor(table, cursor or "1970-01-01 00:00:00+00", 0)
            results.append({"table": table, "status": "no_data"})
            continue

        # D) Write RAW files (append-only)
        dest = write_raw(df, table)
        print(f"{table}: wrote {n} rows → {dest}")

        # E) Advance cursor — cap at "now" so we never save a future time
        max_str = compute_max_ts(df, change_cols, precomputed_count=n)
        if max_str:
            now_utc = _now_utc()
            max_dt  = _parse_cursor_to_dt_utc(max_str)
            capped  = min(max_dt, now_utc)
            save_cursor(table, capped.strftime("%Y-%m-%d %H:%M:%S+00"), n)
            print(f"{table}: cursor advanced to {capped.strftime('%Y-%m-%d %H:%M:%S+00')}")
            results.append({"table": table, "status": "ok", "rows": n,
                            "cursor": capped.strftime("%Y-%m-%d %H:%M:%S+00")})
        else:
            # No max change → just stamp last_run (keeps cadence moving)
            save_cursor(table, cursor or "1970-01-01 00:00:00+00", n)
            results.append({"table": table, "status": "ok_no_cursor", "rows": n})

    # F) Optional quick visibility check for today's partition in one table
    try:
        raw_users_dir = f"{RAW_VOLUME}/users"
        cnt = (spark.read.option("mergeSchema", "true")
                      .parquet(raw_users_dir)
                      .where(F.col("ingest_date") == TODAY)
                      .count())
        print(f"Users rows ingested today: {cnt} (path: {raw_users_dir})")
    except Exception as e:
        print(f"Visibility check error (non-fatal): {e}")

    print("\nSummary:", results)

# Allow Databricks job “Python script” entrypoint
if __name__ == "__main__":
    main()
