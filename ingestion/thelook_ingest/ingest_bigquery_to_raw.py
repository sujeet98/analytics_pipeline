# 01_ingest_bigquery_raw_incremental_v10_uc.py
#
# ------------------------------------------------------------------------------
# OVERVIEW
# ------------------------------------------------------------------------------
# PURPOSE
#   Incrementally ingest BigQuery public dataset
#   `bigquery-public-data.thelook_ecommerce` into an immutable RAW layer on S3,
#   using Databricks (AWS) with Unity Catalog (UC).
#
# WRITING MODE (Option B - UC external tables)
#   • Instead of writing directly to s3a:// paths, we append into UC external
#     Parquet tables (one table per source table) that point at:
#       s3://{BUCKET}/{RAW_PREFIX}/{table}
#   • On the first write we auto-create the UC table (external, partitioned by
#     (ingest_date, run_ts)); on subsequent runs we `insertInto` the table.
#   • This routes S3 access through your **UC storage credential** (no instance
#     profile on the cluster required) while preserving immutable partitions.
#
# INCREMENTAL LOGIC (unchanged)
#   • Per-table cadence gate: only run when the table’s `run_every_minutes` has
#     elapsed since last success.
#   • Lower bound (LB) = max(epoch, min(cursor, now) - grace_minutes).
#   • Facts/dims WITH change columns filter rows using LB; dims WITHOUT change
#     columns are full-snapshotted when due.
#   • Cursor is advanced to the max observed change timestamp (capped at now).
#
# KNOBS
#   - TABLES[*].run_every_minutes  → when each table should run.
#   - TABLES[*].grace_minutes      → how far to re-read for late/backdated rows.
#   - WRITE_PARTS                  → coalesce target (# output files).
#   - INCLUDE_NULLS_ON_FIRST_RUN   → include NULL change timestamps on first load.
#
# OPERATION
#   • Schedule this job every 10 minutes (max concurrent runs = 1).
#   • UC permissions required:
#       - USE CATALOG on your UC catalog
#       - USE/CREATE TABLE on the target schema
#       - CREATE EXTERNAL TABLE on your External Location (e.g., raw_thelook)
# ------------------------------------------------------------------------------

import datetime, json, uuid
from typing import Optional, List, Dict
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from config import (
    get_project, get_bq_auth_options, get_bucket, get_raw_prefix, get_uc_catalog
)

# ------------------------------------------------------------------------------
# ENVIRONMENT
# ------------------------------------------------------------------------------
PROJECT_ID = get_project()                 # GCP project billed for BigQuery reads
BQ_AUTH    = get_bq_auth_options()         # Spark–BigQuery connector auth dict
BUCKET     = get_bucket()                  # Destination S3 bucket (UC external location covers this)
RAW_PREFIX = get_raw_prefix()              # e.g. "raw/thelook"
UC_CATALOG = get_uc_catalog()              # e.g. "sujeet_data_analytics_workspace"

# Global run metadata (UTC for reproducibility)
TODAY  = datetime.date.today().isoformat()                               # "YYYY-MM-DD"
RUN_TS = datetime.datetime.now(datetime.timezone.utc).strftime("%H%M%S") # "HHMMSS" (UTC)

# State files remain in S3 (simple JSON per table)
STATE_PREFIX = f"s3a://{BUCKET}/{RAW_PREFIX}/_state"

# Output file sizing (coalesce reduces partitions; never increases)
WRITE_PARTS = 32

# Include NULL change timestamps on the very first run?
INCLUDE_NULLS_ON_FIRST_RUN = True

# Recommended Spark settings (idempotent; set here for clarity)
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# ------------------------------------------------------------------------------
# TABLE CONFIGURATION
# ------------------------------------------------------------------------------
TABLES: Dict[str, Dict] = {
  # Facts (two cadence buckets for illustration)
  "orders":        {"change_cols": ["created_at","shipped_at","delivered_at","returned_at"],
                    "grace_minutes": 90, "run_every_minutes": 10, "is_dim": False},
  "order_items":   {"change_cols": ["created_at","shipped_at","delivered_at","returned_at"],
                    "grace_minutes": 90, "run_every_minutes": 10, "is_dim": False},

  "events":        {"change_cols": ["created_at"],
                    "grace_minutes": 60, "run_every_minutes": 20, "is_dim": False},
  "inventory_items":{"change_cols": ["created_at","sold_at"],
                    "grace_minutes": 60, "run_every_minutes": 20, "is_dim": False},
  "users":         {"change_cols": ["created_at"],
                    "grace_minutes": 60, "run_every_minutes": 20, "is_dim": False},

  # Dims: no change columns → full snapshot every 6 hours (360 min)
  "products":              {"change_cols": [], "grace_minutes": 0, "run_every_minutes": 360, "is_dim": True},
  "distribution_centers":  {"change_cols": [], "grace_minutes": 0, "run_every_minutes": 360, "is_dim": True},
}

# ------------------------------------------------------------------------------
# HELPERS (time, paths, state, ids)
# ------------------------------------------------------------------------------
def _now_utc() -> datetime.datetime:
    """Timezone-aware 'now' in UTC."""
    return datetime.datetime.now(datetime.timezone.utc)

def _state_path(table: str) -> str:
    """S3 path for this table's state JSON (cursor, last run markers, etc.)."""
    return f"{STATE_PREFIX}/{table}.json"

def _bq_table_id(table: str) -> str:
    """BigQuery table identifier (no SQL)."""
    return f"bigquery-public-data.thelook_ecommerce.{table}"

def _uc_full_table(table: str) -> str:
    """Fully qualified UC table name we append to (external Parquet table)."""
    # We keep one schema named 'raw' inside the chosen UC catalog
    return f"{UC_CATALOG}.raw.{table}"

# ---- State I/O (cursor + cadence markers) ------------------------------------
def _load_state_blob(table: str) -> Optional[dict]:
    """Return the raw JSON state dict for a table, or None if missing."""
    try:
        raw = dbutils.fs.head(_state_path(table), 4096)
        return json.loads(raw)
    except Exception:
        return None

def load_cursor(table: str) -> Optional[str]:
    """Load previous high-watermark cursor ('YYYY-MM-DD HH:MM:SS+00'), or None if first run."""
    blob = _load_state_blob(table)
    return blob.get("cursor") if blob else None

def _get_last_run_utc(table: str) -> Optional[datetime.datetime]:
    """Return the last time this table successfully ran (for cadence gating)."""
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
    Per-table cadence gate:
      • If cadence_minutes <= 0 → always due (no gating).
      • Else → run only if 'cadence_minutes' elapsed since last successful run.
    """
    if cadence_minutes is None or cadence_minutes <= 0:
        return True
    last = _get_last_run_utc(table)
    if not last:
        return True  # never ran
    return (_now_utc() - last).total_seconds() >= cadence_minutes * 60

def save_cursor(table: str, cursor_str: str, rows: int) -> None:
    """Persist the new cursor + minimal metadata (atomic write via tmp + mv)."""
    now_iso = _now_utc().isoformat(timespec="seconds")
    payload = {
        "table": table,
        "cursor": cursor_str,            # 'YYYY-MM-DD HH:MM:SS+00'
        "rows_last_batch": rows,
        "saved_at_utc": now_iso,
        "last_run_utc": now_iso,         # cadence gating uses this
    }
    tmp = f"{_state_path(table)}.tmp.{uuid.uuid4().hex}"
    dbutils.fs.put(tmp, json.dumps(payload), overwrite=True)
    dbutils.fs.mv(tmp, _state_path(table), recurse=False)

def mark_dim_snapshot(table: str, rows: int) -> None:
    """
    For dimensions without change columns, simply record the last snapshot time.
    (We don’t store a cursor for full-snapshot dims.)
    """
    now_iso = _now_utc().isoformat(timespec="seconds")
    payload = {"table": table, "rows_last_batch": rows, "last_run_utc": now_iso}
    tmp = f"{_state_path(table)}.tmp.{uuid.uuid4().hex}"
    dbutils.fs.put(tmp, json.dumps(payload), overwrite=True)
    dbutils.fs.mv(tmp, _state_path(table), recurse=False)

# ------------------------------------------------------------------------------
# INCREMENTAL FILTERING (lower bound + pushdown predicate)
# ------------------------------------------------------------------------------
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

    Why:
      • Minute-level windows fit 5–20 min micro-batches.
      • Cursor is capped at 'now' to avoid future LBs (source may have future-dated rows).
    """
    epoch   = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    now_utc = _now_utc()

    if not cursor:
        return epoch
    try:
        c = _parse_cursor_to_dt_utc(cursor)
    except Exception:
        return epoch

    effective_cursor = min(c, now_utc)  # guard future-dated cursors
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
        return None  # dims (without change cols) → full table read

    lb_lit = F.lit(lb_dt.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")

    cond = None
    for c in change_cols:
        ts_col = F.to_timestamp(F.col(c))
        p = (ts_col >= lb_lit)
        if include_nulls:  # emulate COALESCE-to-epoch on first run
            p = p | ts_col.isNull()
        cond = p if cond is None else (cond | p)
    return cond

# ------------------------------------------------------------------------------
# BIGQUERY READ (TABLE + PUSHDOWN; NO MATERIALIZATION)
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# RAW WRITE via UC tables (external Parquet; no direct s3a:// writes)
# ------------------------------------------------------------------------------
def write_raw(df: DataFrame, table: str) -> str:
    """
    Append into a UC external Parquet table partitioned by (ingest_date, run_ts).

    • First run:
        - CREATE SCHEMA IF NOT EXISTS `{UC_CATALOG}`.raw
        - WRITE DataFrame with `.saveAsTable(full_table)` and
          `.option("path", "s3://{bucket}/{raw_prefix}/{table}")`
          so UC registers an external table at that path.
    • Later runs:
        - `.insertInto(full_table)` to append.

    This keeps immutable, discoverable partitions while routing I/O
    through the UC storage credential (no instance profile needed).
    """
    full_table = _uc_full_table(table)
    table_path = f"s3://{BUCKET}/{RAW_PREFIX}/{table}"   # UC expects s3://, not s3a://

    # Ensure the target schema exists (idempotent; runs fast if already present)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{UC_CATALOG}`.raw")

    if not spark.catalog.tableExists(full_table):
        # First write: create the external table with DF schema + partitions.
        (df.coalesce(WRITE_PARTS)
           .write
           .format("parquet")
           .mode("append")
           .option("path", table_path)
           .partitionBy("ingest_date", "run_ts")
           .saveAsTable(full_table))
    else:
        # Later writes: append using table metadata/location.
        (df.coalesce(WRITE_PARTS)
           .write
           .format("parquet")
           .mode("append")
           .insertInto(full_table))

    return f"table://{full_table}"

# ------------------------------------------------------------------------------
# CURSOR ADVANCEMENT (DataFrame-only ops)
# ------------------------------------------------------------------------------
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

    # Normalize to BigQuery-friendly TIMESTAMP string
    if hasattr(max_ts, "strftime"):
        return max_ts.strftime("%Y-%m-%d %H:%M:%S+00")
    s = str(max_ts).replace("T", " ")
    if "Z" in s:
        s = s.replace("Z", "+00")
    if "+" not in s:
        s = s + "+00"
    return s

# ------------------------------------------------------------------------------
# MAIN INGEST LOOP
# ------------------------------------------------------------------------------
def main():
    results = []

    for table, cfg in TABLES.items():
        change_cols      = cfg["change_cols"]
        grace_minutes    = int(cfg.get("grace_minutes", 0) or 0)
        is_dim           = cfg["is_dim"]
        cadence_minutes  = int(cfg.get("run_every_minutes", 0) or 0)

        print(f"\n=== Ingesting: {table} ===")

        # 0) Per-table cadence gate: skip if not yet due
        if not _is_due(table, cadence_minutes):
            last = _get_last_run_utc(table)
            mins_since = int((_now_utc() - last).total_seconds() / 60) if last else 0
            print(f"{table}: skipped (cadence {cadence_minutes}m; last run {mins_since}m ago).")
            results.append({"table": table, "status": "skipped_cadence",
                            "cadence_min": cadence_minutes, "mins_since": mins_since})
            continue

        # A) Dims WITHOUT change columns → FULL SNAPSHOT when due
        if is_dim and len(change_cols) == 0:
            df = (
                spark.read.format("bigquery")
                .option("parentProject", PROJECT_ID)
                .options(**BQ_AUTH)
                .option("table", _bq_table_id(table))
                .load()
                # Add lineage & partition columns expected by UC table
                .withColumn("ingest_date", F.lit(TODAY))
                .withColumn("run_ts", F.lit(RUN_TS))
                .withColumn("ingest_ts_utc", F.current_timestamp())
                .withColumn("source_table", F.lit(table))
            )

            n = df.count()
            if n == 0:
                print(f"{table}: no rows (full snapshot).")
                results.append({"table": table, "status": "no_data"})
                mark_dim_snapshot(table, 0)  # still stamp last_run so cadence gating works
                continue

            dest = write_raw(df, table)
            print(f"{table}: full snapshot wrote {n} rows → {dest}")
            mark_dim_snapshot(table, n)
            results.append({"table": table, "status": "ok_fullpull", "rows": n})
            continue

        # B) Facts or Dims WITH change columns → incremental micro-batch
        cursor = load_cursor(table)  # (None if first run)

        df = (
            bq_read_filtered(
                table=table,
                change_cols=change_cols,
                grace_minutes=grace_minutes,
                cursor=cursor
            )
            # Add lineage & partition columns expected by UC table
            .withColumn("ingest_date", F.lit(TODAY))
            .withColumn("run_ts", F.lit(RUN_TS))
            .withColumn("ingest_ts_utc", F.current_timestamp())
            .withColumn("source_table", F.lit(table))
        )

        # Materialize & check emptiness (single action)
        n = df.count()
        if n == 0:
            print(f"{table}: no new rows; cursor unchanged.")
            # still stamp last_run to honor cadence even when empty runs occur
            mark_dim_snapshot(table, 0) if is_dim else save_cursor(table, cursor or "1970-01-01 00:00:00+00", 0)
            results.append({"table": table, "status": "no_data"})
            continue

        # Write RAW via UC external table
        dest = write_raw(df, table)
        print(f"{table}: wrote {n} rows → {dest}")

        # Advance cursor — cap at "now" so saved cursor is never future-dated
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
            # No max change → still stamp last_run for cadence, without cursor move
            save_cursor(table, cursor or "1970-01-01 00:00:00+00", n)
            results.append({"table": table, "status": "ok_no_cursor", "rows": n})

    # ------------------------------------------------------------------------------
    # OPTIONAL: quick visibility check for today's partition in UC table
    # ------------------------------------------------------------------------------
    try:
        users_tbl = _uc_full_table("users")
        cnt = spark.table(users_tbl).where(F.col("ingest_date") == TODAY).count()
        print(f"Users rows ingested today: {cnt} (table: {users_tbl})")
    except Exception as e:
        print(f"Visibility check error (non-fatal): {e}")

    print("\nSummary:", results)
