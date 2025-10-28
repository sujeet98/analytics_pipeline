"""
Centralized configuration & helpers for the ingestion job.

Priority:
1) Environment variables (recommended for Databricks Jobs)
2) Databricks Secrets (optional fallback)
3) Sensible defaults for local development

Important env vars you can set on the Job:
  - GCP_PROJECT_ID
  - RAW_BUCKET
  - RAW_PREFIX                  (e.g., "raw/thelook")
  - BRONZE_PREFIX               (optional; for dbt later)
  - GOOGLE_APPLICATION_CREDENTIALS_B64  (base64 of SA JSON; optional if using OIDC)
  - DATABRICKS_SECRETS_SCOPE    (e.g., "analyticsProject") – used only if the env var above is not set
  - UC_CATALOG                  (e.g., "sujeet_data_analytics_workspace")
"""

import os
from typing import Dict, Optional

# ---- internal: optional access to dbutils.secrets (Databricks only) ----------------
def _get_dbutils():
    try:
        return dbutils  # type: ignore
    except NameError:
        return None

def _get_secret(scope: str, key: str) -> Optional[str]:
    dbu = _get_dbutils()
    if not dbu:
        return None
    try:
        return dbu.secrets.get(scope, key)  # type: ignore[attr-defined]
    except Exception:
        return None

# ---- public getters used by the ingestion script -----------------------------------
def get_project() -> str:
    return os.environ.get("GCP_PROJECT_ID", "analyticsproject-468700")

def get_bucket() -> str:
    return os.environ.get("RAW_BUCKET", "analyticsbucketdev-sk")

def get_raw_prefix() -> str:
    # Your portfolio uses a nested RAW path ("raw/thelook")
    return os.environ.get("RAW_PREFIX", "raw/thelook")

def get_bronze_prefix() -> str:
    # Not used by the ingestion job, but handy for dbt later
    return os.environ.get("BRONZE_PREFIX", "bronze/thelook/delta")

def get_bq_auth_options() -> Dict[str, str]:
    """
    BigQuery connector auth dict.
    - If GOOGLE_APPLICATION_CREDENTIALS_B64 is set, pass it via "credentials".
    - Otherwise return {} so the connector uses cluster auth (OIDC/ADC) if configured.
    """
    b64 = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_B64")
    if not b64:
        scope = os.environ.get("DATABRICKS_SECRETS_SCOPE", "analyticsProject")
        b64 = _get_secret(scope, "GOOGLE_APPLICATION_CREDENTIALS_B64")
    return {"credentials": b64} if b64 else {}

def get_uc_catalog() -> str:
    """
    Unity Catalog to write RAW external tables into.
    Default to your current workspace catalog; override per env via UC_CATALOG.
    """
    return os.environ.get("UC_CATALOG", "sujeet_data_analytics_workspace")