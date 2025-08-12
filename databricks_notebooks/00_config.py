# COMMAND ----------
# 00_config — Environment & GCP credential setup (dev-friendly)
# You said your .env stores RAW JSON in GCP_CREDS_JSON (or gcp_creds_json).
# We will base64-encode it for the BigQuery connector.

import os, base64, json

# ---- EDIT these non-GCP values as needed ----
os.environ.setdefault("AWS_S3_BUCKET", "analyticsbucketdev-sk")
os.environ.setdefault("RAW_PREFIX", "raw/thelook")
os.environ.setdefault("BRONZE_PREFIX", "bronze/thelook/delta")

# ---- Read raw JSON creds from env (support both casings) ----
raw_json = os.getenv("GCP_CREDS_JSON")
if not raw_json:
    raise RuntimeError(
        "Missing GCP_CREDS_JSON (raw JSON). Put your full service-account JSON into this env var."
    )

# Validate it's JSON (will throw if malformed)
creds_dict = json.loads(raw_json)

# Project ID: prefer explicit env override, else pick from JSON
project_id = os.getenv("GCP_PROJECT_ID")
if not project_id:
    raise RuntimeError("No GCP_PROJECT_ID found.")

os.environ["GCP_PROJECT_ID"] = project_id

# Base64-encode the raw JSON for the BigQuery connector's 'credentials' option
os.environ["GOOGLE_APPLICATION_CREDENTIALS_B64"] = base64.b64encode(
    raw_json.encode("utf-8")
).decode("ascii")

print("✅ GCP creds loaded from env (raw JSON) and prepared as base64.")
print("   Project ID:", os.environ["GCP_PROJECT_ID"])
print("   Using S3 bucket:", os.environ.get("AWS_S3_BUCKET"))

# Helper accessors used by other notebooks
def get_bq_auth_options():
    # We ALWAYS use inline base64 now (no files)
    return {"credentials": os.environ["GOOGLE_APPLICATION_CREDENTIALS_B64"]}

def get_bucket():        return os.environ["AWS_S3_BUCKET"]
def get_raw_prefix():    return os.environ.get("RAW_PREFIX", "raw/thelook")
def get_bronze_prefix(): return os.environ.get("BRONZE_PREFIX", "bronze/thelook/delta")
def get_project():       return os.environ["GCP_PROJECT_ID"]

print("✅ get_bq_auth_options() ready (inline base64).")
