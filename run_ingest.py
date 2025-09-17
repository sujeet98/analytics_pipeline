# run_ingest.py  (repo root)
from ingestion.thelook_ingest import ingest_bigquery_to_raw as mod

if __name__ == "__main__":
    mod.main()
