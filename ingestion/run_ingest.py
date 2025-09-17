# ingestion/run_ingest.py
import os, sys
# Add the ingestion folder (parent of the package) to sys.path
THIS = os.path.dirname(os.path.abspath(__file__))       # .../ingestion
if THIS not in sys.path:
    sys.path.insert(0, THIS)

from thelook_ingest import ingest_bigquery_to_raw 

if __name__ == "__main__":
    ingest_bigquery_to_raw.main()
