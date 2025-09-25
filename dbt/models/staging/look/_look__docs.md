# Look Staging Docs
Each `stg_look__*` model is a 1:1 cleaned view over its raw table in `bronze_dev`.
Transformations include renaming, typing, normalization, and deduplication by latest `ingest_ts_utc`.
No joins or aggregations are performed here.
