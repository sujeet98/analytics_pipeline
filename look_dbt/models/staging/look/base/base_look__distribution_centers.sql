{# One row per distribution center id #}
with dedup as (
  {{ deduplicate_latest(
       relation=source('look','distribution_centers'),
       key_cols=['id'],
       order_cols=['ingest_ts_utc', 'to_timestamp(ingest_date)']
  ) }}
)
select * from dedup
