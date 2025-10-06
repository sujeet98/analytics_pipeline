{# One row per product id #}
with dedup as (
  {{ deduplicate_latest(
       relation=source('look','products'),
       key_cols=['id'],
       order_cols=['ingest_ts_utc', 'to_timestamp(ingest_date)']
  ) }}
)
select * from dedup
