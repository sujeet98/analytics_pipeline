{# One row per user id #}
with dedup as (
  {{ deduplicate_latest(
       relation=source('look','users'),
       key_cols=['id'],
       order_cols=['ingest_ts_utc', 'to_timestamp(ingest_date)']
  ) }}
)
select * from dedup
