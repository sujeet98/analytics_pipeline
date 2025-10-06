{# One row per order_id #}
with dedup as (
  {{ deduplicate_latest(
       relation=source('look','orders'),
       key_cols=['order_id'],
       order_cols=['ingest_ts_utc', 'to_timestamp(ingest_date)']
  ) }}
)
select * from dedup
