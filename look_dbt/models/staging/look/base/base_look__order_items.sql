{# One row per order item id #}
with dedup as (
  {{ deduplicate_latest(
       relation=source('look','order_items'),
       key_cols=['id'],
       order_cols=['ingest_ts_utc', 'to_timestamp(ingest_date)']
  ) }}
)
select * from dedup
