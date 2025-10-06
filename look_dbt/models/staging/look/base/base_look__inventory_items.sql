{# One row per inventory_item id #}
with dedup as (
  {{ deduplicate_latest(
       relation=source('look','inventory_items'),
       key_cols=['id'],
       order_cols=['ingest_ts_utc', 'to_timestamp(ingest_date)']
  ) }}
)
select * from dedup
