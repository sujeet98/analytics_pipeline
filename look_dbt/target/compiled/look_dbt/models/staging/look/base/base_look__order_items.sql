

with src as (
  select * from `sujeet_data_analytics_workspace`.`bronze_dev`.`order_items`
),
ranked as (
  select
    *,
    row_number() over (
      partition by id
      order by coalesce(ingest_ts_utc, to_timestamp(run_ts)) desc
    ) as rn
  from src
)
select
  id,
  order_id,
  user_id,
  product_id,
  inventory_item_id,
  status,
  created_at,
  shipped_at,
  delivered_at,
  returned_at,
  sale_price,
  ingest_ts_utc,
  source_table,
  ingest_date,
  run_ts,
  _rescued_data
from ranked
where rn = 1