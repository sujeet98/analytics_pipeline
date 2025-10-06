

with src as (
  select * from `sujeet_data_analytics_workspace`.`bronze_dev`.`inventory_items`
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
  product_id,
  created_at,
  sold_at,
  cost,
  product_category,
  product_name,
  product_brand,
  product_retail_price,
  product_department,
  product_sku,
  product_distribution_center_id,
  ingest_ts_utc,
  source_table,
  ingest_date,
  run_ts,
  _rescued_data
from ranked
where rn = 1