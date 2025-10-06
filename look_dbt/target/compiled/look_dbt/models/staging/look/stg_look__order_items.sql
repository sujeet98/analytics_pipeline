

with b as (select * from sujeet_data_analytics_workspace.silver_dev.base_look__order_items)
select
  id        as order_item_id,
  order_id,
  user_id,
  product_id,
  inventory_item_id,
  case lower(status)
  when 'Complete'     then 'Complete'
  when 'Shipped'      then 'Shipped'
  when 'Returned'     then 'Returned'
  when 'Cancelled'    then 'Cancelled'
  when 'Processing'   then 'Processing'
  else 'Unknown'
end as status,
  cast(created_at as timestamp)   as created_at,
  cast(shipped_at as timestamp)   as shipped_at,
  cast(delivered_at as timestamp) as delivered_at,
  cast(returned_at as timestamp)  as returned_at,
  cast(sale_price as decimal(18, 2))    as sale_price,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b