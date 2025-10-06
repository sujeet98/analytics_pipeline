

with b as (select * from sujeet_data_analytics_workspace.silver_dev.base_look__orders)
select
  order_id,
  user_id,
  case lower(status)
  when 'Complete'     then 'Complete'
  when 'Shipped'      then 'Shipped'
  when 'Returned'     then 'Returned'
  when 'Cancelled'    then 'Cancelled'
  when 'Processing'   then 'Processing'
  else 'Unknown'
end as status,
  nullif(trim(gender), '') as gender,
  cast(created_at as timestamp)   as created_at,
  cast(returned_at as timestamp)  as returned_at,
  cast(shipped_at as timestamp)   as shipped_at,
  cast(delivered_at as timestamp) as delivered_at,
  num_of_item,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b