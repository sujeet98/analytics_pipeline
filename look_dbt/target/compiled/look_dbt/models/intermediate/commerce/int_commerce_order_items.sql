



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),
unioned as (
  select * from sujeet_data_analytics_workspace.silver_dev.int_commerce_order_items__look
  where canonical_updated_at >= dateadd(day, -2, (select max_ts from tgt_max))
),
ranked as (
  select u.*,
         row_number() over (partition by order_item_id
                            order by canonical_updated_at desc,
                                     case source_system when 'look' then 1 else 99 end) as rn
  from unioned u
)
select
  order_item_id, order_id, user_id, product_id, inventory_item_id,
  item_status,
  created_at, shipped_at, delivered_at, returned_at,
  item_date, sale_price,
  source_system,
  canonical_updated_at,
  canonical_updated_at as ingest_ts_utc
from ranked
where rn = 1;