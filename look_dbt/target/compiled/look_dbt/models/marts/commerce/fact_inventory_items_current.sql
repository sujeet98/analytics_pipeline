



with base as (
  select
    source_system,
    inventory_item_id,
    product_id,
    distribution_center_id,
    created_at,
    created_date,
    sold_at,
    unit_cost,
    retail_price
  from sujeet_data_analytics_workspace.silver_dev.core_commerce_inventory_items
  
),

prod as (
  select
    b.*,
    dp.product_sk,
    coalesce(dp.distribution_center_id, b.distribution_center_id) as product_dc_id
  from base b
  left join sujeet_data_analytics_workspace.gold_dev.dim_product_current dp
    on dp.global_product_id = concat(b.source_system, ':', cast(b.product_id as string))
),

dcj as (
  select
    p.*,
    ddc.dc_sk
  from prod p
  left join sujeet_data_analytics_workspace.gold_dev.dim_distribution_center_current ddc
    on ddc.global_dc_id = concat(p.source_system, ':', cast(p.product_dc_id as string))
)

select
  inventory_item_id,
  product_sk,
  dc_sk,
  created_at,
  created_date,
  sold_at,
  unit_cost,
  retail_price
from dcj;