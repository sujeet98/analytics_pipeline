



with base as (
  select
    source_system,
    inventory_item_id,
    product_id,
    distribution_center_id,      -- from core; may mirror product’s dc
    created_at,
    created_date,
    sold_at,
    unit_cost,
    retail_price
  from sujeet_data_analytics_workspace.silver_dev.core_commerce_inventory_items
  
),

prod_first as (
  select global_product_id, min(valid_from) as first_valid_from
  from sujeet_data_analytics_workspace.gold_dev.dim_product group by 1
),
prod_earliest as (
  select d.*
  from sujeet_data_analytics_workspace.gold_dev.dim_product d
  join prod_first f
    on d.global_product_id = f.global_product_id
   and d.valid_from = f.first_valid_from
),

dc_first as (
  select global_dc_id, min(valid_from) as first_valid_from
  from sujeet_data_analytics_workspace.gold_dev.dim_distribution_center group by 1
),
dc_earliest as (
  select d.*
  from sujeet_data_analytics_workspace.gold_dev.dim_distribution_center d
  join dc_first f
    on d.global_dc_id = f.global_dc_id
   and d.valid_from = f.first_valid_from
),

prod as (
  select
    b.*,
    coalesce(dp.product_sk, dpe.product_sk) as product_sk,
    -- prefer product's dc from the dimension
    coalesce(dp.distribution_center_id, dpe.distribution_center_id, b.distribution_center_id) as product_dc_id
  from base b
  left join sujeet_data_analytics_workspace.gold_dev.dim_product dp
    on dp.global_product_id = concat(b.source_system, ':', cast(b.product_id as string))
   and b.created_at >= dp.valid_from
   and b.created_at <  dp.valid_to
  left join prod_earliest dpe
    on dpe.global_product_id = concat(b.source_system, ':', cast(b.product_id as string))
   and dp.product_sk is null
),

dcj as (
  select
    p.*,
    case
      when p.product_dc_id is null then null
      else coalesce(dd.dc_sk, dde.dc_sk)
    end as dc_sk
  from prod p
  left join sujeet_data_analytics_workspace.gold_dev.dim_distribution_center dd
    on dd.global_dc_id = concat(p.source_system, ':', cast(p.product_dc_id as string))
   and p.created_at >= dd.valid_from
   and p.created_at <  dd.valid_to
  left join dc_earliest dde
    on dde.global_dc_id = concat(p.source_system, ':', cast(p.product_dc_id as string))
   and dd.dc_sk is null
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