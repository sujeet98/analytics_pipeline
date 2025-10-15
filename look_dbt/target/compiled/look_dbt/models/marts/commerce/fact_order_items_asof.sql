



with base as (
  select
    source_system,
    order_item_id,
    order_id,
    user_id,
    product_id,
    created_at,
    created_date,
    item_date,
    shipped_at, delivered_at, returned_at,
    sale_price
  from sujeet_data_analytics_workspace.silver_dev.core_commerce_order_items
  
),

-- Prepare dimension “earliest version” rows for fallback (facts before first valid_from)
cust_first as (
  select global_customer_id, min(valid_from) as first_valid_from
  from sujeet_data_analytics_workspace.gold_dev.dim_customer group by 1
),
cust_earliest as (
  select d.*
  from sujeet_data_analytics_workspace.gold_dev.dim_customer d
  join cust_first f
    on d.global_customer_id = f.global_customer_id
   and d.valid_from = f.first_valid_from
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

-- Resolve CUSTOMER as-of created_at, with fallback to earliest
cust_join as (
  select
    b.*,
    coalesce(dc.customer_sk, dce.customer_sk) as customer_sk
  from base b
  left join sujeet_data_analytics_workspace.gold_dev.dim_customer dc
    on dc.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string))
   and b.created_at >= dc.valid_from
   and b.created_at <  dc.valid_to
  left join cust_earliest dce
    on dce.global_customer_id = concat(b.source_system, ':', cast(b.user_id as string))
   and dc.customer_sk is null
),

-- Resolve PRODUCT as-of created_at, with fallback to earliest
prod_join as (
  select
    c.*,
    coalesce(dp.product_sk, dpe.product_sk) as product_sk,
    coalesce(dp.distribution_center_id, dpe.distribution_center_id) as product_dc_id
  from cust_join c
  left join sujeet_data_analytics_workspace.gold_dev.dim_product dp
    on dp.global_product_id = concat(c.source_system, ':', cast(c.product_id as string))
   and c.created_at >= dp.valid_from
   and c.created_at <  dp.valid_to
  left join prod_earliest dpe
    on dpe.global_product_id = concat(c.source_system, ':', cast(c.product_id as string))
   and dp.product_sk is null
),

-- Resolve DC SK via product’s dc_id, as-of created_at (DCs are SCD2 too)
dc_join as (
  select
    p.*,
    -- Some products can have null DC; keep nullable
    case
      when p.product_dc_id is null then null
      else coalesce(dd.dc_sk, dde.dc_sk)
    end as dc_sk
  from prod_join p
  left join sujeet_data_analytics_workspace.gold_dev.dim_distribution_center dd
    on dd.global_dc_id = concat(p.source_system, ':', cast(p.product_dc_id as string))
   and p.created_at >= dd.valid_from
   and p.created_at <  dd.valid_to
  left join dc_earliest dde
    on dde.global_dc_id = concat(p.source_system, ':', cast(p.product_dc_id as string))
   and dd.dc_sk is null
)

select
  order_item_id,
  order_id,
  customer_sk,
  product_sk,
  dc_sk,
  created_at,
  created_date,
  item_date,
  shipped_at, delivered_at, returned_at,
  sale_price
from dc_join;