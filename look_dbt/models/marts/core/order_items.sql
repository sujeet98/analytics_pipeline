{{ config(materialized='table') }}

with i as (
  select * from {{ ref('int_order_items_enriched') }}
)

select
  -- keys
  i.order_item_id,
  i.order_id,
  i.user_id,
  i.product_id,
  i.inventory_item_id,

  -- status & timing
  i.item_status     as status,
  i.item_created_at as created_at,

  -- pricing
  i.sale_price      as item_revenue,

  -- product attrs
  i.product_name,
  i.product_brand,
  i.product_sku,

  -- fulfillment attrs
  i.distribution_center_id
from i
