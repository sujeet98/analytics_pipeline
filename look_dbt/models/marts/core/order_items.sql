{#
  Purpose
  -------
  Fact table at order-item grain for BI.
#}

{{ config(
    materialized='table'
) }}

select
  -- keys
  order_item_id,
  order_id,
  user_id,
  product_id,
  inventory_item_id,

  -- degenerate dims / attrs
  item_status,
  product_category,
  product_name,
  product_brand,
  product_department,
  product_sku,
  distribution_center_id,
  distribution_center_name,
  distribution_center_latitude,
  distribution_center_longitude,

  -- timestamps
  created_at       as item_created_at,
  shipped_at       as item_shipped_at,
  delivered_at     as item_delivered_at,
  returned_at      as item_returned_at,

  -- measures
  item_revenue,
  days_to_ship,
  days_in_transit,
  days_to_return

from {{ ref('int_order_items_enriched') }}
;
