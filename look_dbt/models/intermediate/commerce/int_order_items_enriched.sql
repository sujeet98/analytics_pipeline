{{ config(materialized='ephemeral') }}

-- Purpose: Enrich items with product attributes and cost, keeping item grain.
-- Inputs: stg_look__order_items, stg_look__products, stg_look__inventory_items
-- Output grain: 1 row per order_item_id

with oi as (
  select * from {{ ref('stg_look__order_items') }}
), p as (
  select * from {{ ref('stg_look__products') }}
), ii as (
  select * from {{ ref('stg_look__inventory_items') }}
)
select
  oi.order_item_id,
  oi.order_id,
  oi.user_id,
  oi.product_id,
  oi.inventory_item_id,
  oi.status,
  oi.created_at,
  oi.shipped_at,
  oi.delivered_at,
  oi.returned_at,
  -- Monetary fields
  oi.item_revenue,
  coalesce(ii.unit_cost, p.unit_cost) as item_cost,
  {{ as_money_2('oi.item_revenue - coalesce(ii.unit_cost, p.unit_cost)') }} as item_gross_margin,
  -- Product attrs / DC
  p.brand,
  p.category,
  p.department,
  p.sku,
  coalesce(ii.distribution_center_id, p.distribution_center_id) as distribution_center_id,
  -- Dates
  oi.created_date,
  oi.delivered_date,
  oi.returned_date
from oi
left join p using (product_id)
left join ii using (inventory_item_id)
