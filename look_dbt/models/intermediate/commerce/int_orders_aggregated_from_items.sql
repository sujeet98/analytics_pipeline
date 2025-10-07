{{ config(materialized='view') }}

with items as (
  select * from {{ ref('int_order_items_enriched') }}
),
orders as (
  -- canonical one row per order with the correct user_id
  select order_id, user_id, created_at
  from {{ ref('stg_look__orders') }}
),
agg as (
  select
      i.order_id,
      -- choose canonical user_id from orders to ensure uniqueness
      o.user_id as user_id,
      min(i.item_created_at)           as order_first_item_at,
      count(*)                         as item_count,
      sum(coalesce(i.sale_price, 0.0)) as items_gross_revenue
  from items i
  left join orders o
    on i.order_id = o.order_id
  group by i.order_id, o.user_id
)

select * from agg
