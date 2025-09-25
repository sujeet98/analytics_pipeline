{{ config(materialized='ephemeral') }}

-- Purpose: Aggregate enriched items to order grain and join light order attrs.
-- Grain: 1 row per order_id

with o as (
  select * from {{ ref('stg_look__orders') }}
), items as (
  select * from {{ ref('int_order_items_enriched') }}
), agg as (
  select
    order_id,
    sum(item_revenue)      as order_gross_revenue,
    sum(item_cost)         as order_gross_cost,
    sum(item_gross_margin) as order_gross_margin,
    count(*)               as order_item_count
  from items
  group by 1
)
select
  o.order_id,
  o.user_id,
  o.status,
  o.gender,
  o.created_at,
  o.shipped_at,
  o.delivered_at,
  o.returned_at,
  o.num_of_item,
  a.order_item_count,
  {{ as_money_2('a.order_gross_revenue') }} as order_gross_revenue,
  {{ as_money_2('a.order_gross_cost') }}    as order_gross_cost,
  {{ as_money_2('a.order_gross_margin') }}  as order_gross_margin,
  {{ hours_between('o.created_at','o.delivered_at') }} as hours_to_deliver,
  o.created_date
from o
left join agg a using (order_id)
