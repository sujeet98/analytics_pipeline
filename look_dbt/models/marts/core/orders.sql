{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}

with
orders as (
  select * from {{ ref('stg_look__orders') }}
),
items_agg as (
  -- order-level rollups from items
  select * from {{ ref('int_orders_aggregated_from_items') }}
),
final as (
  select
      o.order_id,
      o.user_id,
      o.status,
      o.created_at,                            -- lineage + testing
      coalesce(a.item_count, 0)               as item_count,
      coalesce(a.items_gross_revenue, 0.0)    as order_gross_revenue
  from orders o
  left join items_agg a
    on o.order_id = a.order_id
)

select * from final
