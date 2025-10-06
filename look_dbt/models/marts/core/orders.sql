{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns',
    incremental_strategy='merge',
    constraints={
      "primary_key": "order_id",
      "not_null": ["order_id","user_id","created_at"]
    }
) }}

with orders as (
  select
    order_id,
    user_id,
    status,
    created_at
  from {{ ref('stg_look__orders') }}
),
items as (
  select
    order_id,
    count(*) as item_count,
    sum(sale_price) as order_gross_revenue
  from {{ ref('stg_look__order_items') }}
  group by 1
)

select
  o.order_id,
  o.user_id,
  o.created_at,
  o.status,
  coalesce(i.item_count, 0) as item_count,
  coalesce(i.order_gross_revenue, 0.0) as order_gross_revenue
from orders o
left join items i using (order_id)

{% if is_incremental() %}
where o.created_at > (select coalesce(max(created_at), '1900-01-01') from {{ this }})
{% endif %}
