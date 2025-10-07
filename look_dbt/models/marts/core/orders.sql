{{ config(
  materialized='incremental',
  unique_key='order_id',
  incremental_strategy='merge',
  on_schema_change='sync_all_columns'
) }}

with base as (
  select
    o.order_id,
    o.user_id,
    o.status,
    o.created_at,
    coalesce(a.item_count, 0)            as item_count,
    coalesce(a.items_gross_revenue, 0.0) as order_gross_revenue,
    o.src_ingest_ts
  from {{ ref('stg_look__orders') }} o
  left join {{ ref('int_orders_aggregated_from_items') }} a
    on o.order_id = a.order_id
),
src as (
  select *
  from base
  {% if is_incremental() %}
    where src_ingest_ts >= (
      select coalesce(dateadd('day', -2, max(src_ingest_ts)), timestamp '1970-01-01')
      from {{ this }}
    )
  {% endif %}
)

select * from src
