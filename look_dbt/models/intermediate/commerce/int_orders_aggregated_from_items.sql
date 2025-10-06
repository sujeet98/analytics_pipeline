{#
  Purpose
  -------
  Order-grain measures aggregated from item-level facts.

  Grain: one row per (order_id)
#}

{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns'
) }}

with items as (
    select * from {{ ref('int_order_items_enriched') }}
),

orders as (
    select * from {{ ref('stg_look__orders') }}
),

agg_from_items as (
    select
        i.order_id,
        any_value(o.user_id)                                   as user_id,        -- consistent user_id per order
        any_value(lower(o.status))                             as order_status,    -- canonical order status
        count(*)                                               as items_count,
        sum(coalesce(i.item_revenue, 0.0))                     as order_gross_revenue,

        -- order dates (prefer order table; fallback to items)
        any_value(o.created_at)                                as order_created_at,
        max(i.shipped_at)                                      as last_item_shipped_at,
        max(i.delivered_at)                                    as last_item_delivered_at,
        max(i.returned_at)                                     as last_item_returned_at
    from items i
    left join orders o
      on i.order_id = o.order_id
    group by i.order_id
)

select * from agg_from_items
{% if is_incremental() %}
  where order_created_at >= (select coalesce(max(order_created_at), to_timestamp('1970-01-01')) from {{ this }})
{% endif %}
;
