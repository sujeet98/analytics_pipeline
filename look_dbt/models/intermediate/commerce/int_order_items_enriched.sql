{# 
  Purpose
  -------
  Item-grain enrichment for analytics:
  - joins product + inventory metadata
  - derives consistent revenue/cycle-time fields
  - standardizes null/empty handling for dates & numbers

  Grain: one row per (order_item_id)
#}

{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    on_schema_change='sync_all_columns'
) }}

with oi as (
    select * from {{ ref('stg_look__order_items') }}
),

prod as (
    select * from {{ ref('stg_look__products') }}
),

inv as (
    select * from {{ ref('stg_look__inventory_items') }}
),

dc as (
    select * from {{ ref('stg_look__distribution_centers') }}
),

enriched as (
    select
        -- keys
        oi.order_item_id,
        oi.order_id,
        oi.user_id,
        oi.product_id,
        oi.inventory_item_id,

        -- status & timestamps
        lower(oi.status)                                   as item_status,
        oi.created_at,
        oi.shipped_at,
        oi.delivered_at,
        oi.returned_at,

        -- measures
        cast(oi.sale_price as double)                      as item_revenue,

        -- product attrs (denormalized for convenience)
        prod.category                                      as product_category,
        prod.name                                          as product_name,
        prod.brand                                         as product_brand,
        prod.retail_price                                  as product_retail_price,
        prod.department                                    as product_department,
        prod.sku                                           as product_sku,

        -- distribution center (prefer inventory’s DC if present; otherwise product’s)
        coalesce(inv.distribution_center_id, prod.distribution_center_id) as distribution_center_id,
        dc.name                                            as distribution_center_name,
        dc.latitude                                        as distribution_center_latitude,
        dc.longitude                                       as distribution_center_longitude,

        -- cycle-time helpers (nullable safe)
        case when oi.shipped_at   is not null and oi.created_at   is not null then datediff(oi.shipped_at,   oi.created_at)   end as days_to_ship,
        case when oi.delivered_at is not null and oi.shipped_at   is not null then datediff(oi.delivered_at, oi.shipped_at)   end as days_in_transit,
        case when oi.returned_at  is not null and oi.delivered_at is not null then datediff(oi.returned_at,  oi.delivered_at) end as days_to_return
    from oi
    left join prod
        on oi.product_id = prod.product_id
    left join inv
        on oi.inventory_item_id = inv.inventory_item_id
    left join dc
        on coalesce(inv.distribution_center_id, prod.distribution_center_id) = dc.distribution_center_id
)

select * from enriched
{% if is_incremental() %}
  where created_at >= (select coalesce(max(created_at), to_timestamp('1970-01-01')) from {{ this }})
{% endif %}
;
