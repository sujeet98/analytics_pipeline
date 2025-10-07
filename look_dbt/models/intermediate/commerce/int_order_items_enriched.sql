{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    on_schema_change='append_new_columns'
) }}

with
oi as ( select * from {{ ref('stg_look__order_items') }} ),
inv as ( select * from {{ ref('stg_look__inventory_items') }} ),
prod as ( select * from {{ ref('stg_look__products') }} ),
dc   as ( select * from {{ ref('stg_look__distribution_centers') }} ),

items_joined as (
    select
        -- keys
        oi.order_item_id,
        oi.order_id,
        oi.user_id,
        oi.product_id,
        oi.inventory_item_id,

        -- item facts
        oi.status         as item_status,
        oi.sale_price,
        oi.created_at     as item_created_at,

        -- inventory attrs
        inv.product_id    as inv_product_id,
        inv.product_distribution_center_id,

        -- product attrs
        prod.name         as product_name,
        prod.brand        as product_brand,
        prod.category     as product_category,
        prod.department   as product_department,
        prod.retail_price,
        prod.sku          as product_sku,

        -- resolve DC id from inventory first, else product
        coalesce(inv.product_distribution_center_id, prod.distribution_center_id)
          as distribution_center_id
    from oi
    left join inv
      on oi.inventory_item_id = inv.inventory_item_id
    left join prod
      on oi.product_id = prod.product_id
),

items_with_dc as (
    select
        i.*,
        dc.name       as distribution_center_name,
        dc.latitude   as distribution_center_latitude,
        dc.longitude  as distribution_center_longitude
    from items_joined i
    left join dc
      on i.distribution_center_id = dc.distribution_center_id
)

select * from items_with_dc

{% if is_incremental() %}
  -- protect against reprocessing if upstream bronze/staging are append-only
  where order_item_id not in (select order_item_id from {{ this }})
{% endif %}
