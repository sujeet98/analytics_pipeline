{{ config(
    materialized='table',
    constraints={
      "primary_key": "product_id",
      "not_null": ["product_id","sku"]
    }
) }}

select
  product_id,
  name,
  brand,
  category,
  department,
  sku,
  retail_price,
  cost,
  distribution_center_id
from {{ ref('stg_look__products') }}
