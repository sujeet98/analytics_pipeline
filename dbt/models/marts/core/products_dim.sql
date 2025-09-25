{{ config(materialized='table') }}

-- Purpose: Conformed product dimension
select
  product_id,
  brand,
  category,
  department,
  sku,
  retail_price,
  distribution_center_id
from {{ ref('stg_look__products') }}
