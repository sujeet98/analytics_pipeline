-- Conformed Product dimension.

{{ config(materialized='table') }}

select
  product_id,
  name            as product_name,
  brand           as product_brand,
  category        as product_category,
  department      as product_department,
  sku             as product_sku,
  retail_price,
  cost,
  distribution_center_id,
  src_ingest_ts
from {{ ref('stg_look__products') }};
