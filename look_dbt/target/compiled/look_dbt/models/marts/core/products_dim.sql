-- Conformed Product dimension.



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
from sujeet_data_analytics_workspace.silver_dev.stg_look__products;