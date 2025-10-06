

-- Purpose: Conformed product dimension
select
  product_id,
  brand,
  category,
  department,
  sku,
  retail_price,
  distribution_center_id
from sujeet_data_analytics_workspace.silver_dev.stg_look__products