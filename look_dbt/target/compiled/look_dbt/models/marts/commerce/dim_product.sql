

select
  product_sk,                                   -- PK (SCD2)
  global_product_id,
  source_system,
  product_id,                                   -- source BK
  product_name, category, brand, department, sku,
  retail_price,
  distribution_center_id,                       -- BK (for optional dc lookup)
  valid_from, valid_to, is_current, valid_from_date
from sujeet_data_analytics_workspace.silver_dev.core_commerce_products_scd;