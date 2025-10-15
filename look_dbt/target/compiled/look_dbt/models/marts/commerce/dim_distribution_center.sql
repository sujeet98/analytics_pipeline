

select
  dc_sk,                                        -- PK (SCD2)
  global_dc_id,
  source_system,
  distribution_center_id,
  name, latitude, longitude,
  valid_from, valid_to, is_current, valid_from_date
from sujeet_data_analytics_workspace.silver_dev.core_commerce_distribution_centers_scd;