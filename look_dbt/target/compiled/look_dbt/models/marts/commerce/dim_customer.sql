

select
  customer_sk,                                  -- PK (SCD2)
  global_customer_id,
  source_system,
  user_id,                                      -- source BK (audit)
  email, first_name, last_name,
  gender, age,
  country, state, city, postal_code,
  traffic_source,
  valid_from, valid_to, is_current,
  valid_from_date
from sujeet_data_analytics_workspace.silver_dev.core_commerce_users_scd;