

-- Purpose: Conformed user dimension (PII-safe; hashed emails)
select
  user_id,
  first_name,
  last_name,
  email_sha256,
  age,
  case when age is null then null
       when age < 18 then 'under_18'
       when age between 18 and 24 then '18_24'
       when age between 25 and 34 then '25_34'
       when age between 35 and 44 then '35_44'
       when age between 45 and 54 then '45_54'
       when age between 55 and 64 then '55_64'
       else '65_plus' end as age_band,
  gender,
  state,
  city,
  country,
  traffic_source,
  created_date
from sujeet_data_analytics_workspace.silver_dev.stg_look__users