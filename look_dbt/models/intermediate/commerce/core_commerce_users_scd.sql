{{ config(
  materialized='table',
  unique_key=['global_customer_id','valid_from'],
  schema='silver_dev',
  tags=['core','commerce','scd2']
) }}

with snap as (
  select
    source_system,
    user_id,
    -- publish readable fields (can use original casing if you prefer)
    email_norm       as email,
    first_name_norm  as first_name,
    last_name_norm   as last_name,
    gender,
    age,
    country_norm     as country,
    state_norm       as state,
    city_norm        as city,
    postal_code_norm as postal_code,
    traffic_source,
    dbt_valid_from,
    dbt_valid_to
  from {{ ref('snap_users__look') }}
  -- union all other source snapshots here
),

keyed as (
  select
    concat(source_system, ':', cast(user_id as string)) as global_customer_id,
    *
  from snap
),

normalized as (
  select
    global_customer_id,
    source_system,
    user_id,
    email, first_name, last_name,
    gender, age,
    country, state, city, postal_code,
    traffic_source,
    dbt_valid_from                                as valid_from,
    coalesce(dbt_valid_to, timestamp('9999-12-31')) as valid_to,
    dbt_valid_to is null                          as is_current,
    date(dbt_valid_from)                          as valid_from_date
  from keyed
)

select
  {{ dbt_utils.generate_surrogate_key(['global_customer_id','valid_from']) }} as customer_sk,
  global_customer_id,
  source_system,
  user_id,
  email, first_name, last_name,
  gender, age,
  country, state, city, postal_code,
  traffic_source,
  valid_from,
  valid_to,
  is_current,
  valid_from_date
from normalized;
