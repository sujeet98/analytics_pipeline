{% snapshot snap_users__look %}
{{
  config(
    unique_key    = 'user_id',
    strategy      = 'check',
    check_cols    = [
      'email_norm','first_name_norm','last_name_norm','gender',
      'age','country_norm','state_norm','city_norm','postal_code_norm',
      'traffic_source'
    ],
    invalidate_hard_deletes = true
  )
}}

with src as (
  select
    'look' as source_system,
    user_id,
    -- normalize for stability
    lower(email)                 as email_norm,
    trim(first_name)             as first_name_norm,
    trim(last_name)              as last_name_norm,
    gender,
    age,
    upper(country)               as country_norm,
    upper(state)                 as state_norm,
    upper(city)                  as city_norm,
    upper(postal_code)           as postal_code_norm,
    traffic_source,
    user_created_date,
    created_at
  from {{ ref('int_commerce_users__look') }}
)

select * from src
{% endsnapshot %}
