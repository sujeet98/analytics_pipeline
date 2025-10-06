{{ config(
    materialized='table',
    constraints={
      "primary_key": "user_id",
      "not_null": ["user_id","created_at"]
    }
) }}

with users as (
  select
    user_id,
    first_name,
    last_name,
    email,
    gender,
    state,
    city,
    country,
    created_at
  from {{ ref('stg_look__users') }}
)

select
  user_id,
  first_name,
  last_name,
  email,
  split_part(lower(email), '@', 2) as email_domain,
  gender,
  state,
  city,
  country,
  created_at
from users
