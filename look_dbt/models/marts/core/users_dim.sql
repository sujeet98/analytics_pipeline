-- Conformed User dimension (safe attributes by default).

{{ config(materialized='table') }}

select
  user_id,
  email,
  first_name,
  last_name,
  age,
  gender,
  city,
  state,
  country,
  traffic_source,
  created_at,
  src_ingest_ts
from {{ ref('stg_look__users') }};
