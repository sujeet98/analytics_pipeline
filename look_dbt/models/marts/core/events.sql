{{ config(
    materialized='table',
    constraints={
      "primary_key": "event_id",
      "not_null": ["event_id","created_at"]
    }
) }}

select
  event_id,
  user_id,
  session_id,
  sequence_number,
  created_at,
  event_type,
  city,
  state,
  postal_code,
  browser,
  traffic_source,
  uri,
  ip_address
from {{ ref('stg_look__events') }}
