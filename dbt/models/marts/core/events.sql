{{ config(materialized='table') }}

-- Purpose: Entity table for events at event_id grain
select
  event_id,
  user_id,
  session_id,
  sequence_number,
  event_type,
  browser,
  traffic_source,
  uri,
  created_at,
  event_date
from {{ ref('stg_look__events') }}
