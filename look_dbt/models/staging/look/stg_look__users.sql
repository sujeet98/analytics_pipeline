{{ config(materialized='view') }}

with b as (select * from {{ ref('base_look__users') }})
select
  id as user_id,
  {{ clean_string('first_name') }} as first_name,
  {{ clean_string('last_name') }}  as last_name,
  {{ clean_string('email') }}      as email,
  age,
  {{ clean_string('gender') }}  as gender,
  {{ clean_string('state') }}   as state,
  {{ clean_string('street_address') }} as street_address,
  {{ clean_string('postal_code') }}    as postal_code,
  {{ clean_string('city') }}           as city,
  {{ clean_string('country') }}        as country,
  latitude,
  longitude,
  {{ clean_string('traffic_source') }} as traffic_source,
  {{ parse_ts('created_at') }} as created_at,
  user_geom,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b
