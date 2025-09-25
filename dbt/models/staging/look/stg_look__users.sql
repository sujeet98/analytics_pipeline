{{ config(materialized='view') }}

-- Purpose: Stage users; hash PII for downstream use. No joins.
-- Grain: 1 row per user_id (latest)

with source as (
  select * from {{ source('look','users') }}
), renamed as (
  select
    cast(id as bigint)          as user_id,
    first_name, last_name,
    email,
    {{ email_sha256('email') }} as email_sha256,
    cast(age as int)            as age,
    {{ clean_lower('gender') }} as gender,
    state, street_address, postal_code, city, country,
    latitude, longitude,
    {{ clean_lower('traffic_source') }} as traffic_source,
    {{ clean_ts('created_at') }} as created_at,
    user_geom,
    ingest_ts_utc,
    row_number() over (partition by id order by ingest_ts_utc desc) as _rn
  from source
)
select
  user_id, first_name, last_name, email, email_sha256, age, gender,
  state, street_address, postal_code, city, country,
  latitude, longitude, traffic_source, created_at, user_geom,
  {{ to_date_utc('created_at') }} as created_date
from renamed
where _rn = 1
