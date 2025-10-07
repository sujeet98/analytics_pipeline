-- Staging model: Users
-- Dedup by user_id; keep PII here so downstream marts can choose what to expose.

with raw as (
  select
    id as user_id,
    first_name, last_name, email, age, gender,
    state, street_address, postal_code, city, country,
    latitude, longitude, traffic_source, created_at, user_geom,
    coalesce(
      ingest_ts_utc,
      to_timestamp(concat(ingest_date, ' ', run_ts), 'yyyy-MM-dd HHmmss')
    ) as src_ingest_ts
  from `sujeet_data_analytics_workspace`.`bronze_dev`.`users`
),

ranked as (
  select
    r.*,
    row_number() over (
      partition by r.user_id
      order by r.src_ingest_ts desc
    ) as rn
  from raw r
)

select
  cast(user_id as bigint)         as user_id,
  trim(first_name)                as first_name,
  trim(last_name)                 as last_name,
  trim(email)                     as email,
  cast(age as bigint)             as age,
  trim(gender)                    as gender,
  trim(state)                     as state,
  trim(street_address)            as street_address,
  trim(postal_code)               as postal_code,
  trim(city)                      as city,
  trim(country)                   as country,
  cast(latitude as double)        as latitude,
  cast(longitude as double)       as longitude,
  trim(traffic_source)            as traffic_source,
  cast(created_at as timestamp)   as created_at,
  trim(user_geom)                 as user_geom,
  src_ingest_ts
from ranked
where rn = 1;