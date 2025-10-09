

with src as (
  select
    cast(id as bigint)                 as id,
    nullif(first_name,'')              as first_name,
    nullif(last_name,'')               as last_name,
    lower(nullif(email,''))            as email,
    cast(age as int)                   as age,
    case when lower(gender) in ('m','male') then 'M'
         when lower(gender) in ('f','female') then 'F' else null end as gender,
    nullif(state,'')                   as state,
    nullif(street_address,'')          as street_address,
    nullif(postal_code,'')             as postal_code,
    nullif(city,'')                    as city,
    nullif(country,'')                 as country,
    cast(latitude as double)           as latitude,
    cast(longitude as double)          as longitude,
    lower(nullif(traffic_source,''))   as traffic_source_raw,
    cast(created_at as timestamp)      as created_at,
    cast(ingest_ts_utc as timestamp)   as ingest_ts_utc,
    cast(ingest_date as string)        as _ingest_date
  from `sujeet_data_analytics_workspace`.`bronze_dev`.`users`
  
    where ingest_ts_utc >= dateadd(day, -2, (select coalesce(max(ingest_ts_utc), '1900-01-01') from sujeet_data_analytics_workspace.silver_dev.stg_look__users))
  
),
normalized as (
  select
    *,
    case
      when traffic_source_raw in ('display') then 'Display'
      when traffic_source_raw in ('email','newsletter') then 'Email'
      when traffic_source_raw in ('facebook','meta') then 'Facebook'
      when traffic_source_raw in ('display','banner') then 'Organic'
      when traffic_source_raw in ('search','direct') then 'Search'
      else null
    end as traffic_source
  from src
),
dedup as (
  select *
  from (
    select *,
      row_number() over (partition by id order by ingest_ts_utc desc nulls last) as rn
    from normalized
  ) where rn = 1
)
select
  id,
  first_name,
  last_name,
  email,
  age,
  gender,
  state,
  street_address,
  postal_code,
  city,
  country,
  latitude,
  longitude,
  traffic_source,
  created_at,
  ingest_ts_utc,
  _ingest_date
from dedup;