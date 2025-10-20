{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',                    
    partition_by=['event_date'],
    cluster_by=['user_id','session_id'], 
    tags=['staging','look']
) }}

{% set lookback_days = 7 %}  -- widen window for late events

with src as (
  select
    cast(id as bigint)            as id,
    cast(user_id as bigint)       as user_id,
    cast(sequence_number as int)  as sequence_number,
    cast(session_id as string)    as session_id,
    cast(created_at as timestamp) as event_ts,
    date(created_at)              as event_date,
    nullif(ip_address,'')         as ip_address,
    nullif(city,'')               as city,
    nullif(state,'')              as state,
    nullif(postal_code,'')        as postal_code,
    lower(nullif(browser,''))     as browser_raw,
    lower(nullif(traffic_source,'')) as traffic_source_raw,
    nullif(uri,'')                as uri,
    lower(nullif(event_type,''))  as event_type_raw,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from {{ source('look','events') }}
  {% if is_incremental() %}
    where date(created_at) >= dateadd(day, -{{ lookback_days }}, current_date())
  {% endif %}
),
derived as (
  select
    *,
    case
      when browser_raw like 'chrome%' then 'Chrome'
      when browser_raw like 'safari%' then 'Safari'
      when browser_raw like 'firefox%' then 'Firefox'
      when browser_raw in ('ie','internet explorer','edge','msie') then 'IE'
      else 'Other'
    end as browser,
    case
      when traffic_source_raw in ('adwords','paid','paid_search','google ads','google') then 'Adwords'
      when traffic_source_raw in ('email','newsletter')                                 then 'Email'
      when traffic_source_raw in ('facebook','instagram','meta','fb')                   then 'Facebook'
      when traffic_source_raw in ('youtube','yt','video')                               then 'YouTube'
      when traffic_source_raw in ('organic','search','seo','direct','referral','display','social','other','(direct)') then 'Organic'
      else null
    end as traffic_source,
    case
      when event_type_raw in ('purchase','order_complete','buy','checkout_complete') then 'purchase'
      when event_type_raw in ('cancel','cancellation','order_cancel')                then 'cancel'
      when event_type_raw in ('add_to_cart','add','cart')                            then 'cart'
      when event_type_raw in ('product_detail','pdp','view_product','product')       then 'product'
      when event_type_raw in ('department','category','plp')                         then 'department'
      when event_type_raw in ('page_view','pageview','home')                         then 'home'
      else null
    end as event_type
  from src
),
dedup as (
  select *
  from (
    select *,
      row_number() over (partition by id order by ingest_ts_utc desc nulls last) as rn
    from derived
  )
  where rn = 1
)

select
  id as event_id,
  user_id, sequence_number, session_id,
  event_ts, event_date,
  ip_address, city, state, postal_code,
  browser, traffic_source, uri, event_type,
  ingest_ts_utc
from dedup;
