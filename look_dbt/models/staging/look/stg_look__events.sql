{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    partition_by=['event_date'],
    tags=['staging','look']
) }}

{% set lookback_days = 2 %}

with tgt_max as (
  select
    {% if is_incremental() %} coalesce(max(ingest_ts_utc), timestamp('1900-01-01'))
    {% else %}                 timestamp('1900-01-01') {% endif %} as max_ts
  from {% if is_incremental() %} {{ this }} {% else %} (select 1) _ {% endif %}
),

src as (
  select
    cast(id as bigint)               as id,
    cast(user_id as bigint)          as user_id,
    cast(sequence_number as int)     as sequence_number,
    cast(session_id as string)       as session_id,
    cast(created_at as timestamp)    as event_ts,
    nullif(ip_address,'')            as ip_address,
    nullif(city,'')                  as city,
    nullif(state,'')                 as state,
    nullif(postal_code,'')           as postal_code,
    lower(nullif(browser,''))        as browser_raw,
    lower(nullif(traffic_source,'')) as traffic_source_raw,
    nullif(uri,'')                   as uri,
    lower(nullif(event_type,''))     as event_type_raw,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from {{ source('look','events') }}
  {% if is_incremental() %}
    where ingest_ts_utc >= dateadd(day, -{{ lookback_days }}, (select max_ts from tgt_max))
  {% endif %}
),
derived as (
  select
    *,
    date(event_ts) as event_date,
    -- Normalize browser to: Firefox, Other, IE, Safari, Chrome
    case
      when browser_raw like 'chrome%' then 'Chrome'
      when browser_raw like 'safari%' then 'Safari'
      when browser_raw like 'firefox%' then 'Firefox'
      when browser_raw in ('ie','internet explorer','edge','msie') then 'IE'
      else 'Other'
    end as browser_norm,
    -- Normalize traffic source to: Adwords, Organic, Email, Facebook, YouTube
    case
      when traffic_source_raw in ('adwords','paid','paid_search','google ads','google') then 'Adwords'
      when traffic_source_raw in ('email','newsletter')                                 then 'Email'
      when traffic_source_raw in ('facebook','instagram','meta','fb')                   then 'Facebook'
      when traffic_source_raw in ('youtube','yt','video')                               then 'YouTube'
      when traffic_source_raw in ('organic','search','seo','direct','referral','display','social','other','(direct)') then 'Organic'
      else null
    end as traffic_source_norm,
    -- Normalize event type to: cancel, purchase, product, cart, department, home
    -- We use both the raw event_type and URI heuristics.
    case
      when event_type_raw in ('purchase','order_complete','buy','checkout_complete') then 'purchase'
      when event_type_raw in ('cancel','cancellation','order_cancel')                then 'cancel'
      when event_type_raw in ('add_to_cart','add','cart')                            then 'cart'
      when event_type_raw in ('product_detail','pdp','view_product','product')       then 'product'
      when event_type_raw in ('department','category','plp')                         then 'department'
      when event_type_raw in ('page_view','pageview','home')                         then 'home'
      else null
    end as event_type_norm
  from src
),
dedup as (
  select *
  from (
    select *,
      row_number() over (partition by id order by ingest_ts_utc desc nulls last) as rn
    from derived
  ) where rn = 1
)
select
  id,
  user_id,
  sequence_number,
  session_id,
  event_ts,
  event_date,
  ip_address,
  city,
  state,
  postal_code,
  browser_norm      as browser,
  traffic_source_norm as traffic_source,
  uri,
  event_type_norm   as event_type,
  ingest_ts_utc
from dedup;
