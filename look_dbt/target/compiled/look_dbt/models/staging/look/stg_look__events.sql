-- Staging model: Events
-- 1) Unified ingestion ts
-- 2) Dedupe by event_id
-- 3) Type & normalize event_type into a stable set

with raw as (
  select
    id as event_id,
    user_id,
    sequence_number,
    session_id,
    created_at,
    ip_address, city, state, postal_code,
    browser, traffic_source, uri,
    event_type,
    coalesce(
      ingest_ts_utc,
      to_timestamp(concat(ingest_date, ' ', run_ts), 'yyyy-MM-dd HHmmss')
    ) as src_ingest_ts
  from `sujeet_data_analytics_workspace`.`bronze_dev`.`events`
),

ranked as (
  select
    r.*,
    row_number() over (
      partition by r.event_id
      order by r.src_ingest_ts desc
    ) as rn
  from raw r
)

select
  cast(event_id        as bigint)  as event_id,
  cast(user_id         as bigint)  as user_id,
  cast(sequence_number as bigint)  as sequence_number,
  cast(session_id      as string)  as session_id,
  cast(created_at      as timestamp) as created_at,
  trim(ip_address)     as ip_address,
  trim(city)           as city,
  trim(state)          as state,
  trim(postal_code)    as postal_code,
  trim(browser)        as browser,
  trim(traffic_source) as traffic_source,
  trim(uri)            as uri,
  case lower(coalesce(event_type,''))
    when 'product'    then 'product'
    when 'cart'       then 'cart'
    when 'home'       then 'home'
    when 'cancel'     then 'cancel'
    when 'purchase'   then 'purchase'
    when 'department' then 'department'
    else 'unknown'
  end                         as event_type,
  src_ingest_ts
from ranked
where rn = 1;