

with source_clean as (
  select
    cast(order_id as bigint) as order_id,
    cast(user_id as bigint)  as user_id,
    nullif(lower(status), '') as status_raw,
    nullif(lower(gender), '') as gender_raw,
    cast(created_at as timestamp)   as created_at,
    cast(shipped_at as timestamp)   as shipped_at,
    cast(delivered_at as timestamp) as delivered_at,
    cast(returned_at as timestamp)  as returned_at,
    cast(num_of_item as int)        as num_of_item,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc,
    cast(ingest_date as string)      as _ingest_date,
    cast(source_table as string)     as source_table
  from `sujeet_data_analytics_workspace`.`bronze_dev`.`orders`
  
    where ingest_ts_utc >= dateadd(day, -2, (select coalesce(max(ingest_ts_utc), '1900-01-01') from sujeet_data_analytics_workspace.silver_dev.stg_look__orders))
  
),
normalized as (
  select
    *,
    -- Map to one of: Cancelled, Shipped, Complete, Returned, Processing
    case
      when status_raw in ('cancelled','canceled','void','abandoned') then 'Cancelled'
      when status_raw in ('shipped','in_transit','delivered')        then 'Shipped'
      when status_raw in ('complete','completed')                     then 'Complete'
      when status_raw in ('returned','return','refunded')             then 'Returned'
      when status_raw in ('processing','processed','pending','open')  then 'Processing'
      else null
    end as order_status,
    case when gender_raw in ('m','male') then 'M'
         when gender_raw in ('f','female') then 'F'
         else null end as user_gender
  from source_clean
),
dedup as (
  select *
  from (
    select *,
      row_number() over (partition by order_id order by ingest_ts_utc desc nulls last) as rn
    from normalized
  ) where rn = 1
)
select
  order_id,
  user_id,
  order_status,
  user_gender,
  created_at,
  shipped_at,
  delivered_at,
  returned_at,
  num_of_item,
  ingest_ts_utc,
  _ingest_date
from dedup;