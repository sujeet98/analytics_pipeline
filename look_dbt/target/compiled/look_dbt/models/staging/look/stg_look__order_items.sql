



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),

src as (
  select
    cast(id as bigint)                 as id,
    cast(order_id as bigint)           as order_id,
    cast(user_id as bigint)            as user_id,
    cast(product_id as bigint)         as product_id,
    cast(inventory_item_id as bigint)  as inventory_item_id,
    nullif(lower(status), '')          as status_raw,
    cast(created_at as timestamp)      as created_at,
    cast(shipped_at as timestamp)      as shipped_at,
    cast(delivered_at as timestamp)    as delivered_at,
    cast(returned_at as timestamp)     as returned_at,
    cast(sale_price as double)         as sale_price_raw,
    cast(ingest_ts_utc as timestamp)   as ingest_ts_utc,
    cast(ingest_date as string)        as _ingest_date
  from `sujeet_data_analytics_workspace`.`bronze_dev`.`order_items`
  
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
    end as item_status,
    case when sale_price_raw < 0 then 0.0 else sale_price_raw end as sale_price
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
  order_id,
  user_id,
  product_id,
  inventory_item_id,
  item_status,
  created_at,
  shipped_at,
  delivered_at,
  returned_at,
  sale_price,
  ingest_ts_utc,
  _ingest_date
from dedup;