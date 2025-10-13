



with tgt_max as (
  select
    
      timestamp('1900-01-01')
     as max_ts
  from  (select 1) as _ 
),

src as (
  select
    order_id,
    user_id,
    order_status,
    cast(created_at   as timestamp) as created_at,
    cast(shipped_at   as timestamp) as shipped_at,
    cast(delivered_at as timestamp) as delivered_at,
    cast(returned_at  as timestamp) as returned_at,
    num_of_item,
    cast(ingest_ts_utc as timestamp) as ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.stg_look__orders
  where ingest_ts_utc >= dateadd(day, -2, (select max_ts from tgt_max))
)

select
  order_id, user_id, order_status,
  date(created_at) as order_date,
  created_at, shipped_at, delivered_at, returned_at,
  num_of_item,
  'look'        as source_system,
  ingest_ts_utc as canonical_updated_at,
  ingest_ts_utc
from src;