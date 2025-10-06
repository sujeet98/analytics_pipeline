{{ config(materialized='view') }}

with b as (select * from {{ ref('base_look__orders') }})
select
  order_id,
  user_id,
  {{ normalize_order_status('status') }} as status,
  {{ clean_string('gender') }} as gender,
  {{ parse_ts('created_at') }}   as created_at,
  {{ parse_ts('returned_at') }}  as returned_at,
  {{ parse_ts('shipped_at') }}   as shipped_at,
  {{ parse_ts('delivered_at') }} as delivered_at,
  num_of_item,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b
