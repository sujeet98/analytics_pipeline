-- Staging model: Orders
-- Responsibilities:
--   1) Derive a unified ingestion timestamp (src_ingest_ts) from bronze partition metadata.
--   2) Deduplicate by order_id using "latest by src_ingest_ts".
--   3) Type and normalize fields (status to a controlled set).
-- Why here (vs separate base): fewer scans, simpler DAG, still transparent via clear comments.

with raw as (
  select
    order_id,
    user_id,
    status,
    gender,
    created_at,
    returned_at,
    shipped_at,
    delivered_at,
    num_of_item,
    /* Ingestion lineage: prefer true timestamp; else parse from partition date+time (HHMMSS) */
    coalesce(
      ingest_ts_utc,
      to_timestamp(concat(ingest_date, ' ', run_ts), 'yyyy-MM-dd HHmmss')
    ) as src_ingest_ts
  from `sujeet_data_analytics_workspace`.`bronze_dev`.`orders`
),

ranked as (
  select
    r.*,
    row_number() over (
      partition by r.order_id
      order by r.src_ingest_ts desc
    ) as rn
  from raw r
)

select
  cast(order_id as bigint)  as order_id,
  cast(user_id as bigint)   as user_id,

  /* Controlled vocabulary for status */
  case lower(coalesce(status,''))
    when 'complete'   then 'Complete'
    when 'shipped'    then 'Shipped'
    when 'returned'   then 'Returned'
    when 'cancelled'  then 'Cancelled'
    when 'processing' then 'Processing'
    else 'Unknown'
  end                       as status,

  cast(created_at   as timestamp) as created_at,
  cast(returned_at  as timestamp) as returned_at,
  cast(shipped_at   as timestamp) as shipped_at,
  cast(delivered_at as timestamp) as delivered_at,

  cast(num_of_item as bigint)     as num_of_item,

  src_ingest_ts
from ranked
where rn = 1;