-- Staging model: Order Items
-- 1) Build src_ingest_ts exactly as landing partition semantics.
-- 2) Deduplicate by order_item_id (latest).
-- 3) Type & normalize; keep monetary values as DECIMAL(18,2).

with raw as (
  select
    id               as order_item_id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,
    status,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    sale_price,
    coalesce(
      ingest_ts_utc,
      to_timestamp(concat(ingest_date, ' ', run_ts), 'yyyy-MM-dd HHmmss')
    ) as src_ingest_ts
  from {{ source('bronze_dev','order_items') }}
),

ranked as (
  select
    r.*,
    row_number() over (
      partition by r.order_item_id
      order by r.src_ingest_ts desc
    ) as rn
  from raw r
)

select
  cast(order_item_id     as bigint)        as order_item_id,
  cast(order_id          as bigint)        as order_id,
  cast(user_id           as bigint)        as user_id,
  cast(product_id        as bigint)        as product_id,
  cast(inventory_item_id as bigint)        as inventory_item_id,

  case lower(coalesce(status,''))
    when 'complete'   then 'Complete'
    when 'shipped'    then 'Shipped'
    when 'returned'   then 'Returned'
    when 'cancelled'  then 'Cancelled'
    when 'processing' then 'Processing'
    else 'Unknown'
  end                                         as status,

  cast(created_at   as timestamp)             as created_at,
  cast(shipped_at   as timestamp)             as shipped_at,
  cast(delivered_at as timestamp)             as delivered_at,
  cast(returned_at  as timestamp)             as returned_at,

  cast(sale_price as decimal(18,2))           as sale_price,

  src_ingest_ts
from ranked
where rn = 1;
