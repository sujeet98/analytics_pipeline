-- Staging model: Products
-- Dedup by product_id; carry DC id for enrichment.

with raw as (
  select
    id as product_id,
    cost, category, name, brand, retail_price, department, sku, distribution_center_id,
    coalesce(
      ingest_ts_utc,
      to_timestamp(concat(ingest_date, ' ', run_ts), 'yyyy-MM-dd HHmmss')
    ) as src_ingest_ts
  from {{ source('bronze_dev','products') }}
),

ranked as (
  select
    r.*,
    row_number() over (
      partition by r.product_id
      order by r.src_ingest_ts desc
    ) as rn
  from raw r
)

select
  cast(product_id as bigint)             as product_id,
  cast(cost as decimal(18,2))            as cost,
  trim(category)                         as category,
  trim(name)                             as name,
  trim(brand)                            as brand,
  cast(retail_price as decimal(18,2))    as retail_price,
  trim(department)                       as department,
  trim(sku)                              as sku,
  cast(distribution_center_id as bigint) as distribution_center_id,
  src_ingest_ts
from ranked
where rn = 1;
