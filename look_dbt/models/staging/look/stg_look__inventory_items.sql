-- Staging model: Inventory Items
-- Dedup by inventory_item_id; keep landed product attributes (name/brand/etc.) for convenience.

with raw as (
  select
    id as inventory_item_id,
    product_id, created_at, sold_at, cost,
    product_category, product_name, product_brand, product_retail_price,
    product_department, product_sku, product_distribution_center_id,
    coalesce(
      ingest_ts_utc,
      to_timestamp(concat(ingest_date, ' ', run_ts), 'yyyy-MM-dd HHmmss')
    ) as src_ingest_ts
  from {{ source('bronze_dev','inventory_items') }}
),

ranked as (
  select
    r.*,
    row_number() over (
      partition by r.inventory_item_id
      order by r.src_ingest_ts desc
    ) as rn
  from raw r
)

select
  cast(inventory_item_id as bigint)           as inventory_item_id,
  cast(product_id as bigint)                  as product_id,
  cast(created_at as timestamp)               as created_at,
  cast(sold_at as timestamp)                  as sold_at,
  cast(cost as decimal(18,2))                 as cost,
  trim(product_category)                      as product_category,
  trim(product_name)                          as product_name,
  trim(product_brand)                         as product_brand,
  cast(product_retail_price as decimal(18,2)) as product_retail_price,
  trim(product_department)                    as product_department,
  trim(product_sku)                           as product_sku,
  cast(product_distribution_center_id as bigint) as product_distribution_center_id,
  src_ingest_ts
from ranked
where rn = 1;
