



with tgt_max as (
  select
                     timestamp('1900-01-01')  as max_ts
  from  (select 1) _ 
),

-- === Per-source aligned inputs (add more sources as you onboard them) ===
src_thelook as (
  select
    'look'              as source_system,
    inventory_item_id,
    product_id,
    created_at,
    sold_at,
    unit_cost,
    retail_price,
    product_category,
    product_name,
    product_brand,
    product_department,
    product_sku,
    distribution_center_id,
    canonical_updated_at,
    ingest_ts_utc
  from sujeet_data_analytics_workspace.silver_dev.int_commerce_inventory_items__look
  where canonical_updated_at >= dateadd(day, -2, (select max_ts from tgt_max))
),

-- src_other here

unioned as (
  select * from src_thelook
  -- union all select * from src_other
),

final as (
  select
    concat(source_system, ':', cast(inventory_item_id as string)) as global_inventory_item_id,
    source_system,
    inventory_item_id,
    product_id,
    distribution_center_id,
    created_at,
    sold_at,
    date(created_at) as created_date,
    unit_cost,
    retail_price,
    product_category,
    product_name,
    product_brand,
    product_department,
    product_sku,
    case when sold_at is null then true else false end as is_in_stock,
    case when sold_at is not null
         then datediff(date(sold_at), date(created_at))  -- Spark: DATEDIFF(end,start)
    end as days_to_sale,
    canonical_updated_at,
    ingest_ts_utc
  from unioned
)

select * from final