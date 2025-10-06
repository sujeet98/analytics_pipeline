
      
  
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.stg_look__inventory_items
  (
    `inventory_item_id` comment 'PK (from bronze.inventory_items.id).',
	`product_id` comment 'FK to stg_look__products.product_id.',
	`created_at`,
	`sold_at`,
	`cost`,
	`product_category`,
	`product_name`,
	`product_brand`,
	`product_retail_price`,
	`product_department`,
	`product_sku`,
	`product_distribution_center_id` comment 'FK to stg_look__distribution_centers.distribution_center_id.',
	`ingest_ts_utc`,
	`source_table`,
	`ingest_date`,
	`run_ts`
  )
  comment 'Cleaned + deduped inventory items from bronze_dev.inventory_items.'
  as (
    with b as (select * from sujeet_data_analytics_workspace.silver_dev.base_look__inventory_items)
select
  id as inventory_item_id,
  product_id,
  cast(created_at as timestamp) as created_at,
  cast(sold_at as timestamp)    as sold_at,
  cast(cost as decimal(18, 2))        as cost,
  nullif(trim(product_category), '')       as product_category,
  nullif(trim(product_name), '')           as product_name,
  nullif(trim(product_brand), '')          as product_brand,
  cast(product_retail_price as decimal(18, 2))        as product_retail_price,
  nullif(trim(product_department), '')     as product_department,
  nullif(trim(product_sku), '')            as product_sku,
  product_distribution_center_id,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b
  )


    