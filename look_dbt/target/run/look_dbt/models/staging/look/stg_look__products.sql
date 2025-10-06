
      
  
  
  
  create or replace view sujeet_data_analytics_workspace.silver_dev.stg_look__products
  (
    `product_id` comment 'PK (from bronze.products.id).',
	`cost`,
	`category`,
	`name`,
	`brand`,
	`retail_price`,
	`department`,
	`sku`,
	`distribution_center_id` comment 'FK to stg_look__distribution_centers.distribution_center_id.',
	`ingest_ts_utc`,
	`source_table`,
	`ingest_date`,
	`run_ts`
  )
  comment 'Cleaned + deduped products from bronze_dev.products.'
  as (
    with b as (select * from sujeet_data_analytics_workspace.silver_dev.base_look__products)
select
  id as product_id,
  cast(cost as decimal(18, 2))           as cost,
  nullif(trim(category), '')  as category,
  nullif(trim(name), '')      as name,
  nullif(trim(brand), '')     as brand,
  cast(retail_price as decimal(18, 2))   as retail_price,
  nullif(trim(department), '') as department,
  nullif(trim(sku), '')        as sku,
  distribution_center_id,
  ingest_ts_utc, source_table, ingest_date, run_ts
from b
  )


    